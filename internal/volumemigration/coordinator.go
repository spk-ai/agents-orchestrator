// Package volumemigration coordinates metadata-only adoption of drained existing
// workspaces. It deliberately has no workload execution or storage allocation API.
package volumemigration

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var ErrQuarantined = errors.New("owner retained under migration block: unbound failed generation requires reconciliation")

type Registry interface {
	BeginVolumeAnchorMigration(context.Context, *runnersv1.BeginVolumeAnchorMigrationRequest, ...grpc.CallOption) (*runnersv1.BeginVolumeAnchorMigrationResponse, error)
	GetVolumeAnchorMigration(context.Context, *runnersv1.GetVolumeAnchorMigrationRequest, ...grpc.CallOption) (*runnersv1.GetVolumeAnchorMigrationResponse, error)
	AdvanceVolumeAnchorMigration(context.Context, *runnersv1.AdvanceVolumeAnchorMigrationRequest, ...grpc.CallOption) (*runnersv1.AdvanceVolumeAnchorMigrationResponse, error)
	GetVolume(context.Context, *runnersv1.GetVolumeRequest, ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error)
}

type Native interface {
	ReserveVolumeAnchorAdoption(context.Context, *runnerv1.ReserveVolumeAnchorAdoptionRequest, ...grpc.CallOption) (*runnerv1.ReserveVolumeAnchorAdoptionResponse, error)
	ApplyVolumeAnchorAdoption(context.Context, *runnerv1.ApplyVolumeAnchorAdoptionRequest, ...grpc.CallOption) (*runnerv1.ApplyVolumeAnchorAdoptionResponse, error)
	FinalizeVolumeAnchorAdoption(context.Context, *runnerv1.FinalizeVolumeAnchorAdoptionRequest, ...grpc.CallOption) (*runnerv1.FinalizeVolumeAnchorAdoptionResponse, error)
	ObserveVolumeAnchorAdoption(context.Context, *runnerv1.ObserveVolumeAnchorAdoptionRequest, ...grpc.CallOption) (*runnerv1.ObserveVolumeAnchorAdoptionResponse, error)
}

// Coordinator orders owner blocking, native reservation, registry receipt, native
// apply, registry binding, finalization and independent READY reads. It rereads the
// applied volume before removing the native hold and rechecks all volumes before
// owner completion. Adoption never fabricates allocation provenance. The interfaces
// cannot execute work, allocate/delete storage or install credentials; quarantine
// has no automatic unblock path.
// @see runners::internal/server/volume_anchor_migration
// @see k8s-runner::internal/server/volume_anchor_adoption
// @see api::proto/agynio/api/runners/v1/runners
type Coordinator struct {
	Registry Registry
	Native   Native
	// Checkpoint is an optional audit sink, called after acknowledged operations.
	// An error stops progress without rolling back committed migration evidence.
	Checkpoint func(stage string, migration *runnersv1.VolumeAnchorMigration) error
}

// Run resumes only this immutable plan. The caller must drain every writer and
// verify a restore-tested backup before the first invocation. Ambiguous replies
// stop this invocation; repeating the same plan reads committed progress first.
func (c Coordinator) Run(ctx context.Context, plan *runnersv1.BeginVolumeAnchorMigrationRequest) (*runnersv1.VolumeAnchorMigration, error) {
	if c.Registry == nil || c.Native == nil || plan == nil || !canonicalUUID(plan.Id) || len(plan.Sources) == 0 || len(plan.Sources) > 64 {
		return nil, errors.New("registry, native transport and complete immutable owner plan required")
	}
	plan = proto.Clone(plan).(*runnersv1.BeginVolumeAnchorMigrationRequest)
	seen := map[string]bool{}
	for _, s := range plan.Sources {
		if !canonicalUUID(s.GetVolumeId()) || s.GetExpectedRevision() == 0 || s.GetExpectedRevision() >= 999999999999999997 || seen[s.GetVolumeId()] {
			return nil, errors.New("unique canonical revisioned source volumes required")
		}
		seen[s.VolumeId] = true
	}
	slices.SortFunc(plan.Sources, func(a, b *runnersv1.VolumeAnchorMigrationSource) int {
		return strings.Compare(a.GetVolumeId(), b.GetVolumeId())
	})
	begin, err := c.Registry.BeginVolumeAnchorMigration(ctx, plan)
	if err != nil {
		return nil, fmt.Errorf("begin owner migration: %w", err)
	}
	m := begin.GetMigration()
	if err := validatePlan(plan, m); err != nil {
		return nil, err
	}
	if err := c.confirm(ctx, m); err != nil {
		return m, err
	}
	if err := c.checkpoint("begin", m); err != nil {
		return m, err
	}
	if m.Complete {
		return m, nil
	}
	quarantined := false
	for i := range m.Entries {
		e := m.Entries[i]
		if e.UnresolvedReason != "" {
			quarantined = true
			continue
		}
		if e.Adoption == nil {
			r, err := c.Native.ReserveVolumeAnchorAdoption(ctx, &runnerv1.ReserveVolumeAnchorAdoptionRequest{Id: e.AdoptionId, Expected: e.Source.Previous, Intent: e.Intent})
			if err != nil {
				return m, fmt.Errorf("reserve original volume %s: %w", e.Source.VolumeId, err)
			}
			if err := validateAdoption(e, r.GetAdoption()); err != nil {
				return m, err
			}
			if err := c.checkpoint("native-reserved", m); err != nil {
				return m, err
			}
			req := operation(m, e.Source.VolumeId)
			req.Operation = &runnersv1.AdvanceVolumeAnchorMigrationRequest_Reserve{Reserve: r.Adoption}
			next := proto.Clone(m).(*runnersv1.VolumeAnchorMigration)
			next.Entries[i].Adoption = proto.Clone(r.Adoption).(*runnerv1.VolumeAnchorAdoption)
			m, err = c.advance(ctx, m, next, req, "reserved")
			if err != nil {
				return m, err
			}
			e = m.Entries[i]
		}
		if e.Applied == nil {
			r, err := c.Native.ApplyVolumeAnchorAdoption(ctx, &runnerv1.ApplyVolumeAnchorAdoptionRequest{Adoption: e.Adoption})
			if err != nil {
				return m, fmt.Errorf("apply original volume %s: %w", e.Source.VolumeId, err)
			}
			expected := &runnerv1.ApplyVolumeAnchorAdoptionResponse{Adoption: e.Adoption, Volume: adoptedBinding(e), State: runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_APPLIED}
			if !proto.Equal(expected, r) {
				return m, errors.New("native apply changed the original PVC, owner or adoption receipt")
			}
			if err := c.checkpoint("native-applied", m); err != nil {
				return m, err
			}
			req := operation(m, e.Source.VolumeId)
			req.Operation = &runnersv1.AdvanceVolumeAnchorMigrationRequest_Apply{Apply: r}
			next := proto.Clone(m).(*runnersv1.VolumeAnchorMigration)
			next.Entries[i].Applied = proto.Clone(r).(*runnerv1.ApplyVolumeAnchorAdoptionResponse)
			m, err = c.advance(ctx, m, next, req, "applied")
			if err != nil {
				return m, err
			}
			e = m.Entries[i]
		}
		if e.Ready == nil {
			if err := c.confirmVolume(ctx, m, e); err != nil {
				return m, err
			}
			r, err := c.Native.FinalizeVolumeAnchorAdoption(ctx, &runnerv1.FinalizeVolumeAnchorAdoptionRequest{Adoption: e.Adoption})
			if err != nil {
				return m, fmt.Errorf("finalize original volume %s: %w", e.Source.VolumeId, err)
			}
			if !proto.Equal(r, &runnerv1.FinalizeVolumeAnchorAdoptionResponse{Adoption: e.Adoption, Volume: adoptedBinding(e), State: runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_READY}) {
				return m, errors.New("native finalization did not confirm the original PVC and receipt")
			}
			if err := c.checkpoint("native-ready", m); err != nil {
				return m, err
			}
			ready, err := c.observeReady(ctx, e)
			if err != nil {
				return m, err
			}
			req := operation(m, e.Source.VolumeId)
			req.Operation = &runnersv1.AdvanceVolumeAnchorMigrationRequest_Ready{Ready: ready}
			next := proto.Clone(m).(*runnersv1.VolumeAnchorMigration)
			next.Entries[i].Ready = proto.Clone(ready).(*runnerv1.ObserveVolumeAnchorAdoptionResponse)
			m, err = c.advance(ctx, m, next, req, "ready")
			if err != nil {
				return m, err
			}
		}
	}
	if quarantined {
		return m, ErrQuarantined
	}
	// Recheck every independently persisted receipt before opening the whole
	// owner. A vanished journal or changed binding keeps all its volumes closed.
	for _, e := range m.Entries {
		if err := c.confirmVolume(ctx, m, e); err != nil {
			return m, err
		}
		if _, err := c.observeReady(ctx, e); err != nil {
			return m, err
		}
	}
	req := operation(m, "")
	req.Operation = &runnersv1.AdvanceVolumeAnchorMigrationRequest_Complete{Complete: &runnersv1.CompleteVolumeAnchorMigration{}}
	next := proto.Clone(m).(*runnersv1.VolumeAnchorMigration)
	next.Complete = true
	return c.advance(ctx, m, next, req, "complete")
}

func (c Coordinator) checkpoint(stage string, m *runnersv1.VolumeAnchorMigration) error {
	if c.Checkpoint == nil {
		return nil
	}
	return c.Checkpoint(stage, proto.Clone(m).(*runnersv1.VolumeAnchorMigration))
}

func (c Coordinator) confirm(ctx context.Context, m *runnersv1.VolumeAnchorMigration) error {
	r, err := c.Registry.GetVolumeAnchorMigration(ctx, &runnersv1.GetVolumeAnchorMigrationRequest{OwnerKind: m.OwnerKind, OwnerId: m.OwnerId})
	if err != nil {
		return fmt.Errorf("read committed owner migration: %w", err)
	}
	if !proto.Equal(m, r.GetMigration()) {
		return errors.New("owner migration changed during independent read; resume the original plan")
	}
	return nil
}

func (c Coordinator) advance(ctx context.Context, current, next *runnersv1.VolumeAnchorMigration, req *runnersv1.AdvanceVolumeAnchorMigrationRequest, stage string) (*runnersv1.VolumeAnchorMigration, error) {
	next.Revision++
	r, err := c.Registry.AdvanceVolumeAnchorMigration(ctx, req)
	if err != nil {
		return current, fmt.Errorf("persist %s: %w", stage, err)
	}
	if !proto.Equal(next, r.GetMigration()) {
		return current, errors.New("registry response changed immutable migration progress")
	}
	if err := c.confirm(ctx, next); err != nil {
		return next, err
	}
	return next, c.checkpoint(stage, next)
}

func (c Coordinator) confirmVolume(ctx context.Context, m *runnersv1.VolumeAnchorMigration, e *runnersv1.VolumeAnchorMigrationEntry) error {
	r, err := c.Registry.GetVolume(ctx, &runnersv1.GetVolumeRequest{Id: e.Source.VolumeId})
	if err != nil {
		return fmt.Errorf("read persisted adoption binding: %w", err)
	}
	v := r.GetVolume()
	if v.GetMeta().GetId() != e.Source.VolumeId || v.GetOwnerKind() != m.OwnerKind || v.GetOwnerId() != m.OwnerId ||
		v.GetRunnerId() != m.RunnerId || v.GetOrganizationId() != m.OrganizationId || !v.GetCheckedLifecycle() || v.GetLifecycleRevision() != e.CheckedRevision+1 ||
		v.GetStatus() != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE || v.GetRemovalIntent() != nil || v.GetAnchorReservation() != nil ||
		v.GetInstanceId() != e.Source.Previous.InstanceId || !proto.Equal(v.GetBoundInstance(), e.Applied.GetVolume()) ||
		!proto.Equal(v.GetResourceAnchor(), e.Adoption.Anchor) || !proto.Equal(v.GetAnchorAdoption(), e.Adoption) {
		return errors.New("original applied PVC binding is not independently durable under this migration")
	}
	return nil
}

func (c Coordinator) observeReady(ctx context.Context, e *runnersv1.VolumeAnchorMigrationEntry) (*runnerv1.ObserveVolumeAnchorAdoptionResponse, error) {
	r, err := c.Native.ObserveVolumeAnchorAdoption(ctx, &runnerv1.ObserveVolumeAnchorAdoptionRequest{Adoption: e.Adoption})
	if err != nil {
		return nil, fmt.Errorf("observe original PVC readiness: %w", err)
	}
	if !proto.Equal(r, &runnerv1.ObserveVolumeAnchorAdoptionResponse{Adoption: e.Adoption, Volume: adoptedBinding(e), State: runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_READY}) {
		return nil, errors.New("independent native readiness missing or original adoption changed")
	}
	return r, nil
}

func operation(m *runnersv1.VolumeAnchorMigration, volume string) *runnersv1.AdvanceVolumeAnchorMigrationRequest {
	return &runnersv1.AdvanceVolumeAnchorMigrationRequest{OwnerKind: m.OwnerKind, OwnerId: m.OwnerId, Id: m.Id, ExpectedRevision: m.Revision, VolumeId: volume}
}

func adoptedBinding(e *runnersv1.VolumeAnchorMigrationEntry) *runnerv1.VolumeListItem {
	v := proto.Clone(e.Source.Previous).(*runnerv1.VolumeListItem)
	v.Anchor = proto.Clone(e.Adoption.Anchor).(*runnerv1.ResourceAnchor)
	return v
}

func canonicalUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}

func validateAdoption(e *runnersv1.VolumeAnchorMigrationEntry, a *runnerv1.VolumeAnchorAdoption) error {
	if a == nil || !canonicalUUID(a.InstanceUid) || !canonicalUUID(a.GetAnchor().GetInstanceUid()) || a.Id != e.AdoptionId ||
		!proto.Equal(a.Previous, e.Source.Previous) || len(a.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("matching immutable native adoption required")
	}
	hash, err := hex.DecodeString(a.PvcSpecSha256)
	if err != nil || len(hash) != 32 || hex.EncodeToString(hash) != a.PvcSpecSha256 {
		return errors.New("original native PVC spec hash required")
	}
	intent := proto.Clone(a.Anchor).(*runnerv1.ResourceAnchor)
	intent.InstanceUid = ""
	if !proto.Equal(intent, e.Intent) {
		return errors.New("native adoption intent changed")
	}
	return nil
}

func validatePlan(plan *runnersv1.BeginVolumeAnchorMigrationRequest, m *runnersv1.VolumeAnchorMigration) error {
	if m == nil || m.Id != plan.Id || m.OwnerKind != plan.OwnerKind || m.OwnerId != plan.OwnerId || m.RunnerId != plan.RunnerId ||
		m.OrganizationId != plan.OrganizationId || m.BackendId != plan.BackendId || m.Revision == 0 || m.Revision >= 999999999999999998 ||
		len(m.Entries) != len(plan.Sources) || len(m.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("registry returned a different or incomplete owner migration")
	}
	for i, e := range m.Entries {
		if e == nil || !proto.Equal(e.Source, plan.Sources[i]) || len(e.ProtoReflect().GetUnknown()) != 0 {
			return errors.New("migration source inventory changed")
		}
		if e.UnresolvedReason != "" {
			if e.UnresolvedReason != "unbound_failed_generation" || e.Source.Previous != nil || e.Intent != nil || e.Adoption != nil || e.Applied != nil || e.Ready != nil || m.Complete {
				return errors.New("quarantine invented native storage authority")
			}
			continue
		}
		if e.Source.Previous == nil || !canonicalUUID(e.Source.VolumeId) || e.AdoptionId != uuid.NewSHA1(uuid.MustParse(m.Id), []byte(e.Source.VolumeId)).String() ||
			e.CheckedRevision < e.Source.ExpectedRevision || e.CheckedRevision > e.Source.ExpectedRevision+1 ||
			!proto.Equal(e.Intent, &runnerv1.ResourceAnchor{Kind: runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, ResourceId: e.Source.VolumeId, BackendId: m.BackendId, IdentityLabels: e.Source.Previous.IdentityLabels}) {
			return errors.New("original adoption intent or checked revision changed")
		}
		if e.Adoption != nil {
			if err := validateAdoption(e, e.Adoption); err != nil {
				return err
			}
		} else if e.Applied != nil || e.Ready != nil || m.Complete {
			return errors.New("migration progress has no adoption receipt")
		}
		if e.Applied != nil {
			if !proto.Equal(e.Applied, &runnerv1.ApplyVolumeAnchorAdoptionResponse{Adoption: e.Adoption, Volume: adoptedBinding(e), State: runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_APPLIED}) {
				return errors.New("migration lost original APPLIED binding")
			}
		} else if e.Ready != nil || m.Complete {
			return errors.New("migration progress has no persisted APPLIED binding")
		}
		if e.Ready != nil {
			if !proto.Equal(e.Ready, &runnerv1.ObserveVolumeAnchorAdoptionResponse{Adoption: e.Adoption, Volume: adoptedBinding(e), State: runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_READY}) {
				return errors.New("migration readiness does not match original binding")
			}
		} else if m.Complete {
			return errors.New("completed owner is missing independent native readiness")
		}
	}
	return nil
}
