package volumemigration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var lostReply = errors.New("simulated lost reply after commit")

type migrationFixture struct {
	t                *testing.T
	plan             *runnersv1.BeginVolumeAnchorMigrationRequest
	m                *runnersv1.VolumeAnchorMigration
	volumes          map[string]*runnersv1.Volume
	receipts         map[string]*runnerv1.VolumeAnchorAdoption
	states           map[string]runnerv1.VolumeAnchorAdoptionState
	lose             string
	mutateReserve    func(*runnerv1.VolumeAnchorAdoption)
	missingReadiness bool
	finalizes        int
}

func newMigrationFixture(t *testing.T, sandbox bool) *migrationFixture {
	f := &migrationFixture{t: t, volumes: map[string]*runnersv1.Volume{}, receipts: map[string]*runnerv1.VolumeAnchorAdoption{}, states: map[string]runnerv1.VolumeAnchorAdoptionState{}}
	f.plan = &runnersv1.BeginVolumeAnchorMigrationRequest{Id: uuid.NewString(), OwnerKind: runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
		OwnerId: uuid.NewString(), RunnerId: uuid.NewString(), OrganizationId: uuid.NewString(), BackendId: "kubernetes-namespace/v1/fixture/" + uuid.NewString()}
	if sandbox {
		f.plan.OwnerKind = runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX
	}
	f.m = &runnersv1.VolumeAnchorMigration{Id: f.plan.Id, OwnerKind: f.plan.OwnerKind, OwnerId: f.plan.OwnerId, RunnerId: f.plan.RunnerId, OrganizationId: f.plan.OrganizationId, BackendId: f.plan.BackendId, Revision: 1}
	for i := 0; i < 2; i++ {
		id := uuid.NewString()
		previous := &runnerv1.VolumeListItem{InstanceId: "pv-" + id, VolumeKey: id, InstanceUid: uuid.NewString(), BackendId: f.plan.BackendId, IdentityLabels: map[string]string{"volume_key": id}}
		source := &runnersv1.VolumeAnchorMigrationSource{VolumeId: id, ExpectedRevision: 2, Previous: previous}
		f.plan.Sources = append(f.plan.Sources, source)
		f.m.Entries = append(f.m.Entries, &runnersv1.VolumeAnchorMigrationEntry{Source: source, CheckedRevision: 2, AdoptionId: uuid.NewSHA1(uuid.MustParse(f.plan.Id), []byte(id)).String(),
			Intent: &runnerv1.ResourceAnchor{Kind: runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, ResourceId: id, BackendId: f.plan.BackendId, IdentityLabels: previous.IdentityLabels}})
		f.volumes[id] = &runnersv1.Volume{Meta: &runnersv1.EntityMeta{Id: id}, OwnerKind: f.plan.OwnerKind, OwnerId: f.plan.OwnerId, RunnerId: f.plan.RunnerId, OrganizationId: f.plan.OrganizationId,
			InstanceId: &previous.InstanceId, BoundInstance: previous, CheckedLifecycle: true, LifecycleRevision: 2, Status: runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE}
	}
	if f.plan.Sources[0].VolumeId > f.plan.Sources[1].VolumeId {
		f.plan.Sources[0], f.plan.Sources[1] = f.plan.Sources[1], f.plan.Sources[0]
		f.m.Entries[0], f.m.Entries[1] = f.m.Entries[1], f.m.Entries[0]
	}
	return f
}

func (f *migrationFixture) fail(stage string) error {
	if f.lose == stage {
		f.lose = ""
		return lostReply
	}
	return nil
}

func (f *migrationFixture) BeginVolumeAnchorMigration(_ context.Context, req *runnersv1.BeginVolumeAnchorMigrationRequest, _ ...grpc.CallOption) (*runnersv1.BeginVolumeAnchorMigrationResponse, error) {
	if !proto.Equal(req, f.plan) {
		f.t.Fatal("begin retargeted original plan")
	}
	return &runnersv1.BeginVolumeAnchorMigrationResponse{Migration: proto.Clone(f.m).(*runnersv1.VolumeAnchorMigration)}, f.fail("begin")
}

func (f *migrationFixture) GetVolumeAnchorMigration(_ context.Context, _ *runnersv1.GetVolumeAnchorMigrationRequest, _ ...grpc.CallOption) (*runnersv1.GetVolumeAnchorMigrationResponse, error) {
	return &runnersv1.GetVolumeAnchorMigrationResponse{Migration: proto.Clone(f.m).(*runnersv1.VolumeAnchorMigration)}, nil
}

func (f *migrationFixture) GetVolume(_ context.Context, req *runnersv1.GetVolumeRequest, _ ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error) {
	return &runnersv1.GetVolumeResponse{Volume: proto.Clone(f.volumes[req.Id]).(*runnersv1.Volume)}, nil
}

func (f *migrationFixture) AdvanceVolumeAnchorMigration(_ context.Context, req *runnersv1.AdvanceVolumeAnchorMigrationRequest, _ ...grpc.CallOption) (*runnersv1.AdvanceVolumeAnchorMigrationResponse, error) {
	if req.Id != f.m.Id || req.ExpectedRevision != f.m.Revision || f.m.Complete {
		f.t.Fatal("invalid migration CAS")
	}
	stage := ""
	for _, e := range f.m.Entries {
		if e.Source.VolumeId != req.VolumeId {
			continue
		}
		switch op := req.Operation.(type) {
		case *runnersv1.AdvanceVolumeAnchorMigrationRequest_Reserve:
			e.Adoption, stage = proto.Clone(op.Reserve).(*runnerv1.VolumeAnchorAdoption), "reserved"
		case *runnersv1.AdvanceVolumeAnchorMigrationRequest_Apply:
			if e.Adoption == nil {
				f.t.Fatal("applied before reserve persisted")
			}
			e.Applied, stage = proto.Clone(op.Apply).(*runnerv1.ApplyVolumeAnchorAdoptionResponse), "applied"
			v := f.volumes[e.Source.VolumeId]
			v.BoundInstance, v.ResourceAnchor, v.AnchorAdoption = op.Apply.Volume, e.Adoption.Anchor, e.Adoption
			v.LifecycleRevision++
		case *runnersv1.AdvanceVolumeAnchorMigrationRequest_Ready:
			if e.Applied == nil {
				f.t.Fatal("ready before applied persisted")
			}
			e.Ready, stage = proto.Clone(op.Ready).(*runnerv1.ObserveVolumeAnchorAdoptionResponse), "ready"
		}
	}
	if req.GetComplete() != nil {
		for _, e := range f.m.Entries {
			if e.Ready == nil || e.UnresolvedReason != "" {
				f.t.Fatal("premature owner admission")
			}
		}
		f.m.Complete, stage = true, "complete"
	}
	if stage == "" {
		f.t.Fatal("unknown migration step")
	}
	f.m.Revision++
	return &runnersv1.AdvanceVolumeAnchorMigrationResponse{Migration: proto.Clone(f.m).(*runnersv1.VolumeAnchorMigration)}, f.fail(stage)
}

func (f *migrationFixture) ReserveVolumeAnchorAdoption(_ context.Context, req *runnerv1.ReserveVolumeAnchorAdoptionRequest, _ ...grpc.CallOption) (*runnerv1.ReserveVolumeAnchorAdoptionResponse, error) {
	a := f.receipts[req.Id]
	if a == nil {
		a = &runnerv1.VolumeAnchorAdoption{Id: req.Id, Previous: proto.Clone(req.Expected).(*runnerv1.VolumeListItem), Anchor: proto.Clone(req.Intent).(*runnerv1.ResourceAnchor), InstanceUid: uuid.NewString(), PvcSpecSha256: strings.Repeat("a", 64)}
		a.Anchor.InstanceUid = uuid.NewString()
		f.receipts[req.Id], f.states[req.Id] = a, runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_RESERVED
	}
	r := proto.Clone(a).(*runnerv1.VolumeAnchorAdoption)
	if f.mutateReserve != nil {
		f.mutateReserve(r)
	}
	return &runnerv1.ReserveVolumeAnchorAdoptionResponse{Adoption: r}, f.fail("native-reserved")
}

func (f *migrationFixture) ApplyVolumeAnchorAdoption(_ context.Context, req *runnerv1.ApplyVolumeAnchorAdoptionRequest, _ ...grpc.CallOption) (*runnerv1.ApplyVolumeAnchorAdoptionResponse, error) {
	e := f.entry(req.Adoption.Id)
	if e.Adoption == nil || !proto.Equal(e.Adoption, req.Adoption) {
		f.t.Fatal("applied without durable original receipt")
	}
	f.states[req.Adoption.Id] = runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_APPLIED
	return &runnerv1.ApplyVolumeAnchorAdoptionResponse{Adoption: req.Adoption, Volume: adoptedBinding(e), State: f.states[req.Adoption.Id]}, f.fail("native-applied")
}

func (f *migrationFixture) FinalizeVolumeAnchorAdoption(_ context.Context, req *runnerv1.FinalizeVolumeAnchorAdoptionRequest, _ ...grpc.CallOption) (*runnerv1.FinalizeVolumeAnchorAdoptionResponse, error) {
	e := f.entry(req.Adoption.Id)
	if e.Applied == nil || !proto.Equal(f.volumes[e.Source.VolumeId].BoundInstance, e.Applied.Volume) {
		f.t.Fatal("native finalized before APPLIED binding was durable")
	}
	f.finalizes++
	f.states[req.Adoption.Id] = runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_READY
	return &runnerv1.FinalizeVolumeAnchorAdoptionResponse{Adoption: req.Adoption, Volume: adoptedBinding(e), State: f.states[req.Adoption.Id]}, f.fail("native-ready")
}

func (f *migrationFixture) ObserveVolumeAnchorAdoption(_ context.Context, req *runnerv1.ObserveVolumeAnchorAdoptionRequest, _ ...grpc.CallOption) (*runnerv1.ObserveVolumeAnchorAdoptionResponse, error) {
	if f.missingReadiness {
		return nil, errors.New("original journal missing")
	}
	return &runnerv1.ObserveVolumeAnchorAdoptionResponse{Adoption: req.Adoption, Volume: adoptedBinding(f.entry(req.Adoption.Id)), State: f.states[req.Adoption.Id]}, nil
}

func (f *migrationFixture) entry(id string) *runnersv1.VolumeAnchorMigrationEntry {
	for _, e := range f.m.Entries {
		if e.AdoptionId == id {
			return e
		}
	}
	f.t.Fatal("new adoption identity introduced")
	return nil
}

func TestCoordinatorResumesCommittedBoundaries(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, stage := range []string{"begin", "native-reserved", "reserved", "native-applied", "applied", "native-ready", "ready", "complete"} {
			for _, lost := range []bool{false, true} {
				t.Run(fmt.Sprintf("sandbox=%t/%s/lost-reply=%t", sandbox, stage, lost), func(t *testing.T) {
					f := newMigrationFixture(t, sandbox)
					c := Coordinator{Registry: f, Native: f}
					if lost {
						f.lose = stage
					} else {
						c.Checkpoint = func(s string, _ *runnersv1.VolumeAnchorMigration) error {
							if s == stage {
								return lostReply
							}
							return nil
						}
					}
					if _, err := c.Run(context.Background(), f.plan); !errors.Is(err, lostReply) {
						t.Fatalf("did not stop at committed boundary: %v", err)
					}
					prior := map[string]*runnerv1.VolumeAnchorAdoption{}
					for id, a := range f.receipts {
						prior[id] = proto.Clone(a).(*runnerv1.VolumeAnchorAdoption)
					}
					resumed := Coordinator{Registry: f, Native: f}
					m, err := resumed.Run(context.Background(), f.plan)
					if err != nil || !m.GetComplete() || m.Revision != 8 {
						t.Fatalf("resume failed: %v", err)
					}
					for id, a := range prior {
						if !proto.Equal(a, f.receipts[id]) {
							t.Fatal("resume retargeted native journal, owner or PVC")
						}
					}
					if len(f.receipts) != 2 {
						t.Fatal("allocated extra adoption generation")
					}
					if _, err := resumed.Run(context.Background(), f.plan); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

func TestCoordinatorRejectsMalformedNativeReceipts(t *testing.T) {
	for name, mutate := range map[string]func(*runnerv1.VolumeAnchorAdoption){
		"pvc":     func(a *runnerv1.VolumeAnchorAdoption) { a.Previous.InstanceUid = uuid.NewString() },
		"intent":  func(a *runnerv1.VolumeAnchorAdoption) { a.Anchor.ResourceId = uuid.NewString() },
		"journal": func(a *runnerv1.VolumeAnchorAdoption) { a.InstanceUid = "" },
		"hash":    func(a *runnerv1.VolumeAnchorAdoption) { a.PvcSpecSha256 = "unknown" },
		"id":      func(a *runnerv1.VolumeAnchorAdoption) { a.Id = uuid.NewString() },
	} {
		t.Run(name, func(t *testing.T) {
			f := newMigrationFixture(t, false)
			f.mutateReserve = mutate
			if _, err := (Coordinator{Registry: f, Native: f}).Run(context.Background(), f.plan); err == nil || f.m.Revision != 1 || f.finalizes != 0 {
				t.Fatal("invalid native receipt advanced adoption")
			}
		})
	}
}

func TestCoordinatorRequiresIndependentReadiness(t *testing.T) {
	f := newMigrationFixture(t, false)
	f.missingReadiness = true
	if _, err := (Coordinator{Registry: f, Native: f}).Run(context.Background(), f.plan); err == nil || f.m.Complete || f.m.Entries[0].Ready != nil {
		t.Fatal("native finalization response substituted for independent observation")
	}
}

func TestCoordinatorQuarantinesWithoutNativeAuthority(t *testing.T) {
	f := newMigrationFixture(t, true)
	for _, e := range f.m.Entries {
		e.Source.Previous = nil
		e.Intent, e.CheckedRevision, e.AdoptionId = nil, 0, ""
		e.UnresolvedReason = "unbound_failed_generation"
	}
	if _, err := (Coordinator{Registry: f, Native: f}).Run(context.Background(), f.plan); !errors.Is(err, ErrQuarantined) || f.m.Complete || len(f.receipts) != 0 {
		t.Fatal("quarantine invented native storage or opened owner")
	}
}
