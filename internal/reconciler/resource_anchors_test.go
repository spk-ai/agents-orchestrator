package reconciler

import (
	"context"
	"fmt"
	"maps"
	"testing"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type anchoredOnlyRegistry struct {
	*fakeRunnersClient
	t *testing.T
}

func (r anchoredOnlyRegistry) CreatePreparedWorkload(context.Context, *runnersv1.CreatePreparedWorkloadRequest, ...grpc.CallOption) (*runnersv1.CreatePreparedWorkloadResponse, error) {
	r.t.Fatal("anchored start fell back to legacy registry creation")
	return nil, errNotImplemented
}

func (r anchoredOnlyRegistry) UpdatePreparedWorkload(context.Context, *runnersv1.UpdatePreparedWorkloadRequest, ...grpc.CallOption) (*runnersv1.UpdatePreparedWorkloadResponse, error) {
	r.t.Fatal("anchored transition fell back to single revision")
	return nil, errNotImplemented
}

type anchoredOnlyRunner struct {
	*fakeRunnerClient
	t *testing.T
}

func (r anchoredOnlyRunner) PrepareWorkload(context.Context, *runnerv1.PrepareWorkloadRequest, ...grpc.CallOption) (*runnerv1.PrepareWorkloadResponse, error) {
	r.t.Fatal("anchored start fell back to unanchored preparation")
	return nil, errNotImplemented
}

func TestResourceAnchorCapabilitiesNeverFallback(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, missing := range []string{"none", "create", "reserve", "volume-bind", "workload-bind", "transition", "prepare"} {
			t.Run(fmt.Sprintf("sandbox=%t/%s", sandbox, missing), func(t *testing.T) {
				f := newPreparedControllerFixture(t, sandbox)
				switch missing {
				case "create":
					f.registry.createAnchoredWorkload = nil
				case "reserve":
					f.native.reserveResourceAnchor = nil
				case "volume-bind":
					f.registry.updateVolumeChecked = nil
				case "workload-bind":
					f.registry.bindWorkloadResourceAnchors = nil
				case "transition":
					f.registry.updateAnchoredWorkload = nil
				case "prepare":
					f.native.prepareAnchoredWorkload = nil
				}
				f.r.runners = anchoredOnlyRegistry{f.registry, t}
				native := anchoredOnlyRunner{f.native, t}
				w, err := f.r.startPreparedWorkload(context.Background(), native, f.metadata, f.request, f.infos, f.created)
				if missing != "none" {
					if err == nil || f.activations != 0 || f.prepares != 0 {
						t.Fatal("unsupported capability allowed execution")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := f.r.stopPreparedWorkload(context.Background(), native, w); err != nil || w.RemovalConfirmedAt == nil {
					t.Fatalf("anchored lifecycle failed: %v", err)
				}
			})
		}
	}
}

func TestResourceAnchorRequestOwnerProjection(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, mode := range []string{"explicit", "properties", "explicit-override", "wrong-owner", "mixed-owner", "wrong-manager", "reserved-label", "reserved-property"} {
			t.Run(fmt.Sprintf("sandbox=%t/%s", sandbox, mode), func(t *testing.T) {
				f := newPreparedControllerFixture(t, sandbox)
				ownerKey := assembler.LabelInstanceID
				if sandbox {
					ownerKey = assembler.LabelSandboxOwnerID
				}
				switch mode {
				case "properties":
					f.request.AdditionalProperties = map[string]string{}
					for key, value := range f.request.Labels {
						f.request.AdditionalProperties[assembler.LabelKeyPrefix+key] = value
					}
					f.request.Labels = nil
				case "explicit-override":
					f.request.AdditionalProperties = map[string]string{assembler.LabelKeyPrefix + ownerKey: uuid.NewString()}
				case "wrong-owner":
					f.request.Labels[ownerKey] = uuid.NewString()
				case "mixed-owner":
					key := assembler.LabelSandboxID
					if sandbox {
						key = assembler.LabelInstanceID
					}
					f.request.Labels[key] = uuid.NewString()
				case "wrong-manager":
					f.request.Labels[assembler.LabelManagedBy] = "other"
				case "reserved-label":
					f.request.Labels["app.kubernetes.io/managed-by"] = "k8s-runner"
				case "reserved-property":
					f.request.AdditionalProperties = map[string]string{"label.agyn.dev/managed-by": "agents-orchestrator"}
				}
				reservations := 0
				reserve := f.native.reserveResourceAnchor
				f.native.reserveResourceAnchor = func(ctx context.Context, req *runnerv1.ReserveResourceAnchorRequest, opts ...grpc.CallOption) (*runnerv1.ReserveResourceAnchorResponse, error) {
					reservations++
					return reserve(ctx, req, opts...)
				}
				w, err := f.start()
				if mode == "explicit" || mode == "properties" || mode == "explicit-override" {
					if err != nil || reservations != 2 || f.activations != 1 {
						t.Fatalf("valid native label projection rejected: %v", err)
					}
					labels, err := preparedRequestOwnerLabels(f.request)
					if err != nil || !maps.Equal(labels, w.Preparation.Resources.Workload.IdentityLabels) {
						t.Fatal("persisted owner differs from assembled request")
					}
				} else if err == nil || reservations != 0 || f.prepares != 0 || f.activations != 0 {
					t.Fatal("invalid request owner reached native metadata or execution")
				}
			})
		}
	}
}

func TestResourceAnchorRejectsNativeReservationMismatch(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, fault := range []string{"nil", "uid", "kind", "resource", "backend", "owner", "thread", "unknown"} {
			t.Run(fmt.Sprintf("sandbox=%t/%s", sandbox, fault), func(t *testing.T) {
				f := newPreparedControllerFixture(t, sandbox)
				reserve := f.native.reserveResourceAnchor
				f.native.reserveResourceAnchor = func(ctx context.Context, req *runnerv1.ReserveResourceAnchorRequest, opts ...grpc.CallOption) (*runnerv1.ReserveResourceAnchorResponse, error) {
					response, err := reserve(ctx, req, opts...)
					if err != nil {
						return response, err
					}
					switch fault {
					case "nil":
						return nil, nil
					case "uid":
						response.Anchor.InstanceUid = ""
					case "kind":
						response.Anchor.Kind = runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME
					case "resource":
						response.Anchor.ResourceId = uuid.NewString()
					case "backend":
						response.Anchor.BackendId = "replacement-backend"
					case "owner":
						response.Anchor.IdentityLabels["sandbox-owner-id"] = uuid.NewString()
					case "thread":
						response.Anchor.IdentityLabels["thread-id"] = uuid.NewString()
					case "unknown":
						response.Anchor.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
					}
					return response, nil
				}
				if _, err := f.start(); err == nil || f.prepares != 0 || f.activations != 0 || f.v.ResourceAnchor != nil {
					t.Fatal("unverified native owner granted preparation authority")
				}
			})
		}
	}
}

func TestResourceAnchorRejectsRegistryBindingMismatch(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, fault := range []string{"resource-revision", "preparation-revision", "workload-uid", "volume-uid", "volume-set", "receipt"} {
			t.Run(fmt.Sprintf("sandbox=%t/%s", sandbox, fault), func(t *testing.T) {
				f := newPreparedControllerFixture(t, sandbox)
				if fault == "receipt" {
					update := f.registry.updateVolumeChecked
					f.registry.updateVolumeChecked = func(ctx context.Context, req *runnersv1.UpdateVolumeCheckedRequest, opts ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
						response, err := update(ctx, req, opts...)
						if err == nil && req.GetBindAnchor() != nil {
							response.Volume.AnchorReservation.WorkloadId = uuid.NewString()
						}
						return response, err
					}
				} else {
					bind := f.registry.bindWorkloadResourceAnchors
					f.registry.bindWorkloadResourceAnchors = func(ctx context.Context, req *runnersv1.BindWorkloadResourceAnchorsRequest, opts ...grpc.CallOption) (*runnersv1.BindWorkloadResourceAnchorsResponse, error) {
						response, err := bind(ctx, req, opts...)
						if err != nil {
							return response, err
						}
						p := response.Workload.Preparation
						switch fault {
						case "resource-revision":
							p.Resources.Revision++
						case "preparation-revision":
							p.Revision++
						case "workload-uid":
							p.Resources.Workload.InstanceUid = uuid.NewString()
						case "volume-uid":
							p.Resources.Volumes[0].InstanceUid = uuid.NewString()
						case "volume-set":
							p.Resources.Volumes = nil
						}
						return response, nil
					}
				}
				if _, err := f.start(); err == nil || f.prepares != 0 || f.activations != 0 {
					t.Fatal("unverified registry persistence granted execution")
				}
			})
		}
	}
}

func TestResourceAnchorRemovalMustBeExactAndAbsent(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, fault := range []string{"pending", "nil", "uid", "backend", "unavailable", "unknown-state"} {
			t.Run(fmt.Sprintf("sandbox=%t/%s", sandbox, fault), func(t *testing.T) {
				f := newPreparedControllerFixture(t, sandbox)
				w, err := f.start()
				if err != nil {
					t.Fatal(err)
				}
				workspace := proto.Clone(f.v).(*runnersv1.Volume)
				remove := f.native.removeWorkloadAnchor
				f.native.removeWorkloadAnchor = func(_ context.Context, req *runnerv1.RemoveWorkloadAnchorRequest, _ ...grpc.CallOption) (*runnerv1.RemoveWorkloadAnchorResponse, error) {
					if f.binding != nil || f.active || f.removals != 1 {
						t.Fatal("owner revocation preceded exact Pod absence")
					}
					response := &runnerv1.RemoveWorkloadAnchorResponse{Anchor: proto.Clone(req.Expected).(*runnerv1.ResourceAnchor), State: runnerv1.ResourceAnchorRemovalState_RESOURCE_ANCHOR_REMOVAL_STATE_ABSENT}
					switch fault {
					case "pending":
						response.State = runnerv1.ResourceAnchorRemovalState_RESOURCE_ANCHOR_REMOVAL_STATE_PENDING
					case "nil":
						return nil, nil
					case "uid":
						response.Anchor.InstanceUid = uuid.NewString()
					case "backend":
						response.Anchor.BackendId = "other"
					case "unavailable":
						return nil, status.Error(codes.Unavailable, "unknown revocation")
					case "unknown-state":
						response.State = 99
					}
					return response, nil
				}
				if err := f.r.stopPreparedWorkload(context.Background(), f.native, w); err == nil || f.w.RemovalConfirmedAt != nil || w.RemovalConfirmedAt != nil || !proto.Equal(workspace, f.v) {
					t.Fatal("unverified revocation released admission or changed persistent ownership")
				}
				f.native.removeWorkloadAnchor = remove
				if err := f.r.stopPreparedWorkload(context.Background(), f.native, w); err != nil || w.RemovalConfirmedAt == nil || f.prepares != 1 || f.activations != 1 {
					t.Fatalf("exact revocation did not recover without redispatch: %v", err)
				}
			})
		}
	}
}

func TestResourceAnchorLostRepliesDoNotRedispatch(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, boundary := range []string{"workload-reserve", "volume-reserve", "volume-bind", "workload-bind", "begin-prepare", "prepare"} {
			t.Run(fmt.Sprintf("sandbox=%t/%s", sandbox, boundary), func(t *testing.T) {
				f := newPreparedControllerFixture(t, sandbox)
				lost := status.Error(codes.Unavailable, "lost committed reply")
				switch boundary {
				case "workload-reserve", "volume-reserve":
					reserve := f.native.reserveResourceAnchor
					f.native.reserveResourceAnchor = func(ctx context.Context, req *runnerv1.ReserveResourceAnchorRequest, opts ...grpc.CallOption) (*runnerv1.ReserveResourceAnchorResponse, error) {
						response, err := reserve(ctx, req, opts...)
						if err == nil && (boundary == "workload-reserve" || req.Intent.Kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME) {
							return nil, lost
						}
						return response, err
					}
				case "volume-bind":
					update := f.registry.updateVolumeChecked
					f.registry.updateVolumeChecked = func(ctx context.Context, req *runnersv1.UpdateVolumeCheckedRequest, opts ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
						response, err := update(ctx, req, opts...)
						if err == nil && req.GetBindAnchor() != nil {
							return nil, lost
						}
						return response, err
					}
				case "workload-bind":
					bind := f.registry.bindWorkloadResourceAnchors
					f.registry.bindWorkloadResourceAnchors = func(ctx context.Context, req *runnersv1.BindWorkloadResourceAnchorsRequest, opts ...grpc.CallOption) (*runnersv1.BindWorkloadResourceAnchorsResponse, error) {
						_, err := bind(ctx, req, opts...)
						if err != nil {
							return nil, err
						}
						return nil, lost
					}
				case "begin-prepare":
					update := f.registry.updateAnchoredWorkload
					f.registry.updateAnchoredWorkload = func(ctx context.Context, req *runnersv1.UpdateAnchoredWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.UpdateAnchoredWorkloadResponse, error) {
						response, err := update(ctx, req, opts...)
						if err == nil && req.Operation.GetBeginPreparation() != nil {
							return nil, lost
						}
						return response, err
					}
				case "prepare":
					prepare := f.native.prepareAnchoredWorkload
					f.native.prepareAnchoredWorkload = func(ctx context.Context, req *runnerv1.PrepareAnchoredWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.PrepareAnchoredWorkloadResponse, error) {
						_, err := prepare(ctx, req, opts...)
						if err != nil {
							return nil, err
						}
						return nil, lost
					}
				}
				revocations := 0
				revoke := f.native.removeWorkloadAnchor
				f.native.removeWorkloadAnchor = func(ctx context.Context, req *runnerv1.RemoveWorkloadAnchorRequest, opts ...grpc.CallOption) (*runnerv1.RemoveWorkloadAnchorResponse, error) {
					revocations++
					return revoke(ctx, req, opts...)
				}
				if _, err := f.start(); err == nil || f.activations != 0 || f.prepares > 1 {
					t.Fatal("ambiguous reply activated or repeated execution")
				}
				if boundary == "begin-prepare" || boundary == "prepare" {
					if f.w.RemovalConfirmedAt != nil || f.w.Preparation.Binding != nil || revocations != 1 {
						t.Fatal("unknown preparation did not revoke ownership while retaining admission")
					}
					if err := f.r.stopPreparedWorkload(context.Background(), f.native, f.w); err == nil || f.w.RemovalConfirmedAt != nil || f.prepares > 1 {
						t.Fatal("owner absence incorrectly proved child cleanup")
					}
				} else if f.w.RemovalConfirmedAt == nil || f.prepares != 0 {
					t.Fatal("unused reservation did not retire without execution")
				}
			})
		}
	}
}

func TestResourceAnchorInboxThreadChangesOnlyWithNewWorkload(t *testing.T) {
	f := newPreparedControllerFixture(t, false)
	first, err := f.start()
	if err != nil {
		t.Fatal(err)
	}
	workspace := proto.Clone(f.v).(*runnersv1.Volume)
	thread := first.Preparation.Resources.Workload.IdentityLabels["thread-id"]
	if thread == f.v.ThreadId {
		t.Fatal("fixture reused the instance alias as an inbox thread")
	}
	if err := f.r.stopPreparedWorkload(context.Background(), f.native, first); err != nil {
		t.Fatal(err)
	}
	f.metadata.Id = uuid.NewString()
	f.request.WorkloadId, f.request.Labels["thread-id"], f.created = f.metadata.Id, uuid.NewString(), nil
	next, err := f.start()
	if err != nil || next.Preparation.Resources.Workload.IdentityLabels["thread-id"] == thread || !proto.Equal(workspace, f.v) {
		t.Fatalf("new inbox thread could not reuse persistent instance state: %v", err)
	}
	changed := proto.Clone(next).(*runnersv1.Workload)
	changed.Preparation.Resources.Workload.IdentityLabels["thread-id"] = uuid.NewString()
	if err := validatePreparedSuccessor(next, changed); err == nil {
		t.Fatal("existing native workload changed its inbox thread")
	}
}

func TestResourceAnchorCancellationBeforePreparation(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, boundary := range []string{"volume-reserve", "workload-bind", "begin-prepare"} {
			t.Run(fmt.Sprintf("sandbox=%t/%s", sandbox, boundary), func(t *testing.T) {
				f := newPreparedControllerFixture(t, sandbox)
				cancel := func(ctx context.Context) {
					if err := f.r.stopPreparedWorkload(ctx, f.native, proto.Clone(f.w).(*runnersv1.Workload)); err != nil {
						t.Fatal(err)
					}
				}
				switch boundary {
				case "volume-reserve":
					reserve := f.native.reserveResourceAnchor
					f.native.reserveResourceAnchor = func(ctx context.Context, req *runnerv1.ReserveResourceAnchorRequest, opts ...grpc.CallOption) (*runnerv1.ReserveResourceAnchorResponse, error) {
						response, err := reserve(ctx, req, opts...)
						if err == nil && req.Intent.Kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME {
							cancel(ctx)
						}
						return response, err
					}
				case "workload-bind":
					bind := f.registry.bindWorkloadResourceAnchors
					f.registry.bindWorkloadResourceAnchors = func(ctx context.Context, req *runnersv1.BindWorkloadResourceAnchorsRequest, opts ...grpc.CallOption) (*runnersv1.BindWorkloadResourceAnchorsResponse, error) {
						response, err := bind(ctx, req, opts...)
						if err == nil {
							cancel(ctx)
						}
						return response, err
					}
				case "begin-prepare":
					update := f.registry.updateAnchoredWorkload
					f.registry.updateAnchoredWorkload = func(ctx context.Context, req *runnersv1.UpdateAnchoredWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.UpdateAnchoredWorkloadResponse, error) {
						if req.Operation.GetBeginPreparation() != nil {
							cancel(ctx)
						}
						return update(ctx, req, opts...)
					}
				}
				if _, err := f.start(); err == nil || f.prepares != 0 || f.activations != 0 || f.w.RemovalConfirmedAt == nil || f.w.Preparation.Binding != nil {
					t.Fatal("canceled metadata reservation granted native preparation authority")
				}
			})
		}
	}
}
