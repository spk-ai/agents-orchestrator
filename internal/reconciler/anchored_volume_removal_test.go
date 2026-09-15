package reconciler

import (
	"context"
	"fmt"
	"testing"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func retirementControllerFixture(t *testing.T, sandbox bool) *preparedControllerFixture {
	t.Helper()
	f := newPreparedControllerFixture(t, sandbox)
	w, err := f.r.startPreparedWorkload(context.Background(), f.native, f.metadata, f.request, f.infos, f.created)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.r.stopPreparedWorkload(context.Background(), f.native, w); err != nil {
		t.Fatal(err)
	}
	f.native.removeVolumeBound = func(context.Context, *runnerv1.RemoveVolumeBoundRequest, ...grpc.CallOption) (*runnerv1.RemoveVolumeBoundResponse, error) {
		t.Fatal("anchored retirement fell back to old native deletion")
		return nil, errNotImplemented
	}
	return f
}

func TestAnchoredVolumeControllerRecoversOriginalIntent(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, boundary := range []string{"begin", "native", "confirm", "pending"} {
			t.Run(fmt.Sprintf("sandbox=%t/%s", sandbox, boundary), func(t *testing.T) {
				f := retirementControllerFixture(t, sandbox)
				before := proto.Clone(f.v).(*runnersv1.Volume)
				update := f.registry.updateVolumeChecked
				lost, nativeCalls := false, 0
				f.registry.updateVolumeChecked = func(ctx context.Context, req *runnersv1.UpdateVolumeCheckedRequest, opts ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
					if req.GetBeginAnchoredRemoval() == nil && req.GetConfirmAnchoredRemoval() == nil {
						t.Fatal("old registry retirement operation")
					}
					resp, err := update(ctx, req, opts...)
					if !lost && (boundary == "begin" && req.GetBeginAnchoredRemoval() != nil || boundary == "confirm" && req.GetConfirmAnchoredRemoval() != nil) {
						lost = true
						return nil, status.Error(codes.Unavailable, "lost committed ACK")
					}
					return resp, err
				}
				f.native.removeVolumeAnchored = func(_ context.Context, req *runnerv1.RemoveVolumeAnchoredRequest, _ ...grpc.CallOption) (*runnerv1.RemoveVolumeAnchoredResponse, error) {
					nativeCalls++
					if f.v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING || !proto.Equal(req.Expected, before.BoundInstance) || !f.v.RemovalIntent.GetAnchored() {
						t.Fatal("native deletion before exact durable intent")
					}
					if !lost && boundary == "native" {
						lost = true
						return nil, status.Error(codes.Unavailable, "lost native ACK")
					}
					state := runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT
					if !lost && boundary == "pending" {
						lost = true
						state = runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING
					}
					return &runnerv1.RemoveVolumeAnchoredResponse{State: state, BackendId: req.Expected.BackendId, Anchor: req.Expected.Anchor}, nil
				}
				done, err := f.r.advanceVolumeRemoval(context.Background(), f.native, proto.Clone(f.v).(*runnersv1.Volume))
				if done || boundary != "pending" && err == nil || boundary == "pending" && err != nil {
					t.Fatalf("ambiguous/pending retirement settled: %v", err)
				}
				intent := proto.Clone(f.v.RemovalIntent).(*runnersv1.VolumeRemovalIntent)
				priorCalls := nativeCalls
				restarted := &Reconciler{runners: f.registry}
				done, err = restarted.advanceVolumeRemoval(context.Background(), f.native, proto.Clone(f.v).(*runnersv1.Volume))
				if err != nil || !done {
					t.Fatalf("recovery: %v", err)
				}
				intent.ConfirmedAt = f.v.RemovalIntent.ConfirmedAt
				if !proto.Equal(intent, f.v.RemovalIntent) || !proto.Equal(before.ResourceAnchor, f.v.ResourceAnchor) ||
					!proto.Equal(before.BoundInstance, f.v.BoundInstance) || f.v.AnchoredRemovalObservation == nil || f.prepares != 1 || f.activations != 1 {
					t.Fatal("recovery changed ownership or replayed execution")
				}
				if boundary == "confirm" && nativeCalls != priorCalls {
					t.Fatal("confirmed retirement called native again")
				}
			})
		}
	}
}

func TestAnchoredVolumeControllerRejectsNativeReplies(t *testing.T) {
	for name, mutate := range map[string]func(*runnerv1.RemoveVolumeAnchoredResponse) (*runnerv1.RemoveVolumeAnchoredResponse, error){
		"nil": func(*runnerv1.RemoveVolumeAnchoredResponse) (*runnerv1.RemoveVolumeAnchoredResponse, error) {
			return nil, nil
		},
		"old-runner": func(*runnerv1.RemoveVolumeAnchoredResponse) (*runnerv1.RemoveVolumeAnchoredResponse, error) {
			return nil, status.Error(codes.Unimplemented, "old runner")
		},
		"canceled": func(*runnerv1.RemoveVolumeAnchoredResponse) (*runnerv1.RemoveVolumeAnchoredResponse, error) {
			return nil, context.Canceled
		},
		"wrong-backend": func(o *runnerv1.RemoveVolumeAnchoredResponse) (*runnerv1.RemoveVolumeAnchoredResponse, error) {
			o.BackendId = "other"
			return o, nil
		},
		"wrong-owner": func(o *runnerv1.RemoveVolumeAnchoredResponse) (*runnerv1.RemoveVolumeAnchoredResponse, error) {
			o.Anchor.InstanceUid = uuid.NewString()
			return o, nil
		},
		"unknown-state": func(o *runnerv1.RemoveVolumeAnchoredResponse) (*runnerv1.RemoveVolumeAnchoredResponse, error) {
			o.State = 99
			return o, nil
		},
		"unknown-wire": func(o *runnerv1.RemoveVolumeAnchoredResponse) (*runnerv1.RemoveVolumeAnchoredResponse, error) {
			o.ProtoReflect().SetUnknown([]byte{0x78, 1})
			return o, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := retirementControllerFixture(t, false)
			f.native.removeVolumeAnchored = func(_ context.Context, req *runnerv1.RemoveVolumeAnchoredRequest, _ ...grpc.CallOption) (*runnerv1.RemoveVolumeAnchoredResponse, error) {
				return mutate(&runnerv1.RemoveVolumeAnchoredResponse{State: runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT, BackendId: req.Expected.BackendId, Anchor: proto.Clone(req.Expected.Anchor).(*runnerv1.ResourceAnchor)})
			}
			if done, err := f.r.advanceVolumeRemoval(context.Background(), f.native, proto.Clone(f.v).(*runnersv1.Volume)); done || err == nil || f.v.AnchoredRemovalObservation != nil || f.v.RemovalIntent.ConfirmedAt != nil {
				t.Fatal("unconfirmed native result settled retirement")
			}
		})
	}
}

func TestAnchoredVolumeControllerRejectsRegistryReplies(t *testing.T) {
	for _, phase := range []string{"begin", "confirm"} {
		for _, field := range []string{"binding", "anchor", "reservation", "intent", "observation"} {
			t.Run(phase+"/"+field, func(t *testing.T) {
				f := retirementControllerFixture(t, true)
				update := f.registry.updateVolumeChecked
				calls := 0
				f.registry.updateVolumeChecked = func(ctx context.Context, req *runnersv1.UpdateVolumeCheckedRequest, opts ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
					resp, err := update(ctx, req, opts...)
					if err != nil {
						return resp, err
					}
					if phase == "begin" && req.GetBeginAnchoredRemoval() != nil || phase == "confirm" && req.GetConfirmAnchoredRemoval() != nil {
						switch field {
						case "binding":
							resp.Volume.BoundInstance.InstanceUid = uuid.NewString()
						case "anchor":
							resp.Volume.ResourceAnchor.InstanceUid = uuid.NewString()
						case "reservation":
							resp.Volume.AnchorReservation.WorkloadId = uuid.NewString()
						case "intent":
							resp.Volume.RemovalIntent.Anchored = false
						case "observation":
							if phase == "begin" {
								resp.Volume.AnchoredRemovalObservation = &runnerv1.RemoveVolumeAnchoredResponse{}
							} else {
								resp.Volume.AnchoredRemovalObservation = nil
							}
						}
					}
					return resp, nil
				}
				f.native.removeVolumeAnchored = func(_ context.Context, req *runnerv1.RemoveVolumeAnchoredRequest, _ ...grpc.CallOption) (*runnerv1.RemoveVolumeAnchoredResponse, error) {
					calls++
					return &runnerv1.RemoveVolumeAnchoredResponse{State: runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT, BackendId: req.Expected.BackendId, Anchor: req.Expected.Anchor}, nil
				}
				if done, err := f.r.advanceVolumeRemoval(context.Background(), f.native, proto.Clone(f.v).(*runnersv1.Volume)); done || err == nil {
					t.Fatal("corrupt registry ACK accepted")
				}
				if phase == "begin" && calls != 0 {
					t.Fatal("bad intent ACK authorized native deletion")
				}
			})
		}
	}
}
