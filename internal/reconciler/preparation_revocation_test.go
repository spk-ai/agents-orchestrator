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

func installRevocationFixture(t *testing.T, f *preparedControllerFixture, binding *runnerv1.WorkloadBinding, absent bool) *runnerv1.ObservePreparationRevocationResponse {
	t.Helper()
	resources := f.w.Preparation.Resources
	observation := &runnerv1.ObservePreparationRevocationResponse{
		Revocation: &runnerv1.PreparationRevocation{WorkloadAnchor: proto.Clone(resources.Workload).(*runnerv1.ResourceAnchor),
			VolumeAnchors: resources.Volumes, InstanceUid: uuid.NewString(), SelectedPodUid: binding.InstanceUid},
		State: runnerv1.RevokedPreparationState_REVOKED_PREPARATION_STATE_POD_ABSENT}
	if absent {
		observation.Revocation.SelectedPodUid = ""
		for _, a := range resources.Volumes {
			observation.AbsentVolumeIds = append(observation.AbsentVolumeIds, a.ResourceId)
		}
	} else {
		observation.Volumes = proto.Clone(binding).(*runnerv1.WorkloadBinding).Volumes
	}
	f.native.revokeWorkloadPreparation = func(_ context.Context, req *runnerv1.RevokeWorkloadPreparationRequest, _ ...grpc.CallOption) (*runnerv1.RevokeWorkloadPreparationResponse, error) {
		if f.w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING || f.w.Preparation.Binding != nil ||
			!proto.Equal(req.WorkloadAnchor, resources.Workload) || len(req.VolumeAnchors) != len(resources.Volumes) {
			t.Fatal("native revocation before durable unbound intent or with different owners")
		}
		for i, a := range req.VolumeAnchors {
			if !proto.Equal(a, resources.Volumes[i]) {
				t.Fatal("revocation changed a volume owner")
			}
		}
		if f.active {
			return nil, status.Error(codes.FailedPrecondition, "activation already claimed")
		}
		return &runnerv1.RevokeWorkloadPreparationResponse{Revocation: proto.Clone(observation.Revocation).(*runnerv1.PreparationRevocation)}, nil
	}
	f.native.observePreparationRevocation = func(_ context.Context, req *runnerv1.ObservePreparationRevocationRequest, _ ...grpc.CallOption) (*runnerv1.ObservePreparationRevocationResponse, error) {
		if f.w.Preparation.Resources.PreparationRevocation == nil || !proto.Equal(req.Expected, f.w.Preparation.Resources.PreparationRevocation) || f.w.RemovalConfirmedAt != nil {
			t.Fatal("observation before durable revocation proof")
		}
		// Unit-level protocol response, not evidence of real Kubernetes GC.
		if observation.State == runnerv1.RevokedPreparationState_REVOKED_PREPARATION_STATE_POD_ABSENT {
			f.binding = nil
		}
		return proto.Clone(observation).(*runnerv1.ObservePreparationRevocationResponse), nil
	}
	return observation
}

func TestPreparationRevocationRecoveryAndExplicitFollowup(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, mode := range []string{"zero", "absent", "found", "existing"} {
			t.Run(fmt.Sprintf("sandbox=%t/%s", sandbox, mode), func(t *testing.T) {
				f, binding, prepare := lostPreparedOutcome(t, sandbox, mode)
				observation := installRevocationFixture(t, f, binding, mode == "absent")
				original := proto.Clone(f.v).(*runnersv1.Volume)
				previous := proto.Clone(f.w).(*runnersv1.Workload)
				if err := f.r.stopPreparedWorkload(context.Background(), f.native, previous); err != nil {
					t.Fatal(err)
				}
				if f.w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED || f.w.RemovalConfirmedAt == nil ||
					f.w.Preparation.Binding != nil || f.w.InstanceId != nil || !proto.Equal(f.w.Preparation.Resources.RevocationObservation, observation) ||
					f.prepares != 1 || f.activations != 0 || f.removals != 0 {
					t.Fatal("revocation fabricated a Pod binding, replayed work, or skipped persisted evidence")
				}
				if mode == "absent" && !proto.Equal(f.v, original) {
					t.Fatal("absent first provision changed its reserved workspace")
				}
				if mode == "found" || mode == "existing" {
					if !proto.Equal(f.v.BoundInstance, binding.Volumes[0]) || !proto.Equal(f.v.ResourceAnchor, original.ResourceAnchor) || !proto.Equal(f.v.AnchorReservation, original.AnchorReservation) {
						t.Fatal("revocation replaced a workspace identity or reservation")
					}
				}
				f.r = &Reconciler{runners: f.registry, agents: f.r.agents}
				f.native.observePreparationRevocation = func(context.Context, *runnerv1.ObservePreparationRevocationRequest, ...grpc.CallOption) (*runnerv1.ObservePreparationRevocationResponse, error) {
					t.Fatal("completed recovery repeated native cleanup")
					return nil, nil
				}
				if err := f.r.stopPreparedWorkload(context.Background(), f.native, previous); err != nil {
					t.Fatal(err)
				}
				f.native.prepareWorkload = prepare
				f.metadata.Id = uuid.NewString()
				f.request.WorkloadId, f.created = f.metadata.Id, nil
				resumed, err := f.start()
				if err != nil || resumed.GetPreparation().GetBinding().GetInstanceUid() == binding.InstanceUid {
					t.Fatalf("explicit new workload could not proceed: %v", err)
				}
				if mode == "found" || mode == "existing" {
					if !proto.Equal(resumed.Preparation.Binding.Volumes[0], binding.Volumes[0]) {
						t.Fatal("follow-up replaced the retained workspace")
					}
				}
			})
		}
	}
}

func TestPreparationRevocationPendingRetainsAdmission(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		t.Run(fmt.Sprint(sandbox), func(t *testing.T) {
			f, binding, _ := lostPreparedOutcome(t, sandbox, "first")
			observation := installRevocationFixture(t, f, binding, false)
			observation.State = runnerv1.RevokedPreparationState_REVOKED_PREPARATION_STATE_PENDING
			if err := f.r.stopPreparedWorkload(context.Background(), f.native, proto.Clone(f.w).(*runnersv1.Workload)); err == nil || f.w.RemovalConfirmedAt != nil || f.v.BoundInstance != nil || f.w.Preparation.Resources.PreparationRevocation == nil {
				t.Fatal("pending native cleanup released admission or bound a workspace")
			}
			f.native.revokeWorkloadPreparation = func(context.Context, *runnerv1.RevokeWorkloadPreparationRequest, ...grpc.CallOption) (*runnerv1.RevokeWorkloadPreparationResponse, error) {
				t.Fatal("persisted revocation was redispatched")
				return nil, nil
			}
			f.native.observeWorkloadPreparation = func(context.Context, *runnerv1.ObserveWorkloadPreparationRequest, ...grpc.CallOption) (*runnerv1.ObserveWorkloadPreparationResponse, error) {
				t.Fatal("revoked preparation was rediscovered as an ordinary binding")
				return nil, nil
			}
			if _, err := f.r.persistPreparedBinding(context.Background(), f.w, binding); err == nil || f.w.Preparation.Binding != nil {
				t.Fatal("late prepare reply replaced revocation proof")
			}
			observation.State = runnerv1.RevokedPreparationState_REVOKED_PREPARATION_STATE_POD_ABSENT
			f.r = &Reconciler{runners: f.registry, agents: f.r.agents}
			if err := f.r.stopPreparedWorkload(context.Background(), f.native, proto.Clone(f.w).(*runnersv1.Workload)); err != nil || f.w.RemovalConfirmedAt == nil {
				t.Fatalf("restart could not finish persisted cleanup: %v", err)
			}
		})
	}
}

func TestPreparationRevocationRejectsUnverifiedEvidence(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, fault := range []string{"receipt-nil", "receipt-uid", "receipt-pod", "receipt-workload", "receipt-backend", "receipt-volumes", "receipt-unknown", "observation-nil", "observation-proof", "observation-state", "observation-partition", "observation-volume-uid", "observation-owner", "observation-unknown", "known-missing", "known-replaced", "record-owner", "record-generation", "activation-claimed"} {
			t.Run(fmt.Sprintf("sandbox=%t/%s", sandbox, fault), func(t *testing.T) {
				mode := "first"
				if fault == "known-missing" || fault == "known-replaced" {
					mode = "existing"
				}
				f, binding, _ := lostPreparedOutcome(t, sandbox, mode)
				observation := installRevocationFixture(t, f, binding, fault == "known-missing")
				revoke := f.native.revokeWorkloadPreparation
				f.native.revokeWorkloadPreparation = func(ctx context.Context, req *runnerv1.RevokeWorkloadPreparationRequest, opts ...grpc.CallOption) (*runnerv1.RevokeWorkloadPreparationResponse, error) {
					if fault == "activation-claimed" {
						return nil, status.Error(codes.FailedPrecondition, "activation claimed")
					}
					response, err := revoke(ctx, req, opts...)
					switch fault {
					case "receipt-nil":
						return nil, nil
					case "receipt-uid":
						response.Revocation.InstanceUid = ""
					case "receipt-pod":
						response.Revocation.SelectedPodUid = "not-a-uid"
					case "receipt-workload":
						response.Revocation.WorkloadAnchor.InstanceUid = uuid.NewString()
					case "receipt-backend":
						response.Revocation.WorkloadAnchor.BackendId = "other"
					case "receipt-volumes":
						response.Revocation.VolumeAnchors = nil
					case "receipt-unknown":
						response.Revocation.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 1})
					}
					return response, err
				}
				observe := f.native.observePreparationRevocation
				f.native.observePreparationRevocation = func(ctx context.Context, req *runnerv1.ObservePreparationRevocationRequest, opts ...grpc.CallOption) (*runnerv1.ObservePreparationRevocationResponse, error) {
					response, err := observe(ctx, req, opts...)
					switch fault {
					case "observation-nil":
						return nil, nil
					case "observation-proof":
						response.Revocation.InstanceUid = uuid.NewString()
					case "observation-state":
						response.State = 99
					case "observation-partition":
						response.AbsentVolumeIds = []string{observation.Volumes[0].VolumeKey}
					case "observation-volume-uid":
						response.Volumes[0].InstanceUid = ""
					case "observation-owner":
						response.Volumes[0].IdentityLabels["sandbox-id"] = uuid.NewString()
					case "observation-unknown":
						response.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 1})
					case "known-replaced":
						response.Volumes[0].InstanceUid = uuid.NewString()
					case "record-owner":
						f.v.OwnerId = uuid.NewString()
					case "record-generation":
						f.v.LifecycleRevision++
					}
					return response, err
				}
				if err := f.r.stopPreparedWorkload(context.Background(), f.native, proto.Clone(f.w).(*runnersv1.Workload)); err == nil || f.w.RemovalConfirmedAt != nil || f.w.Preparation.Binding != nil || f.prepares != 1 || f.activations != 0 || f.removals != 0 {
					t.Fatal("unverified revocation released admission, invented a Pod binding, or replayed work")
				}
			})
		}
	}
}

func TestPreparationRevocationLostRepliesRecoverFromDurableState(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, checkpoint := range []string{"revoke", "record", "observe", "volume", "confirm"} {
			for _, committed := range []bool{false, true} {
				t.Run(fmt.Sprintf("sandbox=%t/%s/committed=%t", sandbox, checkpoint, committed), func(t *testing.T) {
					f, binding, _ := lostPreparedOutcome(t, sandbox, "first")
					installRevocationFixture(t, f, binding, false)
					injected := false
					fail := func(stage string) bool {
						if injected || stage != checkpoint {
							return false
						}
						injected = true
						return true
					}
					lost := status.Error(codes.Unavailable, "revocation RPC reply lost")
					revoke := f.native.revokeWorkloadPreparation
					f.native.revokeWorkloadPreparation = func(ctx context.Context, req *runnerv1.RevokeWorkloadPreparationRequest, opts ...grpc.CallOption) (*runnerv1.RevokeWorkloadPreparationResponse, error) {
						if fail("revoke") {
							if committed {
								if _, err := revoke(ctx, req, opts...); err != nil {
									t.Fatal(err)
								}
							}
							return nil, lost
						}
						return revoke(ctx, req, opts...)
					}
					observe := f.native.observePreparationRevocation
					f.native.observePreparationRevocation = func(ctx context.Context, req *runnerv1.ObservePreparationRevocationRequest, opts ...grpc.CallOption) (*runnerv1.ObservePreparationRevocationResponse, error) {
						if fail("observe") {
							if committed {
								if _, err := observe(ctx, req, opts...); err != nil {
									t.Fatal(err)
								}
							}
							return nil, lost
						}
						return observe(ctx, req, opts...)
					}
					update := f.registry.updateAnchoredWorkload
					f.registry.updateAnchoredWorkload = func(ctx context.Context, req *runnersv1.UpdateAnchoredWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.UpdateAnchoredWorkloadResponse, error) {
						if req.Operation.GetRecordRevocation() != nil && fail("record") || req.Operation.GetConfirmRevocation() != nil && fail("confirm") {
							if committed {
								if _, err := update(ctx, req, opts...); err != nil {
									t.Fatal(err)
								}
							}
							return nil, lost
						}
						return update(ctx, req, opts...)
					}
					volume := f.registry.updateVolumeChecked
					f.registry.updateVolumeChecked = func(ctx context.Context, req *runnersv1.UpdateVolumeCheckedRequest, opts ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
						if fail("volume") {
							if committed {
								if _, err := volume(ctx, req, opts...); err != nil {
									t.Fatal(err)
								}
							}
							return nil, lost
						}
						return volume(ctx, req, opts...)
					}
					if err := f.r.stopPreparedWorkload(context.Background(), f.native, proto.Clone(f.w).(*runnersv1.Workload)); err == nil || !injected {
						t.Fatal("cleanup ignored a lost reply")
					}
					if (f.w.RemovalConfirmedAt != nil) != (checkpoint == "confirm" && committed) {
						t.Fatal("cleanup without persisted confirmation released admission")
					}
					f.r = &Reconciler{runners: f.registry, agents: f.r.agents}
					if err := f.r.stopPreparedWorkload(context.Background(), f.native, proto.Clone(f.w).(*runnersv1.Workload)); err != nil {
						t.Fatal(err)
					}
					if f.w.RemovalConfirmedAt == nil || f.w.Preparation.Binding != nil || !proto.Equal(f.v.BoundInstance, binding.Volumes[0]) || f.prepares != 1 || f.activations != 0 || f.removals != 0 {
						t.Fatal("restart lost workspace identity or replayed execution")
					}
				})
			}
		}
	}
}

func TestPreparationRevocationPreservesActionableFailure(t *testing.T) {
	for _, code := range []codes.Code{codes.Unimplemented, codes.FailedPrecondition, codes.Aborted, codes.Unavailable} {
		t.Run(code.String(), func(t *testing.T) {
			f, _, _ := lostPreparedOutcome(t, false, "zero")
			f.native.observeWorkloadPreparation = func(context.Context, *runnerv1.ObserveWorkloadPreparationRequest, ...grpc.CallOption) (*runnerv1.ObserveWorkloadPreparationResponse, error) {
				return nil, status.Error(codes.NotFound, "initial observation")
			}
			f.native.revokeWorkloadPreparation = func(context.Context, *runnerv1.RevokeWorkloadPreparationRequest, ...grpc.CallOption) (*runnerv1.RevokeWorkloadPreparationResponse, error) {
				return nil, status.Error(code, "revocation failure")
			}
			f.native.removeWorkloadAnchor = func(context.Context, *runnerv1.RemoveWorkloadAnchorRequest, ...grpc.CallOption) (*runnerv1.RemoveWorkloadAnchorResponse, error) {
				t.Fatal("failed revocation erased native evidence")
				return nil, nil
			}
			if err := f.r.stopPreparedWorkload(context.Background(), f.native, f.w); status.Code(err) != code || f.w.RemovalConfirmedAt != nil {
				t.Fatalf("initial absence masked actionable revocation failure: %v", err)
			}
		})
	}
}

func TestPreparationRevocationRejectsChangedRegistryReceipts(t *testing.T) {
	for _, phase := range []string{"record", "confirm"} {
		for _, fault := range []string{"proof", "observation", "revision", "erasure"} {
			t.Run(phase+"/"+fault, func(t *testing.T) {
				f, binding, _ := lostPreparedOutcome(t, false, "zero")
				installRevocationFixture(t, f, binding, false)
				update := f.registry.updateAnchoredWorkload
				injected := false
				f.registry.updateAnchoredWorkload = func(ctx context.Context, req *runnersv1.UpdateAnchoredWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.UpdateAnchoredWorkloadResponse, error) {
					response, err := update(ctx, req, opts...)
					if err != nil || injected || phase == "record" && req.Operation.GetRecordRevocation() == nil || phase == "confirm" && req.Operation.GetConfirmRevocation() == nil {
						return response, err
					}
					injected = true
					a := response.Workload.Preparation.Resources
					switch fault {
					case "proof":
						a.PreparationRevocation.InstanceUid = uuid.NewString()
					case "observation":
						a.RevocationObservation = &runnerv1.ObservePreparationRevocationResponse{Revocation: a.PreparationRevocation, State: runnerv1.RevokedPreparationState_REVOKED_PREPARATION_STATE_PENDING}
					case "revision":
						a.Revision--
					case "erasure":
						a.PreparationRevocation, a.RevocationObservation = nil, nil
					}
					return response, nil
				}
				if err := f.r.stopPreparedWorkload(context.Background(), f.native, proto.Clone(f.w).(*runnersv1.Workload)); err == nil || !injected {
					t.Fatal("altered registry acknowledgment was accepted")
				}
				f.r = &Reconciler{runners: f.registry, agents: f.r.agents}
				if err := f.r.stopPreparedWorkload(context.Background(), f.native, proto.Clone(f.w).(*runnersv1.Workload)); err != nil || f.w.RemovalConfirmedAt == nil {
					t.Fatalf("restart did not recover from independently stored unaltered evidence: %v", err)
				}
			})
		}
	}
}
