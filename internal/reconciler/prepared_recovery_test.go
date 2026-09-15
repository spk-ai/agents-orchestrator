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

func lostPreparedOutcome(t *testing.T, sandbox bool, volumes string) (*preparedControllerFixture, *runnerv1.WorkloadBinding, func(context.Context, *runnerv1.PrepareWorkloadRequest, ...grpc.CallOption) (*runnerv1.PrepareWorkloadResponse, error)) {
	t.Helper()
	f := newPreparedControllerFixture(t, sandbox)
	if volumes == "zero" {
		f.infos, f.created, f.request.Volumes = nil, nil, nil
	} else if volumes == "existing" {
		f.v.Status, f.v.LifecycleRevision = runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, 2
		f.v.BoundInstance = checkedTestInstance(f.v, f.infos[0].Spec.PersistentName, uuid.NewString())
		if sandbox {
			f.v.BoundInstance.IdentityLabels["sandbox-owner-id"] = f.humanOwner
		}
		f.v.InstanceId, f.created = stringPtr(f.v.BoundInstance.InstanceId), nil
	}
	prepare := f.native.prepareWorkload
	f.native.prepareWorkload = func(ctx context.Context, req *runnerv1.PrepareWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.PrepareWorkloadResponse, error) {
		if _, err := prepare(ctx, req, opts...); err != nil {
			return nil, err
		}
		return nil, status.Error(codes.Unavailable, "lost prepare response")
	}
	if _, err := f.r.startPreparedWorkload(context.Background(), f.native, f.metadata, f.request, f.infos, f.created); err == nil {
		t.Fatal("lost response reported success")
	}
	if f.w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING || f.w.Preparation.Binding != nil || f.binding == nil || f.active {
		t.Fatal("lost outcome did not retain its unbound removal reservation")
	}
	f.r = &Reconciler{runners: f.registry, agents: f.r.agents}
	return f, proto.Clone(f.binding).(*runnerv1.WorkloadBinding), prepare
}

func recoveryObservation(f *preparedControllerFixture, b *runnerv1.WorkloadBinding) *runnerv1.ObserveWorkloadPreparationResponse {
	labels := map[string]string{"app.kubernetes.io/managed-by": "k8s-runner", "agyn.dev/managed-by": "agents-orchestrator", "managed-by": "agents-orchestrator"}
	if f.w.OwnerKind == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX {
		labels["sandbox-id"], labels["sandbox-owner-id"] = f.w.OwnerId, f.humanOwner
	} else {
		labels["agent-instance-id"], labels["agent-id"], labels["thread-id"] = f.w.OwnerId, f.w.AgentId, f.w.ThreadId
	}
	return &runnerv1.ObserveWorkloadPreparationResponse{Binding: proto.Clone(b).(*runnerv1.WorkloadBinding), IdentityLabels: labels, ResourceVersion: "17", SetupComplete: true}
}

func TestPreparedRecoveryRetiresLostReplyWithoutRedispatch(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, volumes := range []string{"first", "existing", "zero"} {
			t.Run(fmt.Sprintf("sandbox=%t/%s", sandbox, volumes), func(t *testing.T) {
				f, binding, prepare := lostPreparedOutcome(t, sandbox, volumes)
				observations := 0
				f.native.observeWorkloadPreparation = func(_ context.Context, req *runnerv1.ObserveWorkloadPreparationRequest, _ ...grpc.CallOption) (*runnerv1.ObserveWorkloadPreparationResponse, error) {
					observations++
					if f.w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING || req.WorkloadId != f.w.Meta.Id || req.BackendId != f.w.Preparation.BackendId {
						t.Fatal("discovery before durable retirement or on wrong backend")
					}
					return recoveryObservation(f, binding), nil
				}
				previous := proto.Clone(f.w).(*runnersv1.Workload)
				if err := f.r.stopPreparedWorkload(context.Background(), f.native, previous); err != nil {
					t.Fatalf("lost preparation could not be retired: %v", err)
				}
				if f.w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED || f.w.RemovalConfirmedAt == nil || !samePreparedBinding(f.w.Preparation.Binding, binding) || observations != 1 || f.prepares != 1 || f.activations != 0 || f.removals != 1 {
					t.Fatal("recovery replayed startup, lost identity or skipped exact removal")
				}
				if err := f.r.stopPreparedWorkload(context.Background(), f.native, previous); err != nil || observations != 1 || f.removals != 1 {
					t.Fatalf("settled recovery was not idempotent: %v", err)
				}
				f.native.prepareWorkload = prepare
				f.metadata.Id = uuid.NewString()
				f.request.WorkloadId, f.created = f.metadata.Id, nil
				resumed, err := f.r.startPreparedWorkload(context.Background(), f.native, f.metadata, f.request, f.infos, f.created)
				if err != nil || resumed.Preparation.Binding.InstanceUid == binding.InstanceUid {
					t.Fatalf("explicit new workload could not resume: %v", err)
				}
				if volumes != "zero" && !proto.Equal(resumed.Preparation.Binding.Volumes[0], binding.Volumes[0]) {
					t.Fatal("recovery changed the durable workspace")
				}
			})
		}
	}
}

func TestPreparedRecoveryRejectsUnverifiedObservations(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, which := range []string{"not-found", "unimplemented", "nil", "revision", "workload", "backend", "pod-uid", "owner", "manager", "extra-label", "mixed-owner", "volumes", "volume-key", "volume-uid", "volume-generation", "volume-owner", "sandbox-user"} {
			t.Run(fmt.Sprintf("sandbox=%t/%s", sandbox, which), func(t *testing.T) {
				f, binding, _ := lostPreparedOutcome(t, sandbox, "existing")
				f.native.observeWorkloadPreparation = func(context.Context, *runnerv1.ObserveWorkloadPreparationRequest, ...grpc.CallOption) (*runnerv1.ObserveWorkloadPreparationResponse, error) {
					response := recoveryObservation(f, binding)
					switch which {
					case "not-found":
						return nil, status.Error(codes.NotFound, "absent is not final")
					case "unimplemented":
						return nil, status.Error(codes.Unimplemented, "old runner")
					case "nil":
						return nil, nil
					case "revision":
						response.ResourceVersion = ""
					case "workload":
						response.Binding.WorkloadId = uuid.NewString()
					case "backend":
						response.Binding.BackendId = "other"
					case "pod-uid":
						response.Binding.InstanceUid = ""
					case "owner":
						response.IdentityLabels["sandbox-id"] = uuid.NewString()
						response.IdentityLabels["agent-instance-id"] = uuid.NewString()
					case "manager":
						response.IdentityLabels["managed-by"] = "other"
					case "extra-label":
						response.IdentityLabels["untracked"] = "value"
					case "mixed-owner":
						response.IdentityLabels["agent-id"], response.IdentityLabels["sandbox-id"] = uuid.NewString(), uuid.NewString()
					case "volumes":
						response.Binding.Volumes = nil
					case "volume-key":
						response.Binding.Volumes[0].VolumeKey = uuid.NewString()
					case "volume-uid":
						response.Binding.Volumes[0].InstanceUid = uuid.NewString()
					case "volume-generation":
						f.v.Status, f.v.BoundInstance, f.v.InstanceId = runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING, nil, nil
					case "volume-owner":
						f.v.OwnerId = uuid.NewString()
					case "sandbox-user":
						response.IdentityLabels["sandbox-owner-id"] = "other-human"
					}
					return response, nil
				}
				if err := f.r.stopPreparedWorkload(context.Background(), f.native, proto.Clone(f.w).(*runnersv1.Workload)); err == nil {
					t.Fatal("unverified observation released admission")
				}
				if f.w.Preparation.Binding != nil || f.w.RemovalConfirmedAt != nil || f.prepares != 1 || f.activations != 0 || f.removals != 0 {
					t.Fatal("unverified observation caused a lifecycle mutation")
				}
			})
		}
	}
}

func TestPreparedRecoveryAcceptsCompetingRetirement(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, checkpoint := range []string{"observation", "volume-read", "binding-write"} {
			t.Run(fmt.Sprintf("sandbox=%t/%s", sandbox, checkpoint), func(t *testing.T) {
				f, binding, _ := lostPreparedOutcome(t, sandbox, "first")
				previous := proto.Clone(f.w).(*runnersv1.Workload)
				competing := &Reconciler{runners: f.registry, agents: f.r.agents}
				handedOff := false
				// Deterministic RPC interleavings, not concurrent access to this fake.
				handoff := func() {
					if handedOff {
						return
					}
					handedOff = true
					if err := competing.stopPreparedWorkload(context.Background(), f.native, proto.Clone(f.w).(*runnersv1.Workload)); err != nil {
						t.Fatalf("competing recovery failed: %v", err)
					}
				}
				f.native.observeWorkloadPreparation = func(context.Context, *runnerv1.ObserveWorkloadPreparationRequest, ...grpc.CallOption) (*runnerv1.ObserveWorkloadPreparationResponse, error) {
					observed := recoveryObservation(f, binding)
					if checkpoint == "observation" {
						handoff()
					}
					return observed, nil
				}
				getVolume, reads := f.registry.getVolume, 0
				f.registry.getVolume = func(ctx context.Context, req *runnersv1.GetVolumeRequest, opts ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error) {
					reads++
					if checkpoint == "volume-read" && reads == 2 {
						handoff()
					}
					return getVolume(ctx, req, opts...)
				}
				update := f.registry.updatePreparedWorkload
				f.registry.updatePreparedWorkload = func(ctx context.Context, req *runnersv1.UpdatePreparedWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.UpdatePreparedWorkloadResponse, error) {
					if checkpoint == "binding-write" && req.GetBind() != nil {
						handoff()
					}
					return update(ctx, req, opts...)
				}
				if err := f.r.stopPreparedWorkload(context.Background(), f.native, previous); err != nil {
					t.Fatalf("stale recovery did not accept exact competing retirement: %v", err)
				}
				if !handedOff || f.prepares != 1 || f.activations != 0 || f.removals != 1 || f.w.RemovalConfirmedAt == nil || !samePreparedBinding(f.w.Preparation.Binding, binding) {
					t.Fatal("competing retirement repeated execution/removal or lost the exact binding")
				}
			})
		}
	}
}

func TestPreparedRecoveryRetainsDurableStateAcrossRPCFailures(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, checkpoint := range []string{"volume", "binding", "remove", "confirm"} {
			for _, committed := range []bool{false, true} {
				t.Run(fmt.Sprintf("sandbox=%t/%s/committed=%t", sandbox, checkpoint, committed), func(t *testing.T) {
					f, binding, _ := lostPreparedOutcome(t, sandbox, "first")
					f.native.observeWorkloadPreparation = func(context.Context, *runnerv1.ObserveWorkloadPreparationRequest, ...grpc.CallOption) (*runnerv1.ObserveWorkloadPreparationResponse, error) {
						return recoveryObservation(f, binding), nil
					}
					injected := false
					fail := func(stage string) bool {
						if injected || stage != checkpoint {
							return false
						}
						injected = true
						return true
					}
					lost := status.Error(codes.Unavailable, "recovery RPC failed")
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
					update := f.registry.updatePreparedWorkload
					f.registry.updatePreparedWorkload = func(ctx context.Context, req *runnersv1.UpdatePreparedWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.UpdatePreparedWorkloadResponse, error) {
						if req.GetBind() != nil && fail("binding") || req.GetConfirmRemoval() != nil && fail("confirm") {
							if committed {
								if _, err := update(ctx, req, opts...); err != nil {
									t.Fatal(err)
								}
							}
							return nil, lost
						}
						return update(ctx, req, opts...)
					}
					remove := f.native.removePreparedWorkload
					f.native.removePreparedWorkload = func(ctx context.Context, req *runnerv1.RemovePreparedWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.RemovePreparedWorkloadResponse, error) {
						if fail("remove") {
							if committed {
								if _, err := remove(ctx, req, opts...); err != nil {
									t.Fatal(err)
								}
							}
							return nil, lost
						}
						return remove(ctx, req, opts...)
					}
					if err := f.r.stopPreparedWorkload(context.Background(), f.native, proto.Clone(f.w).(*runnersv1.Workload)); err == nil || !injected {
						t.Fatal("recovery ignored the injected failure")
					}
					if (f.w.RemovalConfirmedAt != nil) != (checkpoint == "confirm" && committed) || f.prepares != 1 || f.activations != 0 {
						t.Fatal("ambiguous recovery released admission without confirmed removal or replayed startup")
					}
					f.r = &Reconciler{runners: f.registry, agents: f.r.agents}
					if err := f.r.stopPreparedWorkload(context.Background(), f.native, proto.Clone(f.w).(*runnersv1.Workload)); err != nil {
						t.Fatalf("fresh recovery could not finish the exact removal: %v", err)
					}
					if f.w.RemovalConfirmedAt == nil || f.binding != nil || f.v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE || !samePreparedBinding(f.w.Preparation.Binding, binding) || !proto.Equal(f.v.BoundInstance, binding.Volumes[0]) || f.prepares != 1 || f.activations != 0 {
						t.Fatal("recovery lost its durable workspace, binding or no-replay invariant")
					}
				})
			}
		}
	}
}
