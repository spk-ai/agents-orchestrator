package reconciler

import (
	"context"
	"errors"
	"math"
	"testing"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func checkedTestCreateRequest(v *runnersv1.Volume) *runnersv1.CreateVolumeRequest {
	return &runnersv1.CreateVolumeRequest{
		Id: v.Meta.Id, RunnerId: v.RunnerId, OrganizationId: v.OrganizationId,
		OwnerKind: v.OwnerKind, OwnerId: v.OwnerId, ThreadId: v.ThreadId,
		AgentId: v.AgentId, VolumeId: v.VolumeId, SizeGb: v.SizeGb,
		Status: runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING,
	}
}

func TestCheckedVolumeCreateValidatesAcknowledgement(t *testing.T) {
	for name, change := range map[string]func(*runnersv1.CreateVolumeCheckedResponse){
		"empty":         func(r *runnersv1.CreateVolumeCheckedResponse) { r.Volume = nil },
		"legacy":        func(r *runnersv1.CreateVolumeCheckedResponse) { r.Volume.CheckedLifecycle = false },
		"zero-revision": func(r *runnersv1.CreateVolumeCheckedResponse) { r.Volume.LifecycleRevision = 0 },
		"later-revision": func(r *runnersv1.CreateVolumeCheckedResponse) {
			r.Volume.LifecycleRevision = 2
		},
		"other-owner": func(r *runnersv1.CreateVolumeCheckedResponse) { r.Volume.OwnerId = "other-owner" },
		"other-org":   func(r *runnersv1.CreateVolumeCheckedResponse) { r.Volume.OrganizationId = "other-org" },
		"other-runner": func(r *runnersv1.CreateVolumeCheckedResponse) {
			r.Volume.RunnerId = "other-runner"
		},
		"other-thread": func(r *runnersv1.CreateVolumeCheckedResponse) { r.Volume.ThreadId = "other-thread" },
		"other-class":  func(r *runnersv1.CreateVolumeCheckedResponse) { r.Volume.AgentId = "other-class" },
		"other-key":    func(r *runnersv1.CreateVolumeCheckedResponse) { r.Volume.Meta.Id = "other-key" },
		"other-definition": func(r *runnersv1.CreateVolumeCheckedResponse) {
			r.Volume.VolumeId = "other-definition"
		},
		"other-size":   func(r *runnersv1.CreateVolumeCheckedResponse) { r.Volume.SizeGb = "2" },
		"invalid-size": func(r *runnersv1.CreateVolumeCheckedResponse) { r.Volume.SizeGb = "NaN" },
		"closed": func(r *runnersv1.CreateVolumeCheckedResponse) {
			r.Volume.Status = runnersv1.VolumeStatus_VOLUME_STATUS_FAILED
		},
		"billing-closed": func(r *runnersv1.CreateVolumeCheckedResponse) {
			r.Volume.RemovedAt = timestamppb.Now()
		},
		"already-bound": func(r *runnersv1.CreateVolumeCheckedResponse) {
			r.Volume.BoundInstance = checkedTestInstance(r.Volume, "pvc", "uid")
			r.Volume.InstanceId = stringPtr("pvc")
		},
	} {
		t.Run(name, func(t *testing.T) {
			v := checkedTestVolume("volume", runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING)
			r := &Reconciler{runners: &fakeRunnersClient{
				createVolumeChecked: func(_ context.Context, req *runnersv1.CreateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.CreateVolumeCheckedResponse, error) {
					resp := checkedTestCreate(req.Volume)
					change(resp)
					return resp, nil
				},
				getVolume: func(context.Context, *runnersv1.GetVolumeRequest, ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error) {
					t.Fatal("malformed acknowledgement must not trigger a fresh adoption")
					return nil, errNotImplemented
				},
			}}
			if next, owned, err := r.createOrReuseCheckedVolume(context.Background(), checkedTestCreateRequest(v)); err == nil || next != nil || owned {
				t.Fatalf("accepted malformed create: next=%v owned=%t err=%v", next, owned, err)
			}
		})
	}
}

func TestCheckedVolumeReuseAndExplicitReopen(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, state := range []runnersv1.VolumeStatus{
			runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING, runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE,
			runnersv1.VolumeStatus_VOLUME_STATUS_FAILED, runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING,
			runnersv1.VolumeStatus_VOLUME_STATUS_DELETED,
		} {
			name := "agent/" + state.String()
			if sandbox {
				name = "sandbox/" + state.String()
			}
			t.Run(name, func(t *testing.T) {
				v := checkedTestVolume("volume", state)
				if sandbox {
					v = checkedTestSandboxVolume("volume", "sandbox", state)
				}
				if state == runnersv1.VolumeStatus_VOLUME_STATUS_FAILED {
					v.BoundInstance, v.InstanceId = checkedTestInstance(v, "failed-pvc", "failed-uid"), stringPtr("failed-pvc")
					v.RemovedAt = timestamppb.Now()
				}
				before := proto.Clone(v).(*runnersv1.Volume)
				updates := 0
				r := &Reconciler{runners: &fakeRunnersClient{
					createVolumeChecked: func(context.Context, *runnersv1.CreateVolumeCheckedRequest, ...grpc.CallOption) (*runnersv1.CreateVolumeCheckedResponse, error) {
						return nil, status.Error(codes.AlreadyExists, "existing")
					},
					getVolume: func(context.Context, *runnersv1.GetVolumeRequest, ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error) {
						return &runnersv1.GetVolumeResponse{Volume: v}, nil
					},
					updateVolumeChecked: func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
						updates++
						if req.GetReopen() == nil {
							t.Fatal("closed state requires an explicit reopen operation")
						}
						return checkedTestUpdate(t, v, req), nil
					},
				}}
				next, owned, err := r.createOrReuseCheckedVolume(context.Background(), checkedTestCreateRequest(v))
				if state == runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING {
					if err == nil || owned || next != nil || updates != 0 {
						t.Fatal("pending removal must not be reopened")
					}
					return
				}
				if err != nil || !sameVolumeIdentity(before, next) {
					t.Fatalf("reuse/reopen: %v", err)
				}
				reopened := state == runnersv1.VolumeStatus_VOLUME_STATUS_FAILED || state == runnersv1.VolumeStatus_VOLUME_STATUS_DELETED
				expectedUpdates := 0
				if reopened {
					expectedUpdates = 1
				}
				if owned != reopened || updates != expectedUpdates {
					t.Fatalf("incorrect compensation ownership=%t updates=%d", owned, updates)
				}
				if reopened {
					if next.LifecycleRevision != before.LifecycleRevision+1 || next.Status != runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING || next.RemovedAt != nil {
						t.Fatal("reopen did not preserve the lifecycle contract")
					}
					if state == runnersv1.VolumeStatus_VOLUME_STATUS_DELETED && next.BoundInstance != nil ||
						state == runnersv1.VolumeStatus_VOLUME_STATUS_FAILED && !proto.Equal(before.BoundInstance, next.BoundInstance) {
						t.Fatal("reopen retargeted the wrong physical generation")
					}
				} else if !proto.Equal(before, next) {
					t.Fatal("reuse mutated an existing open generation")
				}
				next.OwnerId = "caller-mutation"
				if v.OwnerId != before.OwnerId {
					t.Fatal("returned snapshot aliases registry state")
				}
			})
		}
	}
}

func TestCheckedVolumeRefusesLegacyAndMalformedBindings(t *testing.T) {
	for name, change := range map[string]func(*runnersv1.Volume){
		"legacy":            func(v *runnersv1.Volume) { v.CheckedLifecycle = false },
		"zero-revision":     func(v *runnersv1.Volume) { v.LifecycleRevision = 0 },
		"overflow":          func(v *runnersv1.Volume) { v.LifecycleRevision = math.MaxUint64 },
		"unbound-active":    func(v *runnersv1.Volume) { v.BoundInstance, v.InstanceId = nil, nil },
		"name-only":         func(v *runnersv1.Volume) { v.BoundInstance = nil },
		"missing-uid":       func(v *runnersv1.Volume) { v.BoundInstance.InstanceUid = "" },
		"wrong-key":         func(v *runnersv1.Volume) { v.BoundInstance.VolumeKey = "other" },
		"wrong-name":        func(v *runnersv1.Volume) { v.InstanceId = stringPtr("other") },
		"wrong-owner":       func(v *runnersv1.Volume) { v.BoundInstance.IdentityLabels["agent-instance-id"] = "other" },
		"wrong-class":       func(v *runnersv1.Volume) { v.BoundInstance.IdentityLabels["agent-id"] = "other" },
		"wrong-manager":     func(v *runnersv1.Volume) { v.BoundInstance.IdentityLabels["managed-by"] = "other" },
		"owner-kind-mix":    func(v *runnersv1.Volume) { v.BoundInstance.IdentityLabels["sandbox-id"] = "other" },
		"unknown-label":     func(v *runnersv1.Volume) { v.BoundInstance.IdentityLabels["not-identity"] = "other" },
		"conflicting-alias": func(v *runnersv1.Volume) { v.AgentInstanceId = stringPtr("other") },
		"unconfirmed-deleted": func(v *runnersv1.Volume) {
			v.Status = runnersv1.VolumeStatus_VOLUME_STATUS_DELETED
			v.RemovedAt = timestamppb.Now()
		},
	} {
		t.Run(name, func(t *testing.T) {
			v := checkedTestVolume("volume", runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
			change(v)
			if err := validateCheckedVolume(v); err == nil {
				t.Fatal("accepted invalid checked record")
			}
			r := &Reconciler{runners: &fakeRunnersClient{updateVolumeChecked: func(context.Context, *runnersv1.UpdateVolumeCheckedRequest, ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
				t.Fatal("invalid record reached the registry writer")
				return nil, errNotImplemented
			}}}
			if done, err := r.advanceVolumeRemoval(context.Background(), &fakeRunnerClient{}, v); done || err == nil {
				t.Fatal("invalid record authorized removal")
			}
		})
	}
}

func TestCheckedVolumeBeginValidatesReplyBeforeNativeDelete(t *testing.T) {
	for name, change := range map[string]func(*runnersv1.UpdateVolumeCheckedResponse){
		"nil-volume":       func(r *runnersv1.UpdateVolumeCheckedResponse) { r.Volume = nil },
		"legacy":           func(r *runnersv1.UpdateVolumeCheckedResponse) { r.Volume.CheckedLifecycle = false },
		"stale":            func(r *runnersv1.UpdateVolumeCheckedResponse) { r.Volume.LifecycleRevision-- },
		"skipped-revision": func(r *runnersv1.UpdateVolumeCheckedResponse) { r.Volume.LifecycleRevision++ },
		"wrong-owner": func(r *runnersv1.UpdateVolumeCheckedResponse) {
			r.Volume.OwnerId = "other"
			r.Volume.BoundInstance.IdentityLabels["agent-instance-id"] = "other"
			r.Volume.RemovalIntent.Expected.IdentityLabels["agent-instance-id"] = "other"
		},
		"wrong-size": func(r *runnersv1.UpdateVolumeCheckedResponse) { r.Volume.SizeGb = "2" },
		"retargeted-uid": func(r *runnersv1.UpdateVolumeCheckedResponse) {
			r.Volume.BoundInstance.InstanceUid, r.Volume.RemovalIntent.Expected.InstanceUid = "replacement", "replacement"
		},
		"different-intent":  func(r *runnersv1.UpdateVolumeCheckedResponse) { r.Volume.RemovalIntent.Id = "replacement-intent" },
		"no-intent":         func(r *runnersv1.UpdateVolumeCheckedResponse) { r.Volume.RemovalIntent = nil },
		"already-confirmed": func(r *runnersv1.UpdateVolumeCheckedResponse) { r.Volume.RemovalIntent.ConfirmedAt = timestamppb.Now() },
	} {
		t.Run(name, func(t *testing.T) {
			v := checkedTestVolume("volume", runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING)
			r := &Reconciler{runners: &fakeRunnersClient{updateVolumeChecked: func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
				resp := checkedTestUpdate(t, proto.Clone(v).(*runnersv1.Volume), req)
				change(resp)
				return resp, nil
			}}}
			native := &fakeRunnerClient{removeVolumeChecked: func(context.Context, *runnerv1.RemoveVolumeCheckedRequest, ...grpc.CallOption) (*runnerv1.RemoveVolumeCheckedResponse, error) {
				t.Fatal("malformed begin reply reached the native delete")
				return nil, errNotImplemented
			}}
			if done, err := r.advanceVolumeRemoval(context.Background(), native, v); done || err == nil {
				t.Fatal("accepted malformed begin acknowledgement")
			}
		})
	}
}

func TestCheckedVolumeRemovalResumesOriginalIntent(t *testing.T) {
	for _, lostAt := range []string{"begin", "native", "confirm", "pending"} {
		t.Run(lostAt, func(t *testing.T) {
			v := checkedTestVolume("volume", runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
			original := proto.Clone(v.BoundInstance).(*runnerv1.VolumeListItem)
			lost, nativeCalls, confirms := false, 0, 0
			registry := &fakeRunnersClient{updateVolumeChecked: func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
				resp := checkedTestUpdate(t, v, req)
				if req.GetConfirmRemoval() != nil {
					confirms++
				}
				if !lost && (lostAt == "begin" && req.GetBeginRemoval() != nil || lostAt == "confirm" && req.GetConfirmRemoval() != nil) {
					lost = true
					return nil, status.Error(codes.Unavailable, "committed reply lost")
				}
				return resp, nil
			}}
			native := &fakeRunnerClient{removeVolumeChecked: func(_ context.Context, req *runnerv1.RemoveVolumeCheckedRequest, _ ...grpc.CallOption) (*runnerv1.RemoveVolumeCheckedResponse, error) {
				nativeCalls++
				if !proto.Equal(req.Expected, original) {
					t.Fatal("restart retargeted the durable intent")
				}
				if !lost && lostAt == "native" {
					lost = true
					return nil, status.Error(codes.Unavailable, "native reply lost")
				}
				if !lost && lostAt == "pending" {
					lost = true
					return &runnerv1.RemoveVolumeCheckedResponse{State: runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING}, nil
				}
				return &runnerv1.RemoveVolumeCheckedResponse{State: runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT}, nil
			}}
			first := &Reconciler{runners: registry}
			done, err := first.advanceVolumeRemoval(context.Background(), native, proto.Clone(v).(*runnersv1.Volume))
			if done || lostAt == "pending" && err != nil || lostAt != "pending" && err == nil {
				t.Fatalf("ambiguous first attempt settled: done=%t err=%v", done, err)
			}
			intent := proto.Clone(v.RemovalIntent).(*runnersv1.VolumeRemovalIntent)
			if lostAt != "confirm" && (confirms != 0 || intent.ConfirmedAt != nil) {
				t.Fatal("pending/unknown native result was confirmed")
			}
			// New controller, only persisted registry state; no volatile intent or
			// fresh inventory/name lookup can substitute a replacement target.
			restarted := &Reconciler{runners: registry}
			done, err = restarted.advanceVolumeRemoval(context.Background(), native, proto.Clone(v).(*runnersv1.Volume))
			if !done || err != nil || v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_DELETED || v.RemovalIntent.ConfirmedAt == nil || v.RemovalIntent.Id != intent.Id || confirms != 1 {
				t.Fatalf("restart failed to confirm original intent: done=%t err=%v confirms=%d", done, err, confirms)
			}
			expectedCalls := 2
			if lostAt == "begin" || lostAt == "confirm" {
				expectedCalls = 1
			}
			if nativeCalls != expectedCalls {
				t.Fatalf("native calls=%d, want %d", nativeCalls, expectedCalls)
			}
		})
	}
}

func TestCheckedVolumeNativeFailureNeverConfirmsOrFallsBack(t *testing.T) {
	for _, scenario := range []string{"nil", "unknown", "pending", "unimplemented", "unavailable", "replacement-conflict"} {
		t.Run(scenario, func(t *testing.T) {
			v := checkedTestVolume("volume", runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
			updates := 0
			r := &Reconciler{runners: &fakeRunnersClient{updateVolumeChecked: func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
				updates++
				if req.GetBeginRemoval() == nil {
					t.Fatal("unconfirmed native response authorized registry finalization")
				}
				return checkedTestUpdate(t, v, req), nil
			}}}
			native := &fakeRunnerClient{
				removeVolume: func(context.Context, *runnerv1.RemoveVolumeRequest, ...grpc.CallOption) (*runnerv1.RemoveVolumeResponse, error) {
					t.Fatal("unsafe legacy removal fallback")
					return nil, errNotImplemented
				},
				removeVolumeChecked: func(context.Context, *runnerv1.RemoveVolumeCheckedRequest, ...grpc.CallOption) (*runnerv1.RemoveVolumeCheckedResponse, error) {
					switch scenario {
					case "nil":
						return nil, nil
					case "unknown":
						return &runnerv1.RemoveVolumeCheckedResponse{State: runnerv1.VolumeRemovalState(99)}, nil
					case "pending":
						return &runnerv1.RemoveVolumeCheckedResponse{State: runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING}, nil
					case "unimplemented":
						return nil, status.Error(codes.Unimplemented, "old runner")
					case "replacement-conflict":
						return nil, status.Error(codes.FailedPrecondition, "UID changed")
					default:
						return nil, status.Error(codes.Unavailable, "runner unreachable")
					}
				},
			}
			done, err := r.advanceVolumeRemoval(context.Background(), native, v)
			if done || scenario == "pending" && err != nil || scenario != "pending" && err == nil || updates != 1 || v.RemovalIntent.ConfirmedAt != nil {
				t.Fatalf("invalid removal result: done=%t err=%v updates=%d", done, err, updates)
			}
		})
	}
}

func TestCheckedVolumeCompensationUsesOwnedSnapshot(t *testing.T) {
	for _, concurrent := range []bool{false, true} {
		t.Run(map[bool]string{false: "owned", true: "stale"}[concurrent], func(t *testing.T) {
			v := checkedTestVolume("volume", runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING)
			snapshot := proto.Clone(v).(*runnersv1.Volume)
			if concurrent {
				v.LifecycleRevision++
			}
			calls := 0
			r := &Reconciler{runners: &fakeRunnersClient{
				getVolume: func(context.Context, *runnersv1.GetVolumeRequest, ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error) {
					t.Fatal("compensation must not acquire a newer generation")
					return nil, errNotImplemented
				},
				updateVolumeChecked: func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
					calls++
					if req.ExpectedRevision != snapshot.LifecycleRevision || req.GetFailProvisioning() == nil {
						t.Fatal("compensation did not use its owned revision")
					}
					if concurrent {
						return nil, status.Error(codes.Aborted, "stale revision")
					}
					return checkedTestUpdate(t, v, req), nil
				},
			}}
			r.markVolumeRecordsFailed(context.Background(), []volumeRecord{{id: "unowned"}, {id: v.Meta.Id, checked: snapshot}})
			if calls != 1 || concurrent && v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING || !concurrent && v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_FAILED {
				t.Fatalf("unsafe compensation: calls=%d state=%s", calls, v.Status)
			}
		})
	}
}

func TestCheckedSandboxBindingRequiresUserOwnership(t *testing.T) {
	for _, mismatch := range []string{"none", "id", "org", "user", "nil", "unavailable"} {
		t.Run(mismatch, func(t *testing.T) {
			v := checkedTestSandboxVolume("volume", "sandbox", runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING)
			item := checkedTestInstance(v, "sandbox-pvc", "sandbox-uid")
			calls := 0
			r := &Reconciler{
				runners: &fakeRunnersClient{updateVolumeChecked: func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
					calls++
					return checkedTestUpdate(t, v, req), nil
				}},
				agents: &testutil.FakeAgentsClient{GetSandboxFunc: func(context.Context, *agentsv1.GetSandboxRequest, ...grpc.CallOption) (*agentsv1.GetSandboxResponse, error) {
					sandbox := &agentsv1.Sandbox{Meta: &agentsv1.EntityMeta{Id: v.OwnerId}, OrganizationId: v.OrganizationId, OwnerId: "fixture-sandbox-user"}
					switch mismatch {
					case "id":
						sandbox.Meta.Id = "other"
					case "org":
						sandbox.OrganizationId = "other"
					case "user":
						sandbox.OwnerId = "other"
					case "nil":
						return nil, nil
					case "unavailable":
						return nil, errors.New("unavailable")
					}
					return &agentsv1.GetSandboxResponse{Sandbox: sandbox}, nil
				}},
			}
			_, err := r.bindCheckedVolume(context.Background(), v, item)
			if mismatch == "none" {
				if err != nil || calls != 1 {
					t.Fatalf("matching owner not bound: %v", err)
				}
			} else if err == nil || calls != 0 {
				t.Fatal("unverified sandbox user reached binding writer")
			}
		})
	}
}

func TestCheckedVolumeRegistryErrorsDoNotFallBackOrRetry(t *testing.T) {
	for _, code := range []codes.Code{codes.Unimplemented, codes.Unavailable, codes.Aborted, codes.FailedPrecondition} {
		t.Run(code.String(), func(t *testing.T) {
			v := checkedTestVolume("volume", runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
			calls := 0
			failure := status.Error(code, "registry refused")
			r := &Reconciler{runners: &fakeRunnersClient{
				createVolumeChecked: func(context.Context, *runnersv1.CreateVolumeCheckedRequest, ...grpc.CallOption) (*runnersv1.CreateVolumeCheckedResponse, error) {
					calls++
					return nil, failure
				},
				updateVolumeChecked: func(context.Context, *runnersv1.UpdateVolumeCheckedRequest, ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
					calls++
					return nil, failure
				},
				getVolume: func(context.Context, *runnersv1.GetVolumeRequest, ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error) {
					t.Fatal("failed mutation must not retarget a newer revision")
					return nil, errNotImplemented
				},
				createVolume: func(context.Context, *runnersv1.CreateVolumeRequest, ...grpc.CallOption) (*runnersv1.CreateVolumeResponse, error) {
					t.Fatal("legacy create fallback")
					return nil, errNotImplemented
				},
				updateVolume: func(context.Context, *runnersv1.UpdateVolumeRequest, ...grpc.CallOption) (*runnersv1.UpdateVolumeResponse, error) {
					t.Fatal("legacy update fallback")
					return nil, errNotImplemented
				},
			}}
			if next, owned, err := r.createOrReuseCheckedVolume(context.Background(), checkedTestCreateRequest(v)); status.Code(err) != code || owned || next != nil || calls != 1 {
				t.Fatal("registry create failure triggered a retry or fallback")
			}
			native := &fakeRunnerClient{removeVolumeChecked: func(context.Context, *runnerv1.RemoveVolumeCheckedRequest, ...grpc.CallOption) (*runnerv1.RemoveVolumeCheckedResponse, error) {
				t.Fatal("failed registry begin authorized native removal")
				return nil, errNotImplemented
			}}
			if done, err := r.advanceVolumeRemoval(context.Background(), native, v); done || status.Code(err) != code || calls != 2 {
				t.Fatal("registry begin failure triggered a retry or fallback")
			}
		})
	}
}

func TestCheckedVolumeConfirmationRequiresMatchingAcknowledgement(t *testing.T) {
	for _, scenario := range []string{"nil", "stale", "new-intent", "new-uid", "no-confirmation", "different-time", "aborted"} {
		t.Run(scenario, func(t *testing.T) {
			v := checkedTestVolume("volume", runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING)
			r := &Reconciler{runners: &fakeRunnersClient{updateVolumeChecked: func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
				resp := checkedTestUpdate(t, v, req)
				if req.GetConfirmRemoval() == nil {
					return resp, nil
				}
				switch scenario {
				case "nil":
					return nil, nil
				case "stale":
					resp.Volume.LifecycleRevision--
				case "new-intent":
					resp.Volume.RemovalIntent.Id = "different"
				case "new-uid":
					resp.Volume.BoundInstance.InstanceUid, resp.Volume.RemovalIntent.Expected.InstanceUid = "different", "different"
				case "no-confirmation":
					resp.Volume.RemovalIntent.ConfirmedAt = nil
				case "different-time":
					resp.Volume.RemovalIntent.RequestedAt.Seconds--
				case "aborted":
					return nil, status.Error(codes.Aborted, "stale confirm")
				}
				return resp, nil
			}}}
			native := &fakeRunnerClient{removeVolumeChecked: func(context.Context, *runnerv1.RemoveVolumeCheckedRequest, ...grpc.CallOption) (*runnerv1.RemoveVolumeCheckedResponse, error) {
				return &runnerv1.RemoveVolumeCheckedResponse{State: runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT}, nil
			}}
			if done, err := r.advanceVolumeRemoval(context.Background(), native, v); done || err == nil {
				t.Fatal("invalid registry confirmation settled deletion")
			}
		})
	}
}

func TestCheckedVolumeReuseRejectsOtherIdentity(t *testing.T) {
	for _, field := range []string{"id", "runner", "organization", "owner", "kind", "thread", "class", "definition", "legacy", "nil"} {
		t.Run(field, func(t *testing.T) {
			v := checkedTestVolume("volume", runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING)
			req := checkedTestCreateRequest(v)
			switch field {
			case "id":
				v.Meta.Id = "other"
			case "runner":
				v.RunnerId = "other"
			case "organization":
				v.OrganizationId = "other"
			case "owner":
				v.OwnerId = "other"
			case "kind":
				v.OwnerKind = runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX
			case "thread":
				v.ThreadId = "other"
			case "class":
				v.AgentId = "other"
			case "definition":
				v.VolumeId = "other"
			case "legacy":
				v.CheckedLifecycle = false
			case "nil":
				v = nil
			}
			r := &Reconciler{runners: &fakeRunnersClient{
				createVolumeChecked: func(context.Context, *runnersv1.CreateVolumeCheckedRequest, ...grpc.CallOption) (*runnersv1.CreateVolumeCheckedResponse, error) {
					return nil, status.Error(codes.AlreadyExists, "existing")
				},
				getVolume: func(context.Context, *runnersv1.GetVolumeRequest, ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error) {
					return &runnersv1.GetVolumeResponse{Volume: v}, nil
				},
				updateVolumeChecked: func(context.Context, *runnersv1.UpdateVolumeCheckedRequest, ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
					t.Fatal("foreign or unverified record reached the writer")
					return nil, errNotImplemented
				},
			}}
			if next, owned, err := r.createOrReuseCheckedVolume(context.Background(), req); err == nil || next != nil || owned {
				t.Fatal("adopted an unverified or foreign record")
			}
		})
	}
}
