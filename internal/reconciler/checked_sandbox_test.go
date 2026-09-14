package reconciler

import (
	"context"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCheckedSandboxTerminationResumesPendingDeletion(t *testing.T) {
	ctx := context.Background()
	sandboxID := uuid.NewString()
	sandbox := &agentsv1.Sandbox{
		Meta: &agentsv1.EntityMeta{Id: sandboxID}, OrganizationId: testOrganizationID, OwnerId: "fixture-sandbox-user",
		Status: agentsv1.SandboxStatus_SANDBOX_STATUS_TERMINATED,
	}
	v := checkedTestSandboxVolume("volume", sandboxID, runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
	expected := proto.Clone(v.BoundInstance).(*runnerv1.VolumeListItem)
	removals, deleted := 0, 0
	registry := &fakeRunnersClient{
		listVolumes: func(_ context.Context, req *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			if len(req.Filter.OwnerIdIn) != 1 || req.Filter.OwnerIdIn[0] != sandboxID {
				t.Fatal("sandbox registry query was not owner-scoped")
			}
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{proto.Clone(v).(*runnersv1.Volume)}}, nil
		},
		listWorkloads: func(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{}, nil
		},
		updateVolumeChecked: func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
			return checkedTestUpdate(t, v, req), nil
		},
	}
	agents := &testutil.FakeAgentsClient{DeleteSandboxFunc: func(_ context.Context, req *agentsv1.DeleteSandboxRequest, _ ...grpc.CallOption) (*agentsv1.DeleteSandboxResponse, error) {
		if req.Id != sandboxID || v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_DELETED || v.RemovalIntent.ConfirmedAt == nil {
			t.Fatal("sandbox finalized before checked physical removal")
		}
		deleted++
		return &agentsv1.DeleteSandboxResponse{}, nil
	}}
	native := &fakeRunnerClient{removeVolumeChecked: func(_ context.Context, req *runnerv1.RemoveVolumeCheckedRequest, _ ...grpc.CallOption) (*runnerv1.RemoveVolumeCheckedResponse, error) {
		removals++
		if !proto.Equal(req.Expected, expected) || req.Expected.InstanceId == v.Meta.Id {
			t.Fatal("sandbox cleanup guessed a name instead of using the stored physical identity")
		}
		state := runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING
		if removals == 2 {
			state = runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT
		}
		return &runnerv1.RemoveVolumeCheckedResponse{State: state}, nil
	}}
	dialer := &fakeRunnerDialer{dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
		if id != v.RunnerId {
			t.Fatal("sandbox cleanup dialed a different runner")
		}
		return native, nil
	}}
	first := &Reconciler{runners: registry, agents: agents, runnerDialer: dialer}
	if err := first.reconcileSandbox(ctx, proto.Clone(sandbox).(*agentsv1.Sandbox), time.Now()); err == nil || deleted != 0 || v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING {
		t.Fatalf("pending deletion did not retain sandbox: err=%v deleted=%d", err, deleted)
	}
	intentID := v.RemovalIntent.Id
	restarted := &Reconciler{runners: registry, agents: agents, runnerDialer: dialer}
	if err := restarted.reconcileSandbox(ctx, proto.Clone(sandbox).(*agentsv1.Sandbox), time.Now()); err != nil || deleted != 1 || removals != 2 || v.RemovalIntent.Id != intentID {
		t.Fatalf("restart did not finish original cleanup: err=%v deleted=%d removals=%d", err, deleted, removals)
	}
}

func TestCheckedSandboxPlanValidatesAllOwnersBeforeStopping(t *testing.T) {
	for _, corrupt := range []string{"later-workload", "volume"} {
		t.Run(corrupt, func(t *testing.T) {
			sandboxID := uuid.NewString()
			workloads := []*runnersv1.Workload{}
			for range 3 {
				workloads = append(workloads, &runnersv1.Workload{
					Meta: &runnersv1.EntityMeta{Id: uuid.NewString()}, OwnerKind: runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
					OwnerId: sandboxID, OrganizationId: testOrganizationID, RunnerId: "runner-1",
					Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED, RemovedAt: timestamppb.Now(),
				})
			}
			v := checkedTestSandboxVolume("volume", sandboxID, runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
			if corrupt == "later-workload" {
				workloads[2].OwnerId = uuid.NewString()
			} else {
				v.OrganizationId = uuid.NewString()
			}
			r := &Reconciler{
				runnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) {
					t.Fatal("partial owner validation allowed a workload stop")
					return nil, errNotImplemented
				}},
				runners: &fakeRunnersClient{
					listWorkloads: func(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
						return &runnersv1.ListWorkloadsResponse{Workloads: workloads}, nil
					},
					listVolumes: func(context.Context, *runnersv1.ListVolumesRequest, ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
						return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{v}}, nil
					},
					updateWorkload: func(context.Context, *runnersv1.UpdateWorkloadRequest, ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
						t.Fatal("partial owner validation allowed a registry write")
						return nil, errNotImplemented
					},
				},
			}
			_, err := r.loadSandboxWorkloadPlan(context.Background(), &agentsv1.Sandbox{Meta: &agentsv1.EntityMeta{Id: sandboxID}, OrganizationId: testOrganizationID})
			if err == nil {
				t.Fatal("accepted cross-owner sandbox plan")
			}
		})
	}
}

func TestCheckedVolumeListingRejectsIncompleteOrAmbiguousPages(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, scenario := range []string{"nil", "nil-later", "duplicate", "duplicate-later", "padded-id", "cycle"} {
			name := "agent/" + scenario
			if sandbox {
				name = "sandbox/" + scenario
			}
			t.Run(name, func(t *testing.T) {
				calls := 0
				v := checkedTestVolume("volume", runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
				r := &Reconciler{runners: &fakeRunnersClient{listVolumes: func(_ context.Context, req *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
					calls++
					if calls > 3 {
						t.Fatal("pagination cycle was not detected")
					}
					switch scenario {
					case "nil":
						return nil, nil
					case "nil-later":
						if req.PageToken == "" {
							return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{v}, NextPageToken: "page-2"}, nil
						}
						return nil, nil
					case "duplicate":
						return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{v, proto.Clone(v).(*runnersv1.Volume)}}, nil
					case "duplicate-later":
						token := ""
						if req.PageToken == "" {
							token = "page-2"
						}
						return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{v}, NextPageToken: token}, nil
					case "padded-id":
						v.Meta.Id = " volume "
						return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{v}}, nil
					case "cycle":
						return &runnersv1.ListVolumesResponse{NextPageToken: "repeated"}, nil
					default:
						return nil, status.Error(codes.Internal, "unknown fixture")
					}
				}}}
				var err error
				if sandbox {
					_, err = r.listSandboxVolumes(context.Background(), "sandbox")
				} else {
					_, err = r.listActiveVolumes(context.Background(), map[string]struct{}{testOrganizationID: {}})
				}
				if err == nil {
					t.Fatal("invalid listing was accepted as authoritative")
				}
			})
		}
	}
}

func TestCheckedSandboxWorkloadListingRejectsIncompletePages(t *testing.T) {
	for _, scenario := range []string{"nil", "nil-item", "padded-id", "duplicate", "duplicate-later", "cycle"} {
		t.Run(scenario, func(t *testing.T) {
			calls := 0
			w := &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload"}}
			r := &Reconciler{runners: &fakeRunnersClient{listWorkloads: func(_ context.Context, req *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
				calls++
				if calls > 3 {
					t.Fatal("workload pagination cycle was not detected")
				}
				switch scenario {
				case "nil":
					return nil, nil
				case "nil-item":
					return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{nil}}, nil
				case "padded-id":
					w.Meta.Id = " workload "
					return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{w}}, nil
				case "duplicate":
					return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{w, w}}, nil
				case "duplicate-later":
					token := ""
					if req.PageToken == "" {
						token = "page-2"
					}
					return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{w}, NextPageToken: token}, nil
				default:
					return &runnersv1.ListWorkloadsResponse{NextPageToken: "repeated"}, nil
				}
			}}}
			if _, err := r.listSandboxWorkloads(context.Background(), "sandbox"); err == nil {
				t.Fatal("incomplete workload listing was accepted as removal evidence")
			}
		})
	}
}
