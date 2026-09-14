package reconciler

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const checkedTestBackend = "kubernetes-namespace/v1/workloads/namespace-original"

func TestVolumeBackendRemovalRequiresMatchingResponse(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, backend := range []string{"", "wrong-backend", checkedTestBackend} {
			for _, state := range []runnerv1.VolumeRemovalState{runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING, runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT} {
				v := checkedTestVolume("volume", runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
				if sandbox {
					v = checkedTestSandboxVolume("volume", "sandbox", runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
				}
				v.BoundInstance.BackendId = checkedTestBackend
				confirms := 0
				registry := &fakeRunnersClient{updateVolumeChecked: func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
					if req.GetConfirmRemoval() != nil {
						confirms++
						if req.GetConfirmRemoval().BackendId != checkedTestBackend {
							t.Fatal("confirmation did not retain verified backend identity")
						}
					}
					return checkedTestUpdate(t, v, req), nil
				}}
				native := &fakeRunnerClient{removeVolumeBound: func(_ context.Context, req *runnerv1.RemoveVolumeBoundRequest, _ ...grpc.CallOption) (*runnerv1.RemoveVolumeBoundResponse, error) {
					if req.Expected.BackendId != checkedTestBackend {
						t.Fatal("removal did not use the stored backend identity")
					}
					return &runnerv1.RemoveVolumeBoundResponse{State: state, BackendId: backend}, nil
				}}
				done, err := (&Reconciler{runners: registry}).advanceVolumeRemoval(context.Background(), native, proto.Clone(v).(*runnersv1.Volume))
				if backend != checkedTestBackend {
					if done || err == nil || confirms != 0 || v.RemovalIntent.ConfirmedAt != nil {
						t.Fatalf("foreign/legacy backend response accepted: backend=%q state=%v done=%t err=%v confirms=%d", backend, state, done, err, confirms)
					}
				} else if err != nil || done != (state == runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT) {
					t.Fatalf("verified backend response rejected: %v, %v", done, err)
				}
			}
		}
	}
}

func TestVolumeBackendInventoryRejectsMixedOrUnknownIdentity(t *testing.T) {
	for _, backend := range []string{"", " padded ", strings.Repeat("x", 513), checkedTestBackend} {
		for _, itemBackend := range []string{"", "other", checkedTestBackend} {
			for _, populated := range []bool{false, true} {
				resp := &runnerv1.ListVolumesResponse{BackendId: backend}
				if populated {
					resp.Volumes = []*runnerv1.VolumeListItem{{VolumeKey: "volume", InstanceId: "claim", BackendId: itemBackend}}
				}
				_, err := indexRunnerVolumes(resp)
				valid := backend == checkedTestBackend && (!populated || itemBackend == backend)
				if (err == nil) != valid {
					t.Fatalf("inventory validation backend=%q item=%q populated=%t: %v", backend, itemBackend, populated, err)
				}
			}
		}
	}
}

func TestVolumeBackendMismatchDoesNotDeclareWorkspaceLost(t *testing.T) {
	for _, state := range []runnersv1.VolumeStatus{runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING} {
		for _, sandbox := range []bool{false, true} {
			f := newVolumeRetentionFixture(t)
			v := checkedTestVolume("volume", state)
			if sandbox {
				v = checkedTestSandboxVolume("volume", "sandbox", state)
			}
			f.records = []*runnersv1.Volume{v}
			f.inventory = &runnerv1.ListVolumesResponse{BackendId: "different-backend"}
			f.reconcile(t)
			if f.runnerLists != 1 || len(f.updated) != 0 || len(f.removed) != 0 || f.ownerMutations != 0 {
				t.Fatal("foreign empty inventory changed an owner, record or disk")
			}
		}
	}
}

func TestVolumeBackendOldRunnerCannotTriggerLegacyFallback(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var legacyCalls atomic.Int32
	server := grpc.NewServer()
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "agynio.api.runner.v1.RunnerService", HandlerType: (*interface{})(nil),
		Methods: []grpc.MethodDesc{{MethodName: "RemoveVolumeChecked", Handler: func(_ any, _ context.Context, decode func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			if err := decode(new(runnerv1.RemoveVolumeCheckedRequest)); err != nil {
				return nil, err
			}
			legacyCalls.Add(1)
			return &runnerv1.RemoveVolumeCheckedResponse{State: runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT}, nil
		}}},
	}, struct{}{})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	v := checkedTestVolume("volume", runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
	confirms := 0
	registry := &fakeRunnersClient{updateVolumeChecked: func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
		if req.GetConfirmRemoval() != nil {
			confirms++
		}
		return checkedTestUpdate(t, v, req), nil
	}}
	settled, err := (&Reconciler{runners: registry}).advanceVolumeRemoval(ctx, runnerv1.NewRunnerServiceClient(conn), proto.Clone(v).(*runnersv1.Volume))
	if settled || status.Code(err) != codes.Unimplemented || legacyCalls.Load() != 0 || confirms != 0 || v.RemovalIntent.ConfirmedAt != nil {
		t.Fatalf("old runner allowed fallback or confirmation: done=%t error=%v calls=%d confirms=%d", settled, err, legacyCalls.Load(), confirms)
	}
}
