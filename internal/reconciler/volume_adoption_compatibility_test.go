package reconciler

import (
	"context"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Native adoption is a separate coordinator contract, not a workload retry path.
func (*fakeRunnerClient) ReserveVolumeAnchorAdoption(context.Context, *runnerv1.ReserveVolumeAnchorAdoptionRequest, ...grpc.CallOption) (*runnerv1.ReserveVolumeAnchorAdoptionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "volume adoption is not part of workload reconciliation")
}

func (*fakeRunnerClient) ApplyVolumeAnchorAdoption(context.Context, *runnerv1.ApplyVolumeAnchorAdoptionRequest, ...grpc.CallOption) (*runnerv1.ApplyVolumeAnchorAdoptionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "volume adoption is not part of workload reconciliation")
}

func (*fakeRunnerClient) ObserveVolumeAnchorAdoption(context.Context, *runnerv1.ObserveVolumeAnchorAdoptionRequest, ...grpc.CallOption) (*runnerv1.ObserveVolumeAnchorAdoptionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "volume adoption is not part of workload reconciliation")
}

func (*fakeRunnerClient) FinalizeVolumeAnchorAdoption(context.Context, *runnerv1.FinalizeVolumeAnchorAdoptionRequest, ...grpc.CallOption) (*runnerv1.FinalizeVolumeAnchorAdoptionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "volume adoption is not part of workload reconciliation")
}
