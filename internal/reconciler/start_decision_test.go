package reconciler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestShouldStartWorkloadStripsRunnerIdentityContext(t *testing.T) {
	ctx := context.Background()
	agentInstanceID := uuid.New()
	agentID := uuid.MustParse(testAgentID)
	listCalls := 0

	runners := &fakeRunnersClient{
		listWorkloadsByAgentInstance: func(ctx context.Context, req *runnersv1.ListWorkloadsByAgentInstanceRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsByAgentInstanceResponse, error) {
			listCalls++
			if req.GetAgentInstanceId() != agentInstanceID.String() {
				return nil, errors.New("unexpected agent instance id")
			}
			metadataValues, _ := metadata.FromOutgoingContext(ctx)
			identityValues := metadataValues.Get(identityMetadataKey)
			if len(identityValues) != 0 {
				return nil, fmt.Errorf("unexpected identity metadata: %v", identityValues)
			}
			return &runnersv1.ListWorkloadsByAgentInstanceResponse{}, nil
		},
	}

	reconciler := newTestReconciler(Config{Runners: runners})
	shouldStart, err := reconciler.shouldStartWorkload(ctx, AgentInstanceTarget{AgentID: agentID, AgentInstanceID: agentInstanceID}, time.Now().UTC(), map[uuid.UUID]time.Time{})
	if err != nil {
		t.Fatalf("should start workload: %v", err)
	}
	if !shouldStart {
		t.Fatal("expected start decision to allow workload")
	}
	if listCalls == 0 {
		t.Fatal("expected list workloads calls")
	}
}

func TestShouldStartWorkloadSkipsActiveWorkloads(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	agentID := uuid.New()
	agentInstanceID := uuid.New()
	fixture := startDecisionFixture{
		t:               t,
		agentInstanceID: agentInstanceID,
		active: []*runnersv1.Workload{
			makeDecisionWorkload("active", runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, now.Add(-2*time.Minute), time.Time{}),
		},
	}

	runners := &fakeRunnersClient{listWorkloadsByAgentInstance: fixture.list}
	reconciler := newTestReconciler(Config{Runners: runners})

	shouldStart, err := reconciler.shouldStartWorkload(ctx, AgentInstanceTarget{AgentID: agentID, AgentInstanceID: agentInstanceID}, now, map[uuid.UUID]time.Time{agentID: now.Add(-time.Hour)})
	if err != nil {
		t.Fatalf("should start workload: %v", err)
	}
	if shouldStart {
		t.Fatal("expected start decision to be blocked")
	}
}

func TestShouldStartWorkloadAllowsStoppedOrEmpty(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	agentID := uuid.New()
	agentInstanceID := uuid.New()
	fixture := startDecisionFixture{
		t:               t,
		agentInstanceID: agentInstanceID,
		latest: []*runnersv1.Workload{
			makeDecisionWorkload("stopped", runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED, now.Add(-time.Hour), now.Add(-time.Hour)),
		},
	}

	runners := &fakeRunnersClient{listWorkloadsByAgentInstance: fixture.list}
	reconciler := newTestReconciler(Config{Runners: runners})

	shouldStart, err := reconciler.shouldStartWorkload(ctx, AgentInstanceTarget{AgentID: agentID, AgentInstanceID: agentInstanceID}, now, map[uuid.UUID]time.Time{})
	if err != nil {
		t.Fatalf("should start workload: %v", err)
	}
	if !shouldStart {
		t.Fatal("expected start decision to allow workload")
	}
}

func TestShouldStartWorkloadRetriesAfterAgentUpdate(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	agentID := uuid.New()
	agentInstanceID := uuid.New()
	removedAt := now.Add(-5 * time.Minute)
	fixture := startDecisionFixture{
		t:               t,
		agentInstanceID: agentInstanceID,
		latest: []*runnersv1.Workload{
			makeDecisionWorkload("failed", runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED, removedAt.Add(-time.Minute), removedAt),
		},
	}

	runners := &fakeRunnersClient{listWorkloadsByAgentInstance: fixture.list}
	reconciler := newTestReconciler(Config{Runners: runners})

	shouldStart, err := reconciler.shouldStartWorkload(ctx, AgentInstanceTarget{AgentID: agentID, AgentInstanceID: agentInstanceID}, now, map[uuid.UUID]time.Time{agentID: removedAt.Add(time.Minute)})
	if err != nil {
		t.Fatalf("should start workload: %v", err)
	}
	if !shouldStart {
		t.Fatal("expected start decision to allow workload")
	}
}

func TestShouldStartWorkloadBackoffSchedule(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2024, 10, 10, 9, 0, 0, 0, time.UTC)
	agentID := uuid.New()
	agentInstanceID := uuid.New()
	updatedAt := base.Add(-24 * time.Hour)

	for failures := 1; failures <= 6; failures++ {
		backoffIndex := failures - 1
		if backoffIndex >= len(startBackoffSchedule) {
			backoffIndex = len(startBackoffSchedule) - 1
		}
		backoff := startBackoffSchedule[backoffIndex]

		for _, delta := range []struct {
			name        string
			removedAt   time.Time
			shouldStart bool
		}{
			{name: "before", removedAt: base.Add(-backoff + time.Second), shouldStart: false},
			{name: "after", removedAt: base.Add(-backoff - time.Second), shouldStart: true},
		} {
			t.Run(fmt.Sprintf("failures-%d-%s", failures, delta.name), func(t *testing.T) {
				failuresList := makeDecisionFailures(failures, delta.removedAt)
				fixture := startDecisionFixture{
					t:               t,
					agentInstanceID: agentInstanceID,
					latest:          []*runnersv1.Workload{failuresList[0]},
					failed:          failuresList,
				}

				runners := &fakeRunnersClient{listWorkloadsByAgentInstance: fixture.list}
				reconciler := newTestReconciler(Config{Runners: runners})

				shouldStart, err := reconciler.shouldStartWorkload(ctx, AgentInstanceTarget{AgentID: agentID, AgentInstanceID: agentInstanceID}, base, map[uuid.UUID]time.Time{agentID: updatedAt})
				if err != nil {
					t.Fatalf("should start workload: %v", err)
				}
				if shouldStart != delta.shouldStart {
					t.Fatalf("expected shouldStart=%v, got %v", delta.shouldStart, shouldStart)
				}
			})
		}
	}
}

// A runner-reported failure carries no removed_at; the row's updated_at
// stands in, so the instance backs off instead of erroring every cycle.
func TestShouldStartWorkloadWaitsForReportedFailureRemoval(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2024, 10, 10, 9, 0, 0, 0, time.UTC)
	agentID := uuid.New()
	agentInstanceID := uuid.New()

	reported := makeDecisionWorkload("reported", runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED, base.Add(-time.Minute), time.Time{})
	reported.Meta.UpdatedAt = timestamppb.New(base.Add(-5 * time.Second))

	fixture := startDecisionFixture{
		t:               t,
		agentInstanceID: agentInstanceID,
		latest:          []*runnersv1.Workload{reported},
		failed:          []*runnersv1.Workload{reported},
		active:          []*runnersv1.Workload{reported},
	}
	runners := &fakeRunnersClient{listWorkloadsByAgentInstance: fixture.list}
	reconciler := newTestReconciler(Config{Runners: runners})
	agentUpdatedAt := map[uuid.UUID]time.Time{agentID: base.Add(-24 * time.Hour)}

	shouldStart, err := reconciler.shouldStartWorkload(ctx, AgentInstanceTarget{AgentID: agentID, AgentInstanceID: agentInstanceID}, base, agentUpdatedAt)
	if err != nil {
		t.Fatalf("should start workload: %v", err)
	}
	if shouldStart {
		t.Fatal("expected backoff within the first window")
	}

	shouldStart, err = reconciler.shouldStartWorkload(ctx, AgentInstanceTarget{AgentID: agentID, AgentInstanceID: agentInstanceID}, base.Add(time.Minute), agentUpdatedAt)
	if err != nil {
		t.Fatalf("should start workload: %v", err)
	}
	if shouldStart {
		t.Fatal("elapsed backoff is not evidence of removal")
	}
}

func TestShouldStartWorkloadResetsOnLastStopped(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2024, 10, 10, 9, 0, 0, 0, time.UTC)
	agentID := uuid.New()
	agentInstanceID := uuid.New()
	updatedAt := base
	lastStoppedAt := base.Add(2 * time.Minute)
	latestRemovedAt := base.Add(4*time.Minute + 15*time.Second)

	fixture := startDecisionFixture{
		t:               t,
		agentInstanceID: agentInstanceID,
		latest: []*runnersv1.Workload{
			makeDecisionWorkload("latest", runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED, latestRemovedAt.Add(-time.Minute), latestRemovedAt),
		},
		stopped: []*runnersv1.Workload{
			makeDecisionWorkload("stopped", runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED, lastStoppedAt, lastStoppedAt),
		},
		failed: []*runnersv1.Workload{
			makeDecisionWorkload("recent", runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED, latestRemovedAt.Add(-2*time.Second), latestRemovedAt),
			makeDecisionWorkload("old", runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED, base.Add(time.Minute), base.Add(time.Minute)),
		},
	}

	runners := &fakeRunnersClient{listWorkloadsByAgentInstance: fixture.list}
	reconciler := newTestReconciler(Config{Runners: runners})

	shouldStart, err := reconciler.shouldStartWorkload(ctx, AgentInstanceTarget{AgentID: agentID, AgentInstanceID: agentInstanceID}, base.Add(4*time.Minute+30*time.Second), map[uuid.UUID]time.Time{agentID: updatedAt})
	if err != nil {
		t.Fatalf("should start workload: %v", err)
	}
	if !shouldStart {
		t.Fatal("expected start decision to allow workload")
	}
}

func TestShouldStartWorkloadPausesAfterMaxFailures(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	agentID := uuid.New()
	agentInstanceID := uuid.New()

	var pauseRequest *agentsv1.PauseInstanceRequest
	var pauseIdentities []string
	agents := &fakeAgentsClient{
		pauseInstance: func(ctx context.Context, req *agentsv1.PauseInstanceRequest, _ ...grpc.CallOption) (*agentsv1.PauseInstanceResponse, error) {
			pauseRequest = req
			if md, ok := metadata.FromOutgoingContext(ctx); ok {
				pauseIdentities = md.Get(identityMetadataKey)
			}
			return &agentsv1.PauseInstanceResponse{}, nil
		},
	}

	failures := makeDecisionFailures(maxStartAttempts, now.Add(-2*time.Minute))
	fixture := startDecisionFixture{
		t:               t,
		agentInstanceID: agentInstanceID,
		latest:          []*runnersv1.Workload{failures[0]},
		failed:          failures,
	}

	runners := &fakeRunnersClient{listWorkloadsByAgentInstance: fixture.list}
	reconciler := newTestReconciler(Config{Runners: runners, Agents: agents})

	shouldStart, err := reconciler.shouldStartWorkload(ctx, AgentInstanceTarget{AgentID: agentID, AgentInstanceID: agentInstanceID}, now, map[uuid.UUID]time.Time{agentID: now.Add(-time.Hour)})
	if err != nil {
		t.Fatalf("should start workload: %v", err)
	}
	if shouldStart {
		t.Fatal("expected start decision to be blocked")
	}
	if pauseRequest == nil {
		t.Fatal("expected pause instance request")
	}
	if pauseRequest.GetId() != agentInstanceID.String() {
		t.Fatalf("unexpected paused instance id: %s", pauseRequest.GetId())
	}
	if pauseRequest.GetPauseReason() != pauseReasonStartFailuresExhausted {
		t.Fatalf("unexpected pause reason: %s", pauseRequest.GetPauseReason())
	}
	// The surrounding runner reads carry the instance's identity, and carrying
	// it here too got the pause refused: an instance holds no rights over its
	// own class. Agents serves the reconciler as an internal caller instead.
	if len(pauseIdentities) != 0 {
		t.Fatalf("expected no caller identity on the pause, got %v", pauseIdentities)
	}
}

type startDecisionFixture struct {
	t               *testing.T
	agentInstanceID uuid.UUID
	active          []*runnersv1.Workload
	latest          []*runnersv1.Workload
	stopped         []*runnersv1.Workload
	failed          []*runnersv1.Workload
}

func (f startDecisionFixture) list(_ context.Context, req *runnersv1.ListWorkloadsByAgentInstanceRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsByAgentInstanceResponse, error) {
	f.t.Helper()
	if req.GetAgentInstanceId() != f.agentInstanceID.String() {
		return nil, fmt.Errorf("unexpected agent instance id: %s", req.GetAgentInstanceId())
	}
	statuses := req.GetStatuses()
	switch {
	case matchStatuses(statuses, []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
	}):
		return &runnersv1.ListWorkloadsByAgentInstanceResponse{Workloads: f.active}, nil
	case matchStatuses(statuses, []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
	}):
		return &runnersv1.ListWorkloadsByAgentInstanceResponse{Workloads: f.latest}, nil
	case matchStatuses(statuses, []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
	}):
		return &runnersv1.ListWorkloadsByAgentInstanceResponse{Workloads: f.stopped}, nil
	case matchStatuses(statuses, []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
	}):
		return &runnersv1.ListWorkloadsByAgentInstanceResponse{Workloads: f.failed}, nil
	default:
		return nil, fmt.Errorf("unexpected statuses: %v", statuses)
	}
}

func matchStatuses(got, want []runnersv1.WorkloadStatus) bool {
	if len(got) != len(want) {
		return false
	}
	gotCopy := append([]runnersv1.WorkloadStatus(nil), got...)
	wantCopy := append([]runnersv1.WorkloadStatus(nil), want...)
	sort.Slice(gotCopy, func(i, j int) bool { return gotCopy[i] < gotCopy[j] })
	sort.Slice(wantCopy, func(i, j int) bool { return wantCopy[i] < wantCopy[j] })
	for i := range gotCopy {
		if gotCopy[i] != wantCopy[i] {
			return false
		}
	}
	return true
}

func makeDecisionWorkload(id string, status runnersv1.WorkloadStatus, createdAt time.Time, removedAt time.Time) *runnersv1.Workload {
	workload := &runnersv1.Workload{
		Meta:   &runnersv1.EntityMeta{Id: id, CreatedAt: timestamppb.New(createdAt)},
		Status: status,
	}
	if !removedAt.IsZero() {
		workload.RemovedAt = timestamppb.New(removedAt)
	}
	return workload
}

func makeDecisionFailures(count int, latestRemovedAt time.Time) []*runnersv1.Workload {
	if count <= 0 {
		return nil
	}
	failures := make([]*runnersv1.Workload, 0, count)
	for i := 0; i < count; i++ {
		createdAt := latestRemovedAt.Add(-time.Duration(i) * time.Minute)
		removedAt := time.Time{}
		if i == 0 {
			removedAt = latestRemovedAt
		}
		failures = append(failures, makeDecisionWorkload(fmt.Sprintf("failure-%d", i), runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED, createdAt, removedAt))
	}
	return failures
}
