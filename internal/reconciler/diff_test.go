package reconciler

import (
	"reflect"
	"sort"
	"testing"
	"time"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestComputeActions(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	agent1 := uuid.New()
	thread1 := uuid.New()
	agent2 := uuid.New()
	thread2 := uuid.New()
	agent3 := uuid.New()
	thread3 := uuid.New()
	missingAgent := uuid.New()
	missingThread := uuid.New()
	idleTimeouts := map[uuid.UUID]time.Duration{
		agent1: 30 * time.Minute,
		agent2: 15 * time.Minute,
		agent3: 10 * time.Minute,
	}
	idleFallback := 12 * time.Minute

	activityOld := now.Add(-20 * time.Minute)

	workload1 := makeWorkload(agent1, thread1, now, nil)
	workload2 := makeWorkload(agent3, thread3, now.Add(-1*time.Minute), &activityOld)
	workload3 := makeWorkload(agent1, thread1, now.Add(-20*time.Minute), nil)
	workload4 := makeWorkload(agent1, thread1, now.Add(-5*time.Minute), nil)
	workload5 := makeWorkload(agent2, thread2, now.Add(-20*time.Minute), nil)
	workload6 := makeWorkload(missingAgent, missingThread, now.Add(-20*time.Minute), nil)
	duplicateOldActivity := now.Add(-2 * time.Minute)
	duplicateNewActivity := now.Add(-1 * time.Minute)
	workloadDuplicateOld := makeWorkload(agent1, thread1, now, &duplicateOldActivity)
	workloadDuplicateNew := makeWorkload(agent1, thread1, now, &duplicateNewActivity)
	workloadStopping := makeWorkload(agent2, thread2, now, nil)
	workloadStopping.Status = runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING

	cases := []struct {
		name     string
		desired  []AgentInstanceTarget
		actual   []*runnersv1.Workload
		expected Actions
	}{
		{
			name:     "empty",
			desired:  nil,
			actual:   nil,
			expected: Actions{ToStart: nil, ToStop: nil},
		},
		{
			name:    "start missing",
			desired: []AgentInstanceTarget{{AgentID: agent1, AgentInstanceID: thread1}},
			actual:  nil,
			expected: Actions{
				ToStart: []AgentInstanceTarget{{AgentID: agent1, AgentInstanceID: thread1}},
			},
		},
		{
			name:   "stop idle by activity",
			actual: []*runnersv1.Workload{workload2},
			expected: Actions{
				ToStop: []*runnersv1.Workload{workload2},
			},
		},
		{
			name:   "stop idle by created_at",
			actual: []*runnersv1.Workload{workload5},
			expected: Actions{
				ToStop: []*runnersv1.Workload{workload5},
			},
		},
		{
			name:   "stop idle with fallback",
			actual: []*runnersv1.Workload{workload6},
			expected: Actions{
				ToStop: []*runnersv1.Workload{workload6},
			},
		},
		{
			name:   "keep recent",
			actual: []*runnersv1.Workload{workload4},
		},
		{
			name:   "stop duplicates within idle",
			actual: []*runnersv1.Workload{workloadDuplicateNew, workloadDuplicateOld},
			expected: Actions{
				ToStop: []*runnersv1.Workload{workloadDuplicateNew},
			},
		},
		{
			name:    "stop duplicates with desired",
			desired: []AgentInstanceTarget{{AgentID: agent1, AgentInstanceID: thread1}},
			actual:  []*runnersv1.Workload{workloadDuplicateNew, workloadDuplicateOld},
			expected: Actions{
				ToStop: []*runnersv1.Workload{workloadDuplicateNew},
			},
		},
		{
			name:   "stop stopping workload",
			actual: []*runnersv1.Workload{workloadStopping},
			expected: Actions{
				ToStop: []*runnersv1.Workload{workloadStopping},
			},
		},
		{
			name:    "match",
			desired: []AgentInstanceTarget{{AgentID: agent1, AgentInstanceID: thread1}},
			actual:  []*runnersv1.Workload{workload1},
		},
		{
			name: "mixed",
			desired: []AgentInstanceTarget{
				{AgentID: agent1, AgentInstanceID: thread1},
				{AgentID: agent2, AgentInstanceID: thread2},
			},
			actual: []*runnersv1.Workload{workload3, workload2},
			expected: Actions{
				ToStart: []AgentInstanceTarget{{AgentID: agent2, AgentInstanceID: thread2}},
				ToStop:  []*runnersv1.Workload{workload2},
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			result, err := ComputeActions(testCase.desired, testCase.actual, nil, idleTimeouts, idleFallback, now)
			if err != nil {
				t.Fatalf("compute actions: %v", err)
			}
			sortAgentInstanceTargets(result.ToStart)
			sortWorkloads(result.ToStop)
			sortAgentInstanceTargets(testCase.expected.ToStart)
			sortWorkloads(testCase.expected.ToStop)
			if !reflect.DeepEqual(result, testCase.expected) {
				t.Fatalf("expected %+v, got %+v", testCase.expected, result)
			}
		})
	}
}

func makeWorkload(agentID, agentInstanceID uuid.UUID, createdAt time.Time, lastActivityAt *time.Time) *runnersv1.Workload {
	workload := &runnersv1.Workload{
		Meta: &runnersv1.EntityMeta{
			Id:        uuid.NewString(),
			CreatedAt: timestamppb.New(createdAt),
		},
		AgentId:         agentID.String(),
		AgentClassId:    stringPtr(agentID.String()),
		AgentInstanceId: stringPtr(agentInstanceID.String()),
	}
	if lastActivityAt != nil {
		workload.LastActivityAt = timestamppb.New(*lastActivityAt)
	}
	return workload
}

func sortAgentInstanceTargets(values []AgentInstanceTarget) {
	sort.Slice(values, func(i, j int) bool {
		if values[i].AgentID == values[j].AgentID {
			return values[i].AgentInstanceID.String() < values[j].AgentInstanceID.String()
		}
		return values[i].AgentID.String() < values[j].AgentID.String()
	})
}

func sortWorkloads(values []*runnersv1.Workload) {
	sort.Slice(values, func(i, j int) bool {
		return values[i].GetMeta().GetId() < values[j].GetMeta().GetId()
	})
}
