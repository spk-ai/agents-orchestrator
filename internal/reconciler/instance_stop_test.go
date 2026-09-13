package reconciler

import (
	"context"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestInactiveInstanceStopRequests(t *testing.T) {
	for _, tc := range []struct {
		name     string
		state    agentsv1.AgentInstanceState
		code     codes.Code
		mismatch string
		disabled bool
		desired  bool
		wantStop bool
	}{
		{name: "paused with fresh keepalives", state: agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_PAUSED, wantStop: true},
		{name: "terminated", state: agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_TERMINATED, wantStop: true},
		{name: "active and idle is not paused", state: agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_ACTIVE},
		{name: "unspecified"},
		{name: "unavailable", code: codes.Unavailable},
		{name: "not found", code: codes.NotFound},
		{name: "wrong instance", state: agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_PAUSED, mismatch: "instance"},
		{name: "wrong class", state: agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_PAUSED, mismatch: "class"},
		{name: "wrong organization", state: agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_PAUSED, mismatch: "organization"},
		{name: "nil response", mismatch: "nil"},
		{name: "disabled", state: agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_PAUSED, disabled: true},
		{name: "desired instance skipped", state: agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_ACTIVE, desired: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			agentID, instanceID := uuid.New(), uuid.New()
			now := time.Now()
			workload := makeWorkload(agentID, instanceID, now, &now)
			workload.OrganizationId = testOrganizationID
			reads := 0
			client := &testutil.FakeAgentsClient{GetInstanceFunc: func(ctx context.Context, req *agentsv1.GetInstanceRequest, _ ...grpc.CallOption) (*agentsv1.GetInstanceResponse, error) {
				reads++
				md, _ := metadata.FromOutgoingContext(ctx)
				if ids := md.Get(identityMetadataKey); len(ids) != 1 || ids[0] != testPlatformIdentityID.String() {
					t.Fatal("lifecycle read must use the orchestrator identity")
				}
				if req.GetId() != instanceID.String() {
					t.Fatal("wrong instance requested")
				}
				if tc.code != codes.OK {
					return nil, status.Error(tc.code, "lifecycle unavailable")
				}
				instance := &agentsv1.AgentInstance{Meta: &agentsv1.EntityMeta{Id: instanceID.String()}, AgentId: agentID.String(), OrganizationId: testOrganizationID, State: tc.state}
				switch tc.mismatch {
				case "instance":
					instance.Meta.Id = uuid.NewString()
				case "class":
					instance.AgentId = uuid.NewString()
				case "organization":
					instance.OrganizationId = uuid.NewString()
				case "nil":
					return nil, nil
				}
				return &agentsv1.GetInstanceResponse{Instance: instance}, nil
			}}
			r := newTestReconciler(Config{Agents: client, StopInactiveInstances: !tc.disabled})
			var desired []AgentInstanceTarget
			if tc.desired {
				desired = []AgentInstanceTarget{{AgentID: agentID, AgentInstanceID: instanceID}}
			}
			requests, err := r.inactiveInstanceStopRequests(context.Background(), desired, []*runnersv1.Workload{workload, workload})
			if err != nil {
				t.Fatal(err)
			}
			_, requested := requests[instanceID]
			if requested != tc.wantStop {
				t.Fatalf("immediate stop = %t, want %t", requested, tc.wantStop)
			}
			wantReads := 1
			if tc.disabled || tc.desired {
				wantReads = 0
			}
			if reads != wantReads {
				t.Fatalf("reads = %d, want %d (deduplicated)", reads, wantReads)
			}
		})
	}
}

func TestComputeActionsHonorsImmediateStop(t *testing.T) {
	now := time.Now()
	agentID, instanceID, activeID := uuid.New(), uuid.New(), uuid.New()
	first := makeWorkload(agentID, instanceID, now, &now)
	second := makeWorkload(agentID, instanceID, now, &now)
	second.Status = runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING
	second.Meta.CreatedAt = nil
	second.LastActivityAt = nil
	active := makeWorkload(agentID, activeID, now, &now)
	requests := map[uuid.UUID]struct{}{instanceID: {}}
	desired := []AgentInstanceTarget{{AgentID: agentID, AgentInstanceID: instanceID}}
	actions, err := ComputeActions(desired, []*runnersv1.Workload{first, second, active}, requests, nil, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(actions.ToStart) != 0 || len(actions.ToStop) != 2 || actions.ToStop[0] != first || actions.ToStop[1] != second {
		t.Fatalf("unexpected actions: %+v", actions)
	}
}
