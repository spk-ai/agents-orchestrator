package reconciler

import (
	"testing"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func checkedTestInstance(v *runnersv1.Volume, name, uid string) *runnerv1.VolumeListItem {
	labels := map[string]string{
		"app.kubernetes.io/managed-by": "k8s-runner", "managed-by": "agents-orchestrator",
		"agyn.dev/managed-by": "agents-orchestrator", "volume_key": v.Meta.Id,
	}
	if isSandboxVolume(v) {
		labels["sandbox-id"], labels["sandbox-owner-id"] = v.OwnerId, "fixture-sandbox-user"
	} else {
		labels["agent-instance-id"], labels["agent-id"] = v.OwnerId, v.AgentId
	}
	return &runnerv1.VolumeListItem{VolumeKey: v.Meta.Id, InstanceId: name, InstanceUid: uid, IdentityLabels: labels, BackendId: checkedTestBackend}
}

func checkedTestVolume(key string, state runnersv1.VolumeStatus) *runnersv1.Volume {
	v := &runnersv1.Volume{
		Meta: &runnersv1.EntityMeta{Id: key}, RunnerId: "runner-1", OrganizationId: testOrganizationID,
		OwnerKind: runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE, OwnerId: testAgentID,
		AgentId: testAgentID, ThreadId: testAgentID, VolumeId: "definition-1", SizeGb: "1",
		CheckedLifecycle: true, LifecycleRevision: 1, Status: state,
	}
	switch state {
	case runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING, runnersv1.VolumeStatus_VOLUME_STATUS_DELETED:
		v.BoundInstance = checkedTestInstance(v, "pvc-"+key, "uid-"+key)
		v.InstanceId = stringPtr(v.BoundInstance.InstanceId)
		v.LifecycleRevision = 2
		if state != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE {
			v.LifecycleRevision = 3
			v.RemovalIntent = &runnersv1.VolumeRemovalIntent{
				Id: "intent-" + key, Expected: proto.Clone(v.BoundInstance).(*runnerv1.VolumeListItem), RequestedAt: timestamppb.Now(),
			}
			if state == runnersv1.VolumeStatus_VOLUME_STATUS_DELETED {
				v.LifecycleRevision = 4
				v.RemovalIntent.ConfirmedAt = timestamppb.Now()
			}
		}
	}
	return v
}

func checkedTestSandboxVolume(key, sandboxID string, state runnersv1.VolumeStatus) *runnersv1.Volume {
	v := checkedTestVolume(key, state)
	v.OwnerKind, v.OwnerId = runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX, sandboxID
	v.AgentId, v.ThreadId = "", ""
	if v.BoundInstance != nil {
		v.BoundInstance = checkedTestInstance(v, v.GetInstanceId(), v.BoundInstance.InstanceUid)
		if v.RemovalIntent != nil {
			v.RemovalIntent.Expected = proto.Clone(v.BoundInstance).(*runnerv1.VolumeListItem)
		}
	}
	return v
}

func checkedTestCreate(req *runnersv1.CreateVolumeRequest) *runnersv1.CreateVolumeCheckedResponse {
	definition, class := req.GetVolumeId(), req.GetAgentId()
	if req.VolumeDefinitionId != nil {
		definition = req.GetVolumeDefinitionId()
	}
	if req.AgentClassId != nil {
		class = req.GetAgentClassId()
	}
	return &runnersv1.CreateVolumeCheckedResponse{Volume: &runnersv1.Volume{
		Meta: &runnersv1.EntityMeta{Id: req.GetId()}, RunnerId: req.GetRunnerId(), OrganizationId: req.GetOrganizationId(),
		OwnerKind: req.GetOwnerKind(), OwnerId: req.GetOwnerId(), ThreadId: req.GetThreadId(), AgentId: class,
		VolumeId: definition, SizeGb: req.GetSizeGb(), Status: req.GetStatus(), CheckedLifecycle: true, LifecycleRevision: 1,
	}}
}

// Explicit fixture state machine. It never promotes legacy records, supplies
// missing inventory UIDs or silently redirects old RPC callbacks to new ones.
func checkedTestUpdate(t *testing.T, v *runnersv1.Volume, req *runnersv1.UpdateVolumeCheckedRequest) *runnersv1.UpdateVolumeCheckedResponse {
	t.Helper()
	if !v.GetCheckedLifecycle() || req.GetId() != v.GetMeta().GetId() || req.GetExpectedRevision() != v.GetLifecycleRevision() {
		t.Fatalf("unexpected checked fixture update: id=%s revision=%d", req.GetId(), req.GetExpectedRevision())
	}
	next := proto.Clone(v).(*runnersv1.Volume)
	switch op := req.Operation.(type) {
	case *runnersv1.UpdateVolumeCheckedRequest_Bind:
		next.Status = runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE
		next.BoundInstance = proto.Clone(op.Bind.Instance).(*runnerv1.VolumeListItem)
		next.InstanceId = stringPtr(next.BoundInstance.InstanceId)
	case *runnersv1.UpdateVolumeCheckedRequest_BeginRemoval:
		if next.RemovalIntent == nil {
			next.RemovalIntent = &runnersv1.VolumeRemovalIntent{
				Id: "intent-" + next.Meta.Id, Expected: proto.Clone(next.BoundInstance).(*runnerv1.VolumeListItem), RequestedAt: timestamppb.Now(),
			}
		}
		next.Status = runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING
	case *runnersv1.UpdateVolumeCheckedRequest_ConfirmRemoval:
		if op.ConfirmRemoval.GetIntentId() != next.GetRemovalIntent().GetId() || op.ConfirmRemoval.GetBackendId() != next.GetRemovalIntent().GetExpected().GetBackendId() {
			t.Fatal("fixture received the wrong confirmation intent")
		}
		next.Status = runnersv1.VolumeStatus_VOLUME_STATUS_DELETED
		next.RemovalIntent.ConfirmedAt = timestamppb.Now()
	case *runnersv1.UpdateVolumeCheckedRequest_FailProvisioning:
		next.Status = runnersv1.VolumeStatus_VOLUME_STATUS_FAILED
		next.RemovedAt = timestamppb.Now()
	case *runnersv1.UpdateVolumeCheckedRequest_Reopen:
		if next.Status == runnersv1.VolumeStatus_VOLUME_STATUS_DELETED {
			next.BoundInstance, next.InstanceId = nil, nil
		}
		next.Status, next.SizeGb = runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING, op.Reopen.GetVolume().GetSizeGb()
		next.RemovalIntent, next.RemovedAt = nil, nil
	default:
		t.Fatalf("unsupported checked fixture operation %T", req.Operation)
	}
	next.LifecycleRevision++
	proto.Reset(v)
	proto.Merge(v, next)
	return &runnersv1.UpdateVolumeCheckedResponse{Volume: proto.Clone(next).(*runnersv1.Volume)}
}
