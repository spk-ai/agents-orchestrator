package reconciler

import (
	"context"
	"errors"
	"testing"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBuildVolumeRecordsUsesStablePersistentKey(t *testing.T) {
	agentInstanceID := uuid.New()
	volumeID := uuid.New()
	info := assembler.PersistentVolumeInfo{
		ID:              volumeID,
		AgentInstanceID: agentInstanceID,
		Volume:          &agentsv1.Volume{Size: "1Gi"},
		Spec:            &runnerv1.VolumeSpec{},
	}

	first, err := buildVolumeRecords([]assembler.PersistentVolumeInfo{info})
	if err != nil {
		t.Fatalf("build volume records: %v", err)
	}
	second, err := buildVolumeRecords([]assembler.PersistentVolumeInfo{info})
	if err != nil {
		t.Fatalf("build volume records again: %v", err)
	}

	expectedKey := uuid.NewSHA1(uuid.NameSpaceOID, []byte(agentInstanceID.String()+":"+volumeID.String())).String()
	if len(first) != 1 || first[0].id != expectedKey {
		t.Fatalf("expected first key %q, got %v", expectedKey, first)
	}
	if len(second) != 1 || second[0].id != expectedKey {
		t.Fatalf("expected second key %q, got %v", expectedKey, second)
	}
	if info.Spec.Labels[assembler.LabelVolumeKey] != expectedKey {
		t.Fatalf("expected volume label %q, got %q", expectedKey, info.Spec.Labels[assembler.LabelVolumeKey])
	}
}

func TestCreateVolumeRecordsReusesExistingActiveRecord(t *testing.T) {
	ctx := context.Background()
	recordID := uuid.NewString()
	agentInstanceID := uuid.New()
	agentID := uuid.New()
	runnerID := "runner-1"
	organizationID := uuid.NewString()
	volumeID := uuid.NewString()
	var updateCount int
	stored := checkedTestVolume(recordID, runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
	stored.ThreadId, stored.AgentId, stored.OwnerId = agentInstanceID.String(), agentID.String(), agentInstanceID.String()
	stored.RunnerId, stored.OrganizationId, stored.VolumeId = runnerID, organizationID, volumeID
	stored.BoundInstance = checkedTestInstance(stored, stored.GetInstanceId(), "existing-uid")
	runners := &fakeRunnersClient{
		createVolumeChecked: func(context.Context, *runnersv1.CreateVolumeCheckedRequest, ...grpc.CallOption) (*runnersv1.CreateVolumeCheckedResponse, error) {
			return nil, status.Error(codes.AlreadyExists, "volume exists")
		},
		getVolume: func(_ context.Context, req *runnersv1.GetVolumeRequest, _ ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error) {
			if req.GetId() != recordID {
				return nil, errNotImplemented
			}
			return &runnersv1.GetVolumeResponse{Volume: stored}, nil
		},
		updateVolumeChecked: func(context.Context, *runnersv1.UpdateVolumeCheckedRequest, ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
			updateCount++
			return &runnersv1.UpdateVolumeCheckedResponse{}, nil
		},
	}
	reconciler := &Reconciler{runners: runners}

	created, err := reconciler.createVolumeRecords(ctx, []volumeRecord{{id: recordID, volumeID: volumeID, sizeGB: "1"}}, runnerID, AgentInstanceTarget{AgentID: agentID, AgentInstanceID: agentInstanceID, ThreadID: agentInstanceID}, organizationID)
	if err != nil {
		t.Fatalf("create volume records: %v", err)
	}
	if len(created) != 0 {
		t.Fatalf("expected no newly created records, got %d", len(created))
	}
	if updateCount != 0 {
		t.Fatalf("expected no volume updates, got %d", updateCount)
	}
}

func TestCreateVolumeRecordsUsesSingleProvisioningRecordAcrossReruns(t *testing.T) {
	ctx := context.Background()
	recordID := uuid.NewString()
	agentInstanceID := uuid.New()
	agentID := uuid.New()
	runnerID := "runner-1"
	organizationID := uuid.NewString()
	volumeID := uuid.NewString()
	storedVolumes := map[string]*runnersv1.Volume{}
	var createIDs []string
	var updateCount int
	runners := &fakeRunnersClient{
		createVolumeChecked: func(_ context.Context, checked *runnersv1.CreateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.CreateVolumeCheckedResponse, error) {
			req := checked.GetVolume()
			createIDs = append(createIDs, req.GetId())
			if _, ok := storedVolumes[req.GetId()]; ok {
				return nil, status.Error(codes.AlreadyExists, "volume exists")
			}
			storedVolumes[req.GetId()] = &runnersv1.Volume{
				CheckedLifecycle: true, LifecycleRevision: 1, SizeGb: req.GetSizeGb(),
				Meta:           &runnersv1.EntityMeta{Id: req.GetId()},
				ThreadId:       req.GetThreadId(),
				AgentId:        req.GetAgentId(),
				RunnerId:       req.GetRunnerId(),
				VolumeId:       req.GetVolumeId(),
				OrganizationId: req.GetOrganizationId(),
				Status:         runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING,
				OwnerKind:      req.GetOwnerKind(),
				OwnerId:        req.GetOwnerId(),
			}
			return &runnersv1.CreateVolumeCheckedResponse{Volume: storedVolumes[req.GetId()]}, nil
		},
		getVolume: func(_ context.Context, req *runnersv1.GetVolumeRequest, _ ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error) {
			volume, ok := storedVolumes[req.GetId()]
			if !ok {
				return nil, errNotImplemented
			}
			return &runnersv1.GetVolumeResponse{Volume: volume}, nil
		},
		updateVolumeChecked: func(context.Context, *runnersv1.UpdateVolumeCheckedRequest, ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
			updateCount++
			return &runnersv1.UpdateVolumeCheckedResponse{}, nil
		},
	}
	reconciler := &Reconciler{runners: runners}
	target := AgentInstanceTarget{AgentID: agentID, AgentInstanceID: agentInstanceID, ThreadID: agentInstanceID}
	records := []volumeRecord{{id: recordID, volumeID: volumeID, sizeGB: "1"}}

	firstCreated, err := reconciler.createVolumeRecords(ctx, records, runnerID, target, organizationID)
	if err != nil {
		t.Fatalf("create first volume records: %v", err)
	}
	secondCreated, err := reconciler.createVolumeRecords(ctx, records, runnerID, target, organizationID)
	if err != nil {
		t.Fatalf("create second volume records: %v", err)
	}

	if len(firstCreated) != 1 {
		t.Fatalf("expected first run to create one record, got %d", len(firstCreated))
	}
	if len(secondCreated) != 0 {
		t.Fatalf("expected second run to reuse existing record, got %d created", len(secondCreated))
	}
	if len(storedVolumes) != 1 {
		t.Fatalf("expected one stored provisioning record, got %d", len(storedVolumes))
	}
	if len(createIDs) != 2 || createIDs[0] != recordID || createIDs[1] != recordID {
		t.Fatalf("expected both reruns to use stable record id %q, got %v", recordID, createIDs)
	}
	if updateCount != 0 {
		t.Fatalf("expected no stale pending cleanup/reactivation updates, got %d", updateCount)
	}
}

func TestCreateVolumeRecordsDoesNotReactivateFailedRecord(t *testing.T) {
	ctx := context.Background()
	recordID := uuid.NewString()
	agentInstanceID := uuid.New()
	agentID := uuid.New()
	runnerID := "runner-1"
	organizationID := uuid.NewString()
	volumeID := uuid.NewString()
	var updateCount int
	runners := &fakeRunnersClient{
		createVolumeChecked: func(context.Context, *runnersv1.CreateVolumeCheckedRequest, ...grpc.CallOption) (*runnersv1.CreateVolumeCheckedResponse, error) {
			return nil, status.Error(codes.AlreadyExists, "volume exists")
		},
		getVolume: func(context.Context, *runnersv1.GetVolumeRequest, ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error) {
			return &runnersv1.GetVolumeResponse{Volume: &runnersv1.Volume{
				Meta:           &runnersv1.EntityMeta{Id: recordID},
				ThreadId:       agentInstanceID.String(),
				AgentId:        agentID.String(),
				RunnerId:       runnerID,
				VolumeId:       volumeID,
				OrganizationId: organizationID,
				Status:         runnersv1.VolumeStatus_VOLUME_STATUS_FAILED,
				OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
				OwnerId:        agentInstanceID.String(),
			}}, nil
		},
		updateVolumeChecked: func(context.Context, *runnersv1.UpdateVolumeCheckedRequest, ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
			updateCount++
			return &runnersv1.UpdateVolumeCheckedResponse{}, nil
		},
	}
	reconciler := &Reconciler{runners: runners}

	_, err := reconciler.createVolumeRecords(ctx, []volumeRecord{{id: recordID, volumeID: volumeID, sizeGB: "1"}}, runnerID, AgentInstanceTarget{AgentID: agentID, AgentInstanceID: agentInstanceID, ThreadID: agentInstanceID}, organizationID)
	if err == nil {
		t.Fatal("expected terminal existing record to fail")
	}
	if updateCount != 0 {
		t.Fatalf("expected no failed record reactivation, got %d updates", updateCount)
	}
}

func TestCreateVolumeRecordsDoesNotFailRecordOnCreateError(t *testing.T) {
	ctx := context.Background()
	recordID := uuid.NewString()
	agentInstanceID := uuid.New()
	agentID := uuid.New()
	volumeID := uuid.NewString()
	var updateCount int
	runners := &fakeRunnersClient{
		createVolumeChecked: func(context.Context, *runnersv1.CreateVolumeCheckedRequest, ...grpc.CallOption) (*runnersv1.CreateVolumeCheckedResponse, error) {
			return nil, errors.New("runtime unavailable")
		},
		updateVolumeChecked: func(context.Context, *runnersv1.UpdateVolumeCheckedRequest, ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
			updateCount++
			return &runnersv1.UpdateVolumeCheckedResponse{}, nil
		},
	}
	reconciler := &Reconciler{runners: runners}

	_, err := reconciler.createVolumeRecords(ctx, []volumeRecord{{id: recordID, volumeID: volumeID, sizeGB: "1"}}, "runner-1", AgentInstanceTarget{AgentID: agentID, AgentInstanceID: agentInstanceID, ThreadID: agentInstanceID}, uuid.NewString())
	if err == nil {
		t.Fatal("expected create error")
	}
	if updateCount != 0 {
		t.Fatalf("expected no volume failure updates, got %d", updateCount)
	}
}

func TestPrepareExistingVolumeRecordRejectsFailedRecord(t *testing.T) {
	ctx := context.Background()
	recordID := uuid.NewString()
	agentInstanceID := uuid.NewString()
	agentID := uuid.NewString()
	runnerID := "runner-1"
	organizationID := uuid.NewString()
	volumeID := uuid.NewString()
	runners := &fakeRunnersClient{
		getVolume: func(_ context.Context, req *runnersv1.GetVolumeRequest, _ ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error) {
			if req.GetId() != recordID {
				return nil, errNotImplemented
			}
			return &runnersv1.GetVolumeResponse{Volume: &runnersv1.Volume{
				Meta:           &runnersv1.EntityMeta{Id: recordID},
				ThreadId:       agentInstanceID,
				AgentId:        agentID,
				RunnerId:       runnerID,
				VolumeId:       volumeID,
				OrganizationId: organizationID,
				Status:         runnersv1.VolumeStatus_VOLUME_STATUS_FAILED,
				OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
				OwnerId:        agentInstanceID,
			}}, nil
		},
	}
	reconciler := &Reconciler{runners: runners}
	req := &runnersv1.CreateVolumeRequest{
		Id:             recordID,
		ThreadId:       agentInstanceID,
		AgentId:        agentID,
		RunnerId:       runnerID,
		VolumeId:       volumeID,
		OrganizationId: organizationID,
		OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
		OwnerId:        agentInstanceID,
	}

	if _, err := reconciler.prepareExistingVolumeRecord(ctx, req); err == nil {
		t.Fatal("expected failed existing record to remain terminal")
	}
}

func TestPrepareExistingVolumeRecordRejectsConflictingRecord(t *testing.T) {
	ctx := context.Background()
	recordID := uuid.NewString()
	agentInstanceID := uuid.NewString()
	runners := &fakeRunnersClient{
		getVolume: func(context.Context, *runnersv1.GetVolumeRequest, ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error) {
			return &runnersv1.GetVolumeResponse{Volume: &runnersv1.Volume{
				Meta:           &runnersv1.EntityMeta{Id: recordID},
				ThreadId:       uuid.NewString(),
				AgentId:        uuid.NewString(),
				RunnerId:       "runner-1",
				VolumeId:       uuid.NewString(),
				OrganizationId: uuid.NewString(),
				Status:         runnersv1.VolumeStatus_VOLUME_STATUS_FAILED,
				OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
				OwnerId:        agentInstanceID,
			}}, nil
		},
	}
	reconciler := &Reconciler{runners: runners}
	req := &runnersv1.CreateVolumeRequest{
		Id:             recordID,
		ThreadId:       agentInstanceID,
		AgentId:        uuid.NewString(),
		RunnerId:       "runner-1",
		VolumeId:       uuid.NewString(),
		OrganizationId: uuid.NewString(),
		OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
		OwnerId:        agentInstanceID,
	}

	if _, err := reconciler.prepareExistingVolumeRecord(ctx, req); err == nil {
		t.Fatal("expected conflicting existing record to fail")
	}
}
