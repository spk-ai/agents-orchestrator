package reconciler

import (
	"context"
	"log"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
)

func workloadStatusPtr(status runnersv1.WorkloadStatus) *runnersv1.WorkloadStatus {
	return &status
}

func volumeStatusPtr(status runnersv1.VolumeStatus) *runnersv1.VolumeStatus {
	return &status
}

func stringPtr(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func boolPtr(value bool) *bool {
	return &value
}

func (r *Reconciler) markWorkloadFailed(ctx context.Context, workloadID string, instanceID *string, reason runnersv1.WorkloadFailureReason, message string, containers []*runnersv1.Container) {
	status := runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED
	reasonValue := reason
	req := &runnersv1.UpdateWorkloadRequest{
		Id:            workloadID,
		Status:        &status,
		FailureReason: &reasonValue,
	}
	if instanceID != nil && *instanceID != "" {
		req.InstanceId = instanceID
	}
	if message != "" {
		req.FailureMessage = stringPtr(message)
	}
	if len(containers) > 0 {
		req.Containers = containers
	}
	if _, err := r.runners.UpdateWorkload(ctx, req); err != nil {
		log.Printf("reconciler: update workload %s to failed: %v", workloadID, err)
	}
}

func (r *Reconciler) markVolumeRecordsFailed(ctx context.Context, records []volumeRecord) {
	if len(records) == 0 {
		return
	}
	for _, record := range records {
		if record.checked == nil || record.checked.GetMeta().GetId() != record.id {
			continue
		}
		// Use only the revision returned to this attempt. A fresh read here
		// could compensate a different workload's provisioning generation.
		_, err := r.updateCheckedVolume(ctx, record.checked, &runnersv1.UpdateVolumeCheckedRequest{
			Operation: &runnersv1.UpdateVolumeCheckedRequest_FailProvisioning{FailProvisioning: &runnersv1.FailVolumeProvisioning{}},
		})
		if err != nil {
			log.Printf("reconciler: update volume %s to failed: %v", record.id, err)
		}
	}
}

func agentWorkloadMetadata(workloadID, runnerID string, target AgentInstanceTarget, assembled *assembler.AssembleResult, zitiIdentityID *string) *runnersv1.CreateWorkloadRequest {
	status := runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING
	zitiIdentityValue := ""
	if zitiIdentityID != nil {
		zitiIdentityValue = *zitiIdentityID
	}
	agentClassID := target.AgentID.String()
	agentInstanceID := target.AgentInstanceID.String()
	return &runnersv1.CreateWorkloadRequest{
		Id:                     workloadID,
		RunnerId:               runnerID,
		ThreadId:               agentInstanceID,
		AgentId:                agentClassID,
		OrganizationId:         assembled.OrganizationID,
		Status:                 status,
		ZitiIdentityId:         zitiIdentityValue,
		AllocatedCpuMillicores: assembled.AllocatedCPUMillicores,
		AllocatedRamBytes:      assembled.AllocatedRAMBytes,
		Flavor:                 assembled.Flavor,
		PersistentShells:       assembled.PersistentShells,
		OwnerKind:              runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
		OwnerId:                agentInstanceID,
		AgentClassId:           &agentClassID,
		AgentInstanceId:        &agentInstanceID,
	}
}

func (r *Reconciler) createVolumeRecords(ctx context.Context, records []volumeRecord, runnerID string, target AgentInstanceTarget, organizationID string) ([]volumeRecord, error) {
	if len(records) == 0 {
		return nil, nil
	}
	created := make([]volumeRecord, 0, len(records))
	for _, record := range records {
		if record.id == "" {
			return created, ErrInvalidVolumeRecord
		}
		if record.volumeID == "" {
			return created, ErrInvalidVolumeRecord
		}
		if record.sizeGB == "" {
			return created, ErrInvalidVolumeRecord
		}
		agentClassID := target.AgentID.String()
		agentInstanceID := target.AgentInstanceID.String()
		req := &runnersv1.CreateVolumeRequest{
			Id:                 record.id,
			RunnerId:           runnerID,
			ThreadId:           agentInstanceID,
			AgentId:            agentClassID,
			OrganizationId:     organizationID,
			VolumeId:           record.volumeID,
			SizeGb:             record.sizeGB,
			Status:             runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING,
			OwnerKind:          runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
			OwnerId:            agentInstanceID,
			VolumeDefinitionId: &record.volumeID,
			AgentClassId:       &agentClassID,
			AgentInstanceId:    &agentInstanceID,
		}
		checked, owned, err := r.createOrReuseCheckedVolume(ctx, req)
		if err != nil {
			return created, err
		}
		if owned {
			record.checked = checked
			created = append(created, record)
		}
	}
	return created, nil
}

func (r *Reconciler) prepareExistingVolumeRecord(ctx context.Context, req *runnersv1.CreateVolumeRequest) (*runnersv1.Volume, error) {
	resp, err := r.runners.GetVolume(ctx, &runnersv1.GetVolumeRequest{Id: req.GetId()})
	if err != nil {
		return nil, err
	}
	volume := resp.GetVolume()
	if err := validateCheckedVolume(volume); err != nil {
		return nil, err
	}
	if !volumeMatchesRequest(volume, req) {
		return nil, ErrInvalidVolumeRecord
	}
	return volume, nil
}

var ErrInvalidVolumeRecord = errInvalidVolumeRecord{}

type errInvalidVolumeRecord struct{}

func (errInvalidVolumeRecord) Error() string {
	return "invalid volume record"
}
