package reconciler

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/runnerdial"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const activeVolumePageSize int32 = 100
const workloadHistoryPageSize int32 = 100

type instanceActivity struct {
	hasActive       bool
	latestRemovedAt *time.Time
}

type volumeTTLInfo struct {
	persistent bool
	ttl        *time.Duration
}

// volumeIdentityID is the identity a volume pins its runner to: the sandbox for
// a sandbox volume, the agent instance for an agent volume.
//
// owner_id carries both and is preferred. agent_instance_id covers rows written
// before owner_kind existed but after instances did. agent_id is the last
// resort, and only that: it names the class, so pinning on it would tie every
// instance of an agent to a single runner.
func volumeIdentityID(volume *runnersv1.Volume) string {
	if ownerID := strings.TrimSpace(volume.GetOwnerId()); ownerID != "" {
		return ownerID
	}
	if instanceID := strings.TrimSpace(volume.GetAgentInstanceId()); instanceID != "" {
		return instanceID
	}
	return strings.TrimSpace(volume.GetAgentId())
}

func isSandboxVolume(volume *runnersv1.Volume) bool {
	return volume.GetOwnerKind() == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX
}

// runnerInScopeForVolumes is runnerInScope for the volume loop; see the note
// there on why this no longer yields an identity.
func runnerInScopeForVolumes(runnerID string, runnerOrganizationID string, organizations map[string]struct{}, volumes map[string]*runnersv1.Volume) (bool, error) {
	orgID := strings.TrimSpace(runnerOrganizationID)
	if orgID != "" {
		_, ok := organizations[orgID]
		return ok, nil
	}
	if len(volumes) == 0 {
		return false, fmt.Errorf("runner %s organization id missing", runnerID)
	}
	return true, nil
}

func (r *Reconciler) reconcileVolumes(ctx context.Context) error {
	if r.agents == nil {
		return fmt.Errorf("agents client not configured")
	}
	organizations, err := r.agentOrganizations(ctx)
	if err != nil {
		return err
	}
	tracked, ignoredVolumeKeysByRunner, err := r.listActiveVolumes(ctx, organizations)
	if err != nil {
		return err
	}
	runnerIDs := map[string]struct{}{}
	volumesByRunner := make(map[string]map[string]*runnersv1.Volume)
	runnerIdentities := map[string]string{}
	for _, volume := range tracked {
		runnerID := volume.GetRunnerId()
		if runnerID == "" {
			log.Printf("reconciler: warn: volume %s missing runner id", volume.GetMeta().GetId())
			continue
		}
		identityID := volumeIdentityID(volume)
		if identityID == "" {
			return fmt.Errorf("volume %s missing owner identity", volume.GetMeta().GetId())
		}
		volumeID := volume.GetMeta().GetId()
		if volumeID == "" {
			log.Printf("reconciler: warn: volume missing id")
			continue
		}
		runnerIDs[runnerID] = struct{}{}
		if volumesByRunner[runnerID] == nil {
			volumesByRunner[runnerID] = map[string]*runnersv1.Volume{}
		}
		volumesByRunner[runnerID][volumeID] = volume
		if _, ok := runnerIdentities[runnerID]; !ok {
			runnerIdentities[runnerID] = identityID
		}
	}
	runners, err := r.listRunnersByOrg(ctx, organizations)
	if err != nil {
		return err
	}
	enrolledRunnerIDs := map[string]struct{}{}
	for _, runner := range runners {
		if runner == nil {
			continue
		}
		runnerID := runner.GetMeta().GetId()
		if runnerID == "" {
			continue
		}
		if runner.GetStatus() != runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED {
			continue
		}
		enrolledRunnerIDs[runnerID] = struct{}{}
		if _, ok := runnerIdentities[runnerID]; ok {
			runnerIDs[runnerID] = struct{}{}
			continue
		}
		if runner.GetOrganizationId() == "" && len(volumesByRunner[runnerID]) == 0 {
			continue
		}
		inScope, err := runnerInScopeForVolumes(runnerID, runner.GetOrganizationId(), organizations, volumesByRunner[runnerID])
		if err != nil {
			return err
		}
		if !inScope {
			continue
		}
		runnerIDs[runnerID] = struct{}{}
	}

	volumeInfoCache := map[string]volumeTTLInfo{}
	instanceCache := map[string]instanceActivity{}
	for runnerID := range runnerIDs {
		trackedVolumes := volumesByRunner[runnerID]
		if _, ok := enrolledRunnerIDs[runnerID]; !ok {
			for volumeID, volume := range trackedVolumes {
				if err := r.closeMissingVolume(ctx, volume); err != nil {
					log.Printf("reconciler: warn: close missing volume %s on unenrolled runner: %v", volumeID, err)
				}
				// A sandbox has no instance to pause; its own reconciler owns
				// what happens when the runner goes away.
				if isSandboxVolume(volume) {
					continue
				}
				r.pauseInstance(ctx, volumeIdentityID(volume), pauseReasonRunnerDeprovisioned)
			}
			continue
		}
		runnerClient, err := r.runnerDialer.Dial(ctx, runnerID)
		if err != nil {
			if runnerdial.IsNoTerminators(err) {
				for volumeID, volume := range trackedVolumes {
					if err := r.handleMissingRunnerVolume(ctx, volume); err != nil {
						log.Printf("reconciler: warn: handle missing volume %s after runner dial failure: %v", volumeID, err)
					}
				}
				continue
			}
			log.Printf("reconciler: warn: dial runner %s for volume reconciliation: %v", runnerID, err)
			continue
		}
		resp, err := runnerClient.ListVolumes(ctx, &runnerv1.ListVolumesRequest{})
		if err != nil {
			if runnerdial.IsNoTerminators(err) {
				for volumeID, volume := range trackedVolumes {
					if err := r.handleMissingRunnerVolume(ctx, volume); err != nil {
						log.Printf("reconciler: warn: handle missing volume %s after runner list failure: %v", volumeID, err)
					}
				}
				continue
			}
			log.Printf("reconciler: warn: list volumes for runner %s: %v", runnerID, err)
			continue
		}
		runnerVolumes := make(map[string]*runnerv1.VolumeListItem)
		for _, item := range resp.GetVolumes() {
			if item == nil {
				continue
			}
			volumeKey := item.GetVolumeKey()
			if volumeKey == "" {
				log.Printf("reconciler: warn: runner %s volume missing volume_key", runnerID)
				continue
			}
			if _, ok := runnerVolumes[volumeKey]; ok {
				log.Printf("reconciler: warn: runner %s volume_key %s duplicated", runnerID, volumeKey)
				continue
			}
			runnerVolumes[volumeKey] = item
		}
		for volumeID := range ignoredVolumeKeysByRunner[runnerID] {
			delete(runnerVolumes, volumeID)
		}

		for volumeID, volume := range trackedVolumes {
			item, ok := runnerVolumes[volumeID]
			if !ok {
				if err := r.closeMissingVolume(ctx, volume); err != nil {
					log.Printf("reconciler: warn: close missing volume %s: %v", volumeID, err)
				}
				if volume.GetStatus() == runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE {
					if isSandboxVolume(volume) {
						// No agent instance stands behind a sandbox: a lost workspace
						// PVC fails the sandbox itself.
						if err := r.markSandboxFailed(ctx, volume.GetOwnerId()); err != nil {
							log.Printf("reconciler: warn: fail sandbox %s after lost workspace volume %s: %v", volume.GetOwnerId(), volumeID, err)
						}
					} else {
						r.pauseInstance(ctx, volumeIdentityID(volume), pauseReasonVolumeLost)
					}
				}
				continue
			}
			delete(runnerVolumes, volumeID)
			if err := r.handlePresentRunnerVolume(ctx, runnerClient, volume, item, volumeInfoCache, instanceCache); err != nil {
				log.Printf("reconciler: warn: handle volume %s on runner %s: %v", volumeID, runnerID, err)
			}
		}

		for _, item := range runnerVolumes {
			instanceID := item.GetInstanceId()
			if instanceID == "" {
				log.Printf("reconciler: warn: runner %s orphan volume missing instance id", runnerID)
				continue
			}
			if err := r.removeRunnerVolume(ctx, runnerClient, instanceID); err != nil {
				log.Printf("reconciler: warn: remove orphan volume %s on runner %s: %v", instanceID, runnerID, err)
			}
		}
	}
	return nil
}

func (r *Reconciler) listActiveVolumes(ctx context.Context, organizations map[string]struct{}) ([]*runnersv1.Volume, map[string]map[string]struct{}, error) {
	active := []*runnersv1.Volume{}
	ignoredVolumeKeysByRunner := map[string]map[string]struct{}{}
	if len(organizations) == 0 {
		return active, ignoredVolumeKeysByRunner, nil
	}
	pageToken := ""
	statuses := []runnersv1.VolumeStatus{
		runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING,
		runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE,
		runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING,
	}
	for {
		resp, err := r.runners.ListVolumes(ctx, &runnersv1.ListVolumesRequest{
			PageSize:  activeVolumePageSize,
			PageToken: pageToken,
			Filter: &runnersv1.ListVolumesFilter{
				StatusIn: statuses,
			},
		})
		if err != nil {
			return nil, nil, fmt.Errorf("list volumes: %w", err)
		}
		for _, volume := range resp.GetVolumes() {
			if volume == nil {
				return nil, nil, fmt.Errorf("volume is nil")
			}
			meta := volume.GetMeta()
			if meta == nil {
				return nil, nil, fmt.Errorf("volume meta missing")
			}
			if meta.GetId() == "" {
				return nil, nil, fmt.Errorf("volume meta id missing")
			}
			orgID := strings.TrimSpace(volume.GetOrganizationId())
			if orgID == "" {
				return nil, nil, fmt.Errorf("volume %s organization id missing", meta.GetId())
			}
			parsedOrgID, err := uuidutil.ParseUUID(orgID, "volume.organization_id")
			if err != nil {
				return nil, nil, err
			}
			if _, ok := organizations[parsedOrgID.String()]; !ok {
				continue
			}
			active = append(active, volume)
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return active, ignoredVolumeKeysByRunner, nil
}

// closeMissingVolume finalizes a record whose disk the runner authoritatively
// does not have. Provisioning is left alone: the record is written before the
// disk exists.
func (r *Reconciler) closeMissingVolume(ctx context.Context, volume *runnersv1.Volume) error {
	volumeID := volume.GetMeta().GetId()
	if volumeID == "" {
		return nil
	}
	switch volume.GetStatus() {
	case runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE,
		runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING:
		status := runnersv1.VolumeStatus_VOLUME_STATUS_DELETED
		_, err := r.runners.UpdateVolume(ctx, &runnersv1.UpdateVolumeRequest{
			Id:        volumeID,
			Status:    &status,
			RemovedAt: timestamppb.New(time.Now().UTC()),
		})
		return err
	default:
		return nil
	}
}

func (r *Reconciler) handleMissingRunnerVolume(ctx context.Context, volume *runnersv1.Volume) error {
	volumeID := volume.GetMeta().GetId()
	if volumeID == "" {
		return nil
	}
	switch volume.GetStatus() {
	case runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING:
		return nil
	case runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE:
		return nil
	case runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING:
		status := runnersv1.VolumeStatus_VOLUME_STATUS_DELETED
		_, err := r.runners.UpdateVolume(ctx, &runnersv1.UpdateVolumeRequest{
			Id:        volumeID,
			Status:    &status,
			RemovedAt: timestamppb.New(time.Now().UTC()),
		})
		return err
	default:
		return nil
	}
}

func (r *Reconciler) handlePresentRunnerVolume(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, volume *runnersv1.Volume, item *runnerv1.VolumeListItem, volumeInfoCache map[string]volumeTTLInfo, instanceCache map[string]instanceActivity) error {
	volumeID := volume.GetMeta().GetId()
	if volumeID == "" {
		return nil
	}
	instanceID := item.GetInstanceId()
	if instanceID == "" {
		return nil
	}
	switch volume.GetStatus() {
	case runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING:
		status := runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE
		_, err := r.runners.UpdateVolume(ctx, &runnersv1.UpdateVolumeRequest{
			Id:         volumeID,
			Status:     &status,
			InstanceId: stringPtr(instanceID),
		})
		return err
	case runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE:
		if volume.GetInstanceId() != instanceID {
			if _, err := r.runners.UpdateVolume(ctx, &runnersv1.UpdateVolumeRequest{
				Id:         volumeID,
				InstanceId: stringPtr(instanceID),
			}); err != nil {
				return err
			}
		}
		if isSandboxVolume(volume) {
			// The workspace volume lives and dies with its sandbox: it must survive
			// idle stops and reconnects, so it has no independent TTL.
			return nil
		}
		expired, err := r.volumeTTLExpired(ctx, volume, volumeInfoCache, instanceCache)
		if err != nil {
			return err
		}
		if !expired {
			return nil
		}
		status := runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING
		if _, err := r.runners.UpdateVolume(ctx, &runnersv1.UpdateVolumeRequest{Id: volumeID, Status: &status}); err != nil {
			return err
		}
		return r.removeRunnerVolume(ctx, runnerClient, instanceID)
	case runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING:
		return r.removeRunnerVolume(ctx, runnerClient, instanceID)
	default:
		return nil
	}
}

func (r *Reconciler) removeRunnerVolume(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, instanceID string) error {
	_, err := runnerClient.RemoveVolume(ctx, &runnerv1.RemoveVolumeRequest{
		VolumeName: instanceID,
		Force:      true,
	})
	return err
}

func (r *Reconciler) volumeTTLExpired(ctx context.Context, volume *runnersv1.Volume, volumeInfoCache map[string]volumeTTLInfo, instanceCache map[string]instanceActivity) (bool, error) {
	volumeID := volume.GetVolumeId()
	if volumeID == "" {
		return false, fmt.Errorf("volume %s missing volume_id", volume.GetMeta().GetId())
	}
	info, err := r.volumeTTLInfo(ctx, volumeID, volumeInfoCache)
	if err != nil {
		return false, err
	}
	if !info.persistent || info.ttl == nil {
		return false, nil
	}
	agentInstanceID := volumeIdentityID(volume)
	if agentInstanceID == "" {
		return false, fmt.Errorf("volume %s missing agent_instance_id", volume.GetMeta().GetId())
	}
	activity, err := r.agentInstanceActivity(ctx, agentInstanceID, instanceCache)
	if err != nil {
		return false, err
	}
	if activity.hasActive || activity.latestRemovedAt == nil {
		return false, nil
	}
	if time.Since(*activity.latestRemovedAt) < *info.ttl {
		return false, nil
	}
	return true, nil
}

func (r *Reconciler) volumeTTLInfo(ctx context.Context, volumeID string, cache map[string]volumeTTLInfo) (volumeTTLInfo, error) {
	if cached, ok := cache[volumeID]; ok {
		return cached, nil
	}
	resp, err := r.agents.GetVolume(ctx, &agentsv1.GetVolumeRequest{Id: volumeID})
	if err != nil {
		return volumeTTLInfo{}, fmt.Errorf("get volume %s: %w", volumeID, err)
	}
	volume := resp.GetVolume()
	if volume == nil {
		return volumeTTLInfo{}, fmt.Errorf("volume %s missing", volumeID)
	}
	info := volumeTTLInfo{persistent: volume.GetPersistent()}
	if ttl := volume.GetTtl(); ttl != "" {
		parsed, err := parseVolumeTTL(ttl)
		if err != nil {
			return volumeTTLInfo{}, err
		}
		info.ttl = &parsed
	}
	cache[volumeID] = info
	return info, nil
}

func (r *Reconciler) agentInstanceActivity(ctx context.Context, agentInstanceID string, cache map[string]instanceActivity) (instanceActivity, error) {
	if cached, ok := cache[agentInstanceID]; ok {
		return cached, nil
	}
	workloads, err := r.listWorkloadsByAgentInstance(ctx, agentInstanceID, nil, 0)
	if err != nil {
		return instanceActivity{}, err
	}
	activity := instanceActivity{}
	for _, workload := range workloads {
		// Billing can end before physical removal. Neither removed_at nor an
		// older confirmed workload may expire a disk still held by another.
		if workload.GetRemovalConfirmedAt() == nil {
			activity.hasActive = true
			continue
		}
		switch workload.GetStatus() {
		case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
			runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
			runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING:
			activity.hasActive = true
		}
		switch workload.GetStatus() {
		case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
			runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED:
		default:
			continue
		}
		removedTime := workload.GetRemovalConfirmedAt().AsTime().UTC()
		if activity.latestRemovedAt == nil || removedTime.After(*activity.latestRemovedAt) {
			copy := removedTime
			activity.latestRemovedAt = &copy
		}
	}
	cache[agentInstanceID] = activity
	return activity, nil
}

func parseVolumeTTL(value string) (time.Duration, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, fmt.Errorf("ttl is empty")
	}
	parsed, err := time.ParseDuration(trimmed)
	if err == nil {
		if parsed <= 0 {
			return 0, fmt.Errorf("ttl must be greater than 0")
		}
		return parsed, nil
	}
	if !strings.HasSuffix(trimmed, "d") {
		return 0, fmt.Errorf("parse ttl %q: %w", value, err)
	}
	dayValue := strings.TrimSuffix(trimmed, "d")
	floatValue, parseErr := strconv.ParseFloat(dayValue, 64)
	if parseErr != nil {
		return 0, fmt.Errorf("parse ttl %q: %w", value, parseErr)
	}
	if floatValue <= 0 {
		return 0, fmt.Errorf("ttl must be greater than 0")
	}
	return time.Duration(floatValue * float64(24*time.Hour)), nil
}
