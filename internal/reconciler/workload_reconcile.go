package reconciler

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// runnerInScope reports whether this reconciler owns the runner: its
// organization must be one that has agents.
//
// This used to return an identity to impersonate on calls about the runner, and
// its stricter failures were artifacts of needing exactly one -- a runner
// carrying workloads for two owners had no single identity to borrow and was
// rejected outright. Nothing needs one now, so the question is only whether the
// runner is in scope. A runner reporting no organization is judged by the
// workloads already tracked on it, which were filtered to those organizations
// before they reached here.
func runnerInScope(runnerID string, runnerOrganizationID string, organizations map[string]struct{}, workloads map[string]*runnersv1.Workload) (bool, error) {
	orgID := strings.TrimSpace(runnerOrganizationID)
	if orgID != "" {
		_, ok := organizations[orgID]
		return ok, nil
	}
	if len(workloads) == 0 {
		return false, fmt.Errorf("runner %s organization id missing", runnerID)
	}
	return true, nil
}

func (r *Reconciler) reconcileWorkloads(ctx context.Context) error {
	organizations, err := r.agentOrganizations(ctx)
	if err != nil {
		return err
	}
	tracked, err := r.listActiveWorkloads(ctx, organizations)
	if err != nil {
		return err
	}
	// Sandboxes are reconciled here too -- nothing else moves a workload out of
	// running, so a sandbox whose pod was gone stayed active forever. They are
	// listed separately because the agent listing feeds callers that would read
	// a sandbox as an orphan.
	sandboxes, err := r.listActiveSandboxWorkloads(ctx)
	if err != nil {
		return err
	}
	tracked = append(tracked, sandboxes...)
	runnerIDs := map[string]struct{}{}
	workloadsByRunner := make(map[string]map[string]*runnersv1.Workload)
	runnerIdentities := map[string]string{}
	for _, workload := range tracked {
		runnerID := workload.GetRunnerId()
		if runnerID == "" {
			log.Printf("reconciler: warn: workload %s missing runner id", workload.GetMeta().GetId())
			continue
		}
		identityID := workloadRunnerIdentityID(workload)
		if identityID == "" {
			return fmt.Errorf("workload %s missing owner identity", workload.GetMeta().GetId())
		}
		workloadID := workload.GetMeta().GetId()
		if workloadID == "" {
			log.Printf("reconciler: warn: workload missing id")
			continue
		}
		runnerIDs[runnerID] = struct{}{}
		if workloadsByRunner[runnerID] == nil {
			workloadsByRunner[runnerID] = map[string]*runnersv1.Workload{}
		}
		workloadsByRunner[runnerID][workloadID] = workload
		if _, ok := runnerIdentities[runnerID]; !ok {
			runnerIdentities[runnerID] = identityID
		}
	}
	// The runners the tracked workloads actually name, not only those belonging
	// to an organization that owns an agent. listRunnersByOrg is keyed by the
	// organizations agents run in and returns nothing when there are none, so a
	// platform running only sandboxes never learned its runner existed -- and
	// every sandbox was condemned as missing from a runner nothing had asked.
	// A cluster-scoped runner has no organization to be listed under at all.
	runners, err := r.runnersForWorkloads(ctx, organizations, workloadsByRunner)
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
		if _, ok := runnerIDs[runnerID]; ok {
			continue
		}
		if runner.GetOrganizationId() == "" && len(workloadsByRunner[runnerID]) == 0 {
			continue
		}
		inScope, err := runnerInScope(runnerID, runner.GetOrganizationId(), organizations, workloadsByRunner[runnerID])
		if err != nil {
			return err
		}
		if !inScope {
			continue
		}
		runnerIDs[runnerID] = struct{}{}
	}

	for runnerID := range runnerIDs {
		trackedWorkloads := workloadsByRunner[runnerID]
		if _, ok := enrolledRunnerIDs[runnerID]; !ok {
			for workloadID, workload := range trackedWorkloads {
				log.Printf("reconciler: workload %s on unenrolled runner %s requires removal confirmation", workloadID, runnerID)
				if instanceID := strings.TrimSpace(workloadAgentInstanceID(workload)); instanceID != "" {
					r.pauseInstance(ctx, instanceID, pauseReasonRunnerDeprovisioned)
				}
			}
			continue
		}
		runnerClient, err := r.runnerDialer.Dial(ctx, runnerID)
		if err != nil {
			log.Printf("reconciler: warn: dial runner %s for workload reconciliation: %v", runnerID, err)
			continue
		}
		resp, err := runnerClient.ListWorkloads(ctx, &runnerv1.ListWorkloadsRequest{})
		if err != nil {
			log.Printf("reconciler: warn: list workloads for runner %s: %v", runnerID, err)
			continue
		}
		runnerWorkloads := make(map[string]*runnerv1.WorkloadListItem)
		for _, item := range resp.GetWorkloads() {
			if item == nil {
				continue
			}
			workloadKey := item.GetWorkloadKey()
			if workloadKey == "" {
				log.Printf("reconciler: warn: runner %s workload missing workload_key", runnerID)
				continue
			}
			if _, ok := runnerWorkloads[workloadKey]; ok {
				log.Printf("reconciler: warn: runner %s workload_key %s duplicated", runnerID, workloadKey)
				continue
			}
			runnerWorkloads[workloadKey] = item
		}

		for workloadID, workload := range trackedWorkloads {
			item, ok := runnerWorkloads[workloadID]
			if !ok {
				// Says what the runner actually reported. Marking a workload lost
				// stops it and deletes its OpenZiti identity, so a wrong verdict
				// here kills a healthy workload -- and it was reached silently.
				reported := make([]string, 0, len(runnerWorkloads))
				for key := range runnerWorkloads {
					reported = append(reported, key)
				}
				sort.Strings(reported)
				log.Printf("reconciler: workload %s not among the %d the runner %s reported: %v",
					workloadID, len(reported), runnerID, reported)
				if err := r.handleMissingRunnerWorkload(ctx, runnerClient, workload); err != nil {
					log.Printf("reconciler: warn: handle missing workload %s: %v", workloadID, err)
				}
				continue
			}
			delete(runnerWorkloads, workloadID)
			if err := r.handlePresentRunnerWorkload(ctx, runnerClient, workload, item); err != nil {
				log.Printf("reconciler: warn: handle workload %s on runner %s: %v", workloadID, runnerID, err)
			}
		}

		for _, item := range runnerWorkloads {
			instanceID := normalizeRunnerWorkloadID(item.GetInstanceId())
			if instanceID == "" {
				log.Printf("reconciler: warn: runner %s orphan workload missing instance id", runnerID)
				continue
			}
			if err := r.stopRunnerWorkload(ctx, runnerClient, instanceID); err != nil {
				log.Printf("reconciler: warn: stop orphan workload %s on runner %s: %v", instanceID, runnerID, err)
			}
		}
	}
	return nil
}

const (
	startGracePeriod     = 60 * time.Second
	initRetryThreshold   = 3
	crashloopThreshold   = 3
	crashLoopBackoffFlag = "CrashLoopBackOff"
)

var imagePullReasons = map[string]struct{}{
	"ImagePullBackOff": {},
	"ErrImagePull":     {},
}

var configInvalidReasons = map[string]struct{}{
	"CreateContainerConfigError": {},
	"CreateContainerError":       {},
	"InvalidImageName":           {},
}

func (r *Reconciler) handleMissingRunnerWorkload(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, workload *runnersv1.Workload) error {
	workloadID := workload.GetMeta().GetId()
	if workloadID == "" {
		return fmt.Errorf("workload missing id")
	}
	// List filters and StopWorkload acknowledgements do not prove removal.
	// Check both requested and returned IDs (including legacy runner aliases).
	checked := map[string]struct{}{}
	for _, id := range []string{workload.GetInstanceId(), workloadID} {
		id = normalizeRunnerWorkloadID(id)
		if _, ok := checked[id]; ok || id == "" {
			continue
		}
		checked[id] = struct{}{}
		_, err := r.inspectRunnerWorkload(ctx, runnerClient, id)
		if err == nil {
			return fmt.Errorf("workload %s removal pending: runner still reports %s", workloadID, id)
		}
		if status.Code(err) != codes.NotFound {
			return fmt.Errorf("confirm workload %s removal: %w", workloadID, err)
		}
	}
	terminal := workload.GetStatus()
	req := &runnersv1.UpdateWorkloadRequest{Id: workloadID, RemovedAt: timestamppb.New(time.Now().UTC())}
	switch workload.GetStatus() {
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING:
		terminal = runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED
		reason := runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_RUNTIME_LOST
		req.FailureReason = &reason
		req.FailureMessage = stringPtr("workload missing on runner")
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING:
		terminal = runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED:
	default:
		return fmt.Errorf("workload %s has unspecified status", workloadID)
	}
	req.Status = &terminal
	if _, err := r.runners.UpdateWorkload(ctx, req); err != nil {
		return err
	}
	workload.Status = terminal
	workload.RemovedAt = req.RemovedAt
	r.revokePullCredential(ctx, workloadID)
	if r.zitiMgmt != nil && workload.GetZitiIdentityId() != "" {
		return r.deleteIdentity(ctx, workload.GetZitiIdentityId())
	}
	return nil
}

func (r *Reconciler) handlePresentRunnerWorkload(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, workload *runnersv1.Workload, item *runnerv1.WorkloadListItem) error {
	workloadID := workload.GetMeta().GetId()
	if workloadID == "" {
		return nil
	}
	instanceID := normalizeRunnerWorkloadID(item.GetInstanceId())
	if instanceID == "" {
		return nil
	}
	updateReq := &runnersv1.UpdateWorkloadRequest{Id: workloadID}
	shouldUpdate := false
	if workload.GetInstanceId() != instanceID {
		updateReq.InstanceId = stringPtr(instanceID)
		shouldUpdate = true
	}
	inspectResp, err := r.inspectRunnerWorkload(ctx, runnerClient, instanceID)
	inspectStateRunning := false
	inspectContainerCount := 0
	var containers []*runnersv1.Container
	if err != nil {
		log.Printf("reconciler: warn: inspect workload %s: %v", workloadID, err)
	} else {
		inspectStateRunning = inspectResp.GetStateRunning()
		inspectContainerCount = len(inspectResp.GetContainers())
		mapped, mapErr := mapRunnerContainers(inspectResp.GetContainers())
		if mapErr != nil {
			log.Printf("reconciler: warn: map workload %s containers: %v", workloadID, mapErr)
		} else if mapped != nil {
			containers = mapped
			updateReq.Containers = containers
			shouldUpdate = true
		}
	}
	switch workload.GetStatus() {
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING:
		if containers != nil {
			ready, failure, err := classifyStartingContainers(containers, workload, time.Now().UTC())
			if err != nil {
				return err
			}
			if failure != nil {
				r.failWorkloadOnRunner(ctx, runnerClient, workload, instanceID, failure, containers)
				return nil
			}
			if ready {
				status := runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING
				updateReq.Status = &status
				shouldUpdate = true
			}
		} else if inspectStateRunning && inspectContainerCount == 0 {
			// Some runner versions report state_running without containers; avoid a stuck STARTING state.
			status := runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING
			updateReq.Status = &status
			shouldUpdate = true
		}
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING:
		if containers != nil {
			failure, err := classifyRunningContainers(containers)
			if err != nil {
				return err
			}
			if failure != nil {
				r.failWorkloadOnRunner(ctx, runnerClient, workload, instanceID, failure, containers)
				return nil
			}
		}
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING:
		// stop below
	default:
	}
	if shouldUpdate {
		if _, err := r.runners.UpdateWorkload(ctx, updateReq); err != nil {
			return err
		}
		if updateReq.Status != nil {
			workload.Status = *updateReq.Status
		}
		if updateReq.InstanceId != nil {
			workload.InstanceId = updateReq.InstanceId
		}
	}
	if workload.GetStatus() == runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING || workload.GetStatus() == runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
		return r.stopWorkloadOnRunner(ctx, runnerClient, workload)
	}
	return nil
}

type workloadFailure struct {
	reason  runnersv1.WorkloadFailureReason
	message string
}

func classifyStartingContainers(containers []*runnersv1.Container, workload *runnersv1.Workload, now time.Time) (bool, *workloadFailure, error) {
	createdAt, err := workloadCreatedAt(workload)
	if err != nil {
		return false, nil, err
	}
	startAge := now.Sub(createdAt)
	mainRunning := true
	foundMain := false
	initBlocked := false
	for _, container := range containers {
		if container == nil {
			return false, nil, fmt.Errorf("workload %s has nil container", workload.GetMeta().GetId())
		}
		switch container.GetRole() {
		case runnersv1.ContainerRole_CONTAINER_ROLE_INIT:
			status := container.GetStatus()
			if isConfigInvalidFailure(container) {
				return false, &workloadFailure{reason: runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CONFIG_INVALID, message: containerFailureMessage(container)}, nil
			}
			if isImagePullFailure(container) && startAge > startGracePeriod {
				return false, &workloadFailure{reason: runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_IMAGE_PULL_FAILED, message: containerFailureMessage(container)}, nil
			}
			switch status {
			case runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING:
				initBlocked = true
			case runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED:
				if container.GetExitCode() != 0 {
					return false, &workloadFailure{reason: runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_START_FAILED, message: containerFailureMessage(container)}, nil
				}
			case runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING:
			default:
				initBlocked = true
			}
		case runnersv1.ContainerRole_CONTAINER_ROLE_MAIN:
			foundMain = true
			status := container.GetStatus()
			if status != runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING {
				mainRunning = false
			}
			if status == runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED {
				message := containerFailureMessage(container)
				exitCode := container.GetExitCode()
				if message == "" {
					message = fmt.Sprintf("main container terminated with exit code %d", exitCode)
				} else if exitCode != 0 {
					message = fmt.Sprintf("%s (exit code %d)", message, exitCode)
				}
				return false, &workloadFailure{reason: runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_START_FAILED, message: message}, nil
			}
			if status == runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING {
				if container.GetReason() == crashLoopBackoffFlag && container.GetRestartCount() >= crashloopThreshold {
					return false, &workloadFailure{reason: runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CRASHLOOP, message: containerFailureMessage(container)}, nil
				}
				if isImagePullFailure(container) && startAge > startGracePeriod {
					return false, &workloadFailure{reason: runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_IMAGE_PULL_FAILED, message: containerFailureMessage(container)}, nil
				}
				if isConfigInvalidFailure(container) && startAge > startGracePeriod {
					return false, &workloadFailure{reason: runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CONFIG_INVALID, message: containerFailureMessage(container)}, nil
				}
			}
		default:
		}
	}
	if !foundMain {
		return false, nil, fmt.Errorf("workload %s missing main container", workload.GetMeta().GetId())
	}
	if !initBlocked && mainRunning {
		return true, nil, nil
	}
	return false, nil, nil
}

func classifyRunningContainers(containers []*runnersv1.Container) (*workloadFailure, error) {
	for _, container := range containers {
		if container == nil {
			return nil, fmt.Errorf("container is nil")
		}
		if container.GetRole() != runnersv1.ContainerRole_CONTAINER_ROLE_MAIN {
			continue
		}
		if container.GetStatus() == runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING && container.GetReason() == crashLoopBackoffFlag && container.GetRestartCount() >= crashloopThreshold {
			return &workloadFailure{reason: runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CRASHLOOP, message: containerFailureMessage(container)}, nil
		}
	}
	return nil, nil
}

func (r *Reconciler) failWorkloadOnRunner(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, workload *runnersv1.Workload, instanceID string, failure *workloadFailure, containers []*runnersv1.Container) {
	if failure == nil {
		return
	}
	workloadID := workload.GetMeta().GetId()
	if workloadID == "" {
		return
	}
	r.markWorkloadFailed(ctx, workloadID, stringPtr(instanceID), failure.reason, failure.message, containers)
	workload.Status = runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED
	workload.InstanceId = stringPtr(instanceID)
	if err := r.stopWorkloadOnRunner(ctx, runnerClient, workload); err != nil {
		log.Printf("reconciler: stop workload %s (instance %s) after failure: %v", workloadID, instanceID, err)
	}
}

func isImagePullFailure(container *runnersv1.Container) bool {
	_, ok := imagePullReasons[container.GetReason()]
	return ok
}

func isConfigInvalidFailure(container *runnersv1.Container) bool {
	_, ok := configInvalidReasons[container.GetReason()]
	return ok
}

func containerFailureMessage(container *runnersv1.Container) string {
	if container.GetMessage() != "" {
		return container.GetMessage()
	}
	return container.GetReason()
}

// runnersForWorkloads lists the runners of every organization that owns an
// agent, then adds any runner a tracked workload names that the listing missed.
func (r *Reconciler) runnersForWorkloads(ctx context.Context, organizations map[string]struct{}, workloadsByRunner map[string]map[string]*runnersv1.Workload) ([]*runnersv1.Runner, error) {
	runners, err := r.listRunnersByOrg(ctx, organizations)
	if err != nil {
		return nil, err
	}
	listed := map[string]struct{}{}
	for _, runner := range runners {
		if id := runner.GetMeta().GetId(); id != "" {
			listed[id] = struct{}{}
		}
	}
	for runnerID, workloads := range workloadsByRunner {
		if _, ok := listed[runnerID]; ok || len(workloads) == 0 {
			continue
		}
		identityID := ""
		for _, workload := range workloads {
			if identityID = workloadRunnerIdentityID(workload); identityID != "" {
				break
			}
		}
		if identityID == "" {
			continue
		}
		resp, err := r.runners.GetRunner(ctx, &runnersv1.GetRunnerRequest{Id: runnerID})
		if err != nil {
			// Reported rather than treated as absent: absent means every workload
			// on it is stopped and its identity deleted.
			log.Printf("reconciler: warn: get runner %s named by %d tracked workloads: %v", runnerID, len(workloads), err)
			continue
		}
		if runner := resp.GetRunner(); runner != nil {
			runners = append(runners, runner)
			listed[runnerID] = struct{}{}
		}
	}
	return runners, nil
}
