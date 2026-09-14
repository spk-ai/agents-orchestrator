package reconciler

import (
	"context"
	"fmt"
	"log"
	"time"

	meteringv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/metering/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/agynio/agents-orchestrator/internal/runnerdial"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	reconcileTimeout        = 30 * time.Second
	volumeReconcileInterval = time.Minute
)

type Reconciler struct {
	threads                   threadsv1.ThreadsServiceClient
	agents                    agentsClient
	runnerDialer              runnerdial.RunnerDialer
	runners                   runnersClient
	metering                  meteringv1.MeteringServiceClient
	meteringSampleInterval    time.Duration
	zitiMgmt                  zitimgmtv1.ZitiManagementServiceClient
	groups                    groupsClient
	assembler                 *assembler.Assembler
	wake                      <-chan struct{}
	sandboxWake               <-chan struct{}
	poll                      time.Duration
	workloadReconcileInterval time.Duration
	idle                      time.Duration
	stopSec                   uint32
	stopInactiveInstances     bool
	// Optional: without them nothing is minted and the spec keeps whatever
	// pull credentials it already carried.
	imageProxy     ImageProxyClient
	imageProxyHost string
	// The identity this process acts as, for the callees that require a caller
	// rather than serving an absent one as the platform.
	platformIdentityID uuid.UUID
}

// WithImageProxy enables the per-workload pull credential lifecycle.
func (r *Reconciler) WithImageProxy(proxy ImageProxyClient, host string) *Reconciler {
	r.imageProxy = proxy
	r.imageProxyHost = host
	return r
}

type Config struct {
	Threads                   threadsv1.ThreadsServiceClient
	Agents                    agentsClient
	RunnerDialer              runnerdial.RunnerDialer
	Runners                   runnersClient
	Metering                  meteringv1.MeteringServiceClient
	ZitiMgmt                  zitimgmtv1.ZitiManagementServiceClient
	Groups                    groupsClient
	Assembler                 *assembler.Assembler
	Wake                      <-chan struct{}
	SandboxWake               <-chan struct{}
	Poll                      time.Duration
	WorkloadReconcileInterval time.Duration
	Idle                      time.Duration
	StopSec                   uint32
	StopInactiveInstances     bool
	MeteringSampleInterval    time.Duration
	PlatformIdentityID        uuid.UUID
}

func New(cfg Config) *Reconciler {
	return &Reconciler{
		threads:                   cfg.Threads,
		agents:                    cfg.Agents,
		runnerDialer:              cfg.RunnerDialer,
		runners:                   cfg.Runners,
		metering:                  cfg.Metering,
		meteringSampleInterval:    cfg.MeteringSampleInterval,
		zitiMgmt:                  cfg.ZitiMgmt,
		groups:                    cfg.Groups,
		assembler:                 cfg.Assembler,
		wake:                      cfg.Wake,
		sandboxWake:               cfg.SandboxWake,
		poll:                      cfg.Poll,
		workloadReconcileInterval: cfg.WorkloadReconcileInterval,
		idle:                      cfg.Idle,
		stopSec:                   cfg.StopSec,
		stopInactiveInstances:     cfg.StopInactiveInstances,
		platformIdentityID:        cfg.PlatformIdentityID,
	}
}

func (r *Reconciler) Run(ctx context.Context) error {
	if r.metering == nil {
		return fmt.Errorf("metering client not configured")
	}
	if r.meteringSampleInterval <= 0 {
		return fmt.Errorf("metering sample interval must be greater than 0")
	}
	go r.runWorkloadReconcileLoop(ctx)
	go r.runSandboxReconcileLoop(ctx)
	go r.runVolumeReconcileLoop(ctx)
	go r.runMeteringSampleLoop(ctx)

	ticker := time.NewTicker(r.poll)
	defer ticker.Stop()

	r.runCycle(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.wake:
			r.runCycle(ctx)
		case <-ticker.C:
			r.runCycle(ctx)
		}
	}
}

func (r *Reconciler) runCycle(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()
	if err := r.reconcile(rctx); err != nil {
		log.Printf("reconciler: cycle failed: %v", err)
	}
}

func (r *Reconciler) reconcile(ctx context.Context) error {
	desired, idleTimeouts, agentUpdatedAt, err := r.fetchDesired(ctx)
	if err != nil {
		return err
	}
	actual, err := r.fetchActual(ctx)
	if err != nil {
		return err
	}
	// The agents behind running workloads, not just the ones with work waiting.
	// An idle timeout only decides anything once an agent has gone idle, and by
	// then it has dropped out of the desired set -- so reading the timeout from
	// the desired set alone meant a workload was stopped on the platform
	// fallback the moment it finished answering, whatever the agent asked for.
	if err := r.addIdleTimeoutsForWorkloads(ctx, actual, idleTimeouts); err != nil {
		return err
	}
	stopRequests, err := r.inactiveInstanceStopRequests(ctx, desired, actual)
	if err != nil {
		return err
	}
	actions, err := ComputeActions(desired, actual, stopRequests, idleTimeouts, r.idle, time.Now().UTC())
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	started := 0
	for _, candidate := range actions.ToStart {
		ok, err := r.shouldStartWorkload(ctx, candidate, now, agentUpdatedAt)
		if err != nil {
			log.Printf("reconciler: start decision for agent %s instance %s: %v", candidate.AgentID.String(), candidate.AgentInstanceID.String(), err)
			continue
		}
		if !ok {
			continue
		}
		r.startWorkload(ctx, candidate)
		started++
	}
	for _, workload := range actions.ToStop {
		r.stopWorkload(ctx, workload)
	}
	if r.zitiMgmt != nil {
		if err := r.reconcileOrphanIdentities(ctx); err != nil {
			return err
		}
	}
	if r.zitiMgmt != nil && r.groups != nil {
		if err := r.ReconcileAllAgentGroupRoles(ctx); err != nil {
			return err
		}
	}
	log.Printf(
		"reconciler: cycle complete - desired=%d actual=%d started=%d stopped=%d",
		len(desired),
		len(actual),
		started,
		len(actions.ToStop),
	)
	return nil
}

type identityInfo struct {
	id            string
	enrollmentJWT string
}

func (i *identityInfo) idPtr() *string {
	if i == nil {
		return nil
	}
	return &i.id
}

func (r *Reconciler) createIdentity(ctx context.Context, target AgentInstanceTarget, workloadID uuid.UUID, organizationID string, environmentID string, llmRoleAttributes []string) (*identityInfo, error) {
	if r.zitiMgmt == nil {
		return nil, nil
	}
	roleAttributes, err := r.agentGroupRoleAttributes(ctx, target.AgentID, organizationID)
	if err != nil {
		return nil, fmt.Errorf("list groups for agent %s instance %s: %w", target.AgentID.String(), target.AgentInstanceID.String(), err)
	}
	// The llm-native-<vendor> attributes are the only thing that opts a workload
	// into vendor interception; nothing is provisioned per workload.
	roleAttributes = append(roleAttributes, llmRoleAttributes...)
	// AgentId is the instance -- that is what the identity resolves to.
	// AgentClassId and EnvironmentId are recorded alongside it.
	identityResp, err := r.zitiMgmt.CreateAgentIdentity(ctx, &zitimgmtv1.CreateAgentIdentityRequest{
		AgentId:                  target.AgentInstanceID.String(),
		WorkloadId:               workloadID.String(),
		AdditionalRoleAttributes: roleAttributes,
		AgentClassId:             target.AgentID.String(),
		EnvironmentId:            environmentID,
	})
	if err != nil {
		return nil, fmt.Errorf("create ziti identity for agent %s instance %s: %w", target.AgentID.String(), target.AgentInstanceID.String(), err)
	}
	identityID := identityResp.GetZitiIdentityId()
	enrollmentJWT := identityResp.GetEnrollmentJwt()
	if identityID == "" || enrollmentJWT == "" {
		var identityPtr *string
		if identityID != "" {
			identityPtr = &identityID
		}
		r.compensateIdentity(ctx, identityPtr, "missing identity fields")
		return nil, fmt.Errorf("ziti identity response missing fields for agent %s instance %s", target.AgentID.String(), target.AgentInstanceID.String())
	}
	return &identityInfo{id: identityID, enrollmentJWT: enrollmentJWT}, nil
}

func (r *Reconciler) compensateIdentity(ctx context.Context, zitiIdentityID *string, reason string) {
	if zitiIdentityID == nil {
		return
	}
	if err := r.deleteIdentity(ctx, *zitiIdentityID); err != nil {
		log.Printf("reconciler: delete ziti identity %s after %s: %v", *zitiIdentityID, reason, err)
	}
}

func (r *Reconciler) startWorkload(ctx context.Context, target AgentInstanceTarget) {
	assembled, err := r.assembler.Assemble(ctx, target.AgentID, target.AgentInstanceID, target.ThreadID)
	if err != nil {
		log.Printf("reconciler: assemble workload for agent %s instance %s: %v", target.AgentID.String(), target.AgentInstanceID.String(), err)
		return
	}
	pinnedRunnerID, err := r.pinnedRunnerForAgentInstance(ctx, target.AgentInstanceID.String())
	if err != nil {
		log.Printf("reconciler: list volumes for agent %s instance %s: %v", target.AgentID.String(), target.AgentInstanceID.String(), err)
		return
	}
	var selectedRunner *runnersv1.Runner
	if pinnedRunnerID != "" {
		runner, enrolled, err := r.getRunnerIfEnrolled(ctx, pinnedRunnerID)
		if err != nil {
			log.Printf("reconciler: get runner %s for agent %s instance %s: %v", pinnedRunnerID, target.AgentID.String(), target.AgentInstanceID.String(), err)
			return
		}
		if !enrolled {
			r.pauseInstance(ctx, target.AgentInstanceID.String(), pauseReasonRunnerDeprovisioned)
			return
		}
		selectedRunner = runner
	} else if assembled.RunnerID != "" {
		// An agent with an environment is placed on the environment's runner
		// instead of by labels and capabilities. A thread that already has
		// volumes keeps its pin above: the agent's state physically lives on
		// that runner and cannot be moved by picking a different one.
		runner, enrolled, err := r.getRunnerIfEnrolled(ctx, assembled.RunnerID)
		if err != nil {
			log.Printf("reconciler: get environment runner %s for agent %s thread %s: %v", assembled.RunnerID, target.AgentID.String(), target.ThreadID.String(), err)
			return
		}
		if !enrolled {
			log.Printf("reconciler: environment runner %s is not enrolled for agent %s thread %s", assembled.RunnerID, target.AgentID.String(), target.ThreadID.String())
			return
		}
		selectedRunner = runner
	} else {
		selectedRunner, err = r.selectRunner(ctx, assembled.OrganizationID, assembled.RunnerLabels, assembled.Request.GetCapabilities())
		if err != nil {
			log.Printf("reconciler: select runner for agent %s instance %s: %v", target.AgentID.String(), target.AgentInstanceID.String(), err)
			return
		}
	}
	runnerID := selectedRunner.GetMeta().GetId()
	if runnerID == "" {
		log.Printf("reconciler: runner missing id for agent %s instance %s", target.AgentID.String(), target.AgentInstanceID.String())
		return
	}
	runnerClient, err := r.runnerDialer.Dial(ctx, runnerID)
	if err != nil {
		log.Printf("reconciler: dial runner %s for agent %s instance %s: %v", runnerID, target.AgentID.String(), target.AgentInstanceID.String(), err)
		return
	}
	request := assembled.Request
	workloadID := uuid.New()
	workloadIDValue := workloadID.String()
	if request.AdditionalProperties == nil {
		request.AdditionalProperties = map[string]string{}
	}
	request.AdditionalProperties[assembler.LabelKeyPrefix+assembler.LabelWorkloadKey] = workloadIDValue
	volumeRecords, err := buildVolumeRecords(assembled.PersistentVolumes)
	if err != nil {
		log.Printf("reconciler: build volume records for agent %s instance %s: %v", target.AgentID.String(), target.AgentInstanceID.String(), err)
		return
	}
	request.WorkloadId = workloadIDValue
	request.Main.Env = append(request.Main.Env, &runnerv1.EnvVar{Name: "WORKLOAD_ID", Value: workloadIDValue})
	// The credential is scoped to this workload and the images it may pull, so
	// it can only be minted once the workload has an id.
	if credentials, err := r.mintPullCredential(ctx, workloadIDValue, assembled); err != nil {
		log.Printf("reconciler: %v", err)
		return
	} else if len(credentials) > 0 {
		request.ImagePullCredentials = credentials
	}
	identity, err := r.createIdentity(ctx, target, workloadID, assembled.OrganizationID, assembled.EnvironmentID, assembled.LLMRoleAttributes)
	if err != nil {
		log.Printf("reconciler: %v", err)
		return
	}
	zitiIdentityID := identity.idPtr()
	if identity != nil {
		if err := attachZitiEnrollmentToken(request, identity.enrollmentJWT); err != nil {
			log.Printf("reconciler: set ziti enrollment jwt for agent %s instance %s: %v", target.AgentID.String(), target.AgentInstanceID.String(), err)
			r.compensateIdentity(ctx, zitiIdentityID, "missing ziti enroll container")
			return
		}
	}
	createdVolumes, err := r.createVolumeRecords(ctx, volumeRecords, runnerID, target, assembled.OrganizationID)
	if err != nil {
		log.Printf("reconciler: create volume records for agent %s instance %s: %v", target.AgentID.String(), target.AgentInstanceID.String(), err)
		r.markVolumeRecordsFailed(ctx, createdVolumes)
		r.compensateIdentity(ctx, zitiIdentityID, "volume record failure")
		return
	}
	metadata := agentWorkloadMetadata(workloadIDValue, runnerID, target, assembled, zitiIdentityID)
	if _, err := r.startPreparedWorkload(ctx, runnerClient, metadata, request, assembled.PersistentVolumes, createdVolumes); err != nil {
		log.Printf("reconciler: prepared start for agent %s instance %s: %v", target.AgentID.String(), target.AgentInstanceID.String(), err)
	}
}

func (r *Reconciler) stopWorkload(ctx context.Context, workload *runnersv1.Workload) {
	workloadID := workload.GetMeta().GetId()
	if workloadID == "" {
		log.Printf("reconciler: workload missing id")
		return
	}
	if err := r.stopWorkloadWithContext(ctx, workload); err != nil {
		log.Printf("reconciler: stop workload %s: %v", workload.GetMeta().GetId(), err)
	}
}

func (r *Reconciler) stopWorkloadWithContext(ctx context.Context, workload *runnersv1.Workload) error {
	workloadID := workload.GetMeta().GetId()
	if workloadID == "" {
		return fmt.Errorf("workload missing id")
	}
	runnerID := workload.GetRunnerId()
	if runnerID == "" {
		return fmt.Errorf("workload %s missing runner id", workloadID)
	}
	runnerClient, err := r.runnerDialer.Dial(ctx, runnerID)
	if err != nil {
		return fmt.Errorf("dial runner %s for workload %s: %w", runnerID, workloadID, err)
	}
	return r.stopWorkloadOnRunner(ctx, runnerClient, workload)
}

func (r *Reconciler) stopWorkloadOnRunner(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, workload *runnersv1.Workload) error {
	if workload.GetPreparation() != nil {
		return r.stopPreparedWorkload(ctx, runnerClient, workload)
	}
	workloadID := workload.GetMeta().GetId()
	if workload.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED && workload.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING {
		stopping := runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING
		if _, err := r.runners.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{Id: workloadID, Status: &stopping}); err != nil {
			return fmt.Errorf("update workload %s to stopping: %w", workloadID, err)
		}
		workload.Status = stopping
	}
	instanceID := normalizeRunnerWorkloadID(workload.GetInstanceId())
	if instanceID == "" {
		// The requested ID is persisted before StartWorkload. Its reply can be
		// lost even though the runner created the workload.
		instanceID = workloadID
	}
	if err := r.stopRunnerWorkload(ctx, runnerClient, instanceID); err != nil {
		return fmt.Errorf("stop workload %s: %w", workloadID, err)
	}
	return r.handleMissingRunnerWorkload(ctx, runnerClient, workload)
}

func (r *Reconciler) stopRunnerWorkload(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, instanceID string) error {
	if err := r.stopRunnerWorkloadID(ctx, runnerClient, instanceID); err == nil {
		return nil
	} else if status.Code(err) != codes.NotFound {
		return err
	} else if _, parseErr := uuid.Parse(instanceID); parseErr != nil {
		return err
	}
	return r.stopRunnerWorkloadWithPrefix(ctx, runnerClient, instanceID)
}

func (r *Reconciler) stopRunnerWorkloadID(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, workloadID string) error {
	_, err := runnerClient.StopWorkload(ctx, &runnerv1.StopWorkloadRequest{
		WorkloadId: workloadID,
		TimeoutSec: r.stopSec,
	})
	return err
}

func (r *Reconciler) stopRunnerWorkloadWithPrefix(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, instanceID string) error {
	prefixedID := runnerWorkloadPrefix + instanceID
	if err := r.stopRunnerWorkloadID(ctx, runnerClient, prefixedID); err == nil {
		return nil
	} else if status.Code(err) != codes.NotFound {
		return err
	}
	return nil
}

func (r *Reconciler) inspectRunnerWorkload(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, instanceID string) (*runnerv1.InspectWorkloadResponse, error) {
	resp, err := r.inspectRunnerWorkloadID(ctx, runnerClient, instanceID)
	if err == nil {
		return resp, nil
	}
	if status.Code(err) != codes.NotFound {
		return nil, err
	}
	if _, parseErr := uuid.Parse(instanceID); parseErr != nil {
		return nil, err
	}
	return r.inspectRunnerWorkloadWithPrefix(ctx, runnerClient, instanceID)
}

func (r *Reconciler) inspectRunnerWorkloadID(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, workloadID string) (*runnerv1.InspectWorkloadResponse, error) {
	return runnerClient.InspectWorkload(ctx, &runnerv1.InspectWorkloadRequest{WorkloadId: workloadID})
}

func (r *Reconciler) inspectRunnerWorkloadWithPrefix(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, instanceID string) (*runnerv1.InspectWorkloadResponse, error) {
	prefixedID := runnerWorkloadPrefix + instanceID
	resp, err := r.inspectRunnerWorkloadID(ctx, runnerClient, prefixedID)
	if err == nil {
		return resp, nil
	}
	if status.Code(err) != codes.NotFound {
		return nil, err
	}
	return nil, err
}

func (r *Reconciler) deleteIdentity(ctx context.Context, identityID string) error {
	_, err := r.zitiMgmt.DeleteIdentity(ctx, &zitimgmtv1.DeleteIdentityRequest{ZitiIdentityId: identityID})
	return err
}

func runnerStatus(status runnerv1.WorkloadStatus) (runnersv1.WorkloadStatus, error) {
	switch status {
	case runnerv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED, fmt.Errorf("runner returned unspecified workload status")
	case runnerv1.WorkloadStatus_WORKLOAD_STATUS_STARTING:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING, nil
	case runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, nil
	case runnerv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED, nil
	case runnerv1.WorkloadStatus_WORKLOAD_STATUS_FAILED:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED, nil
	default:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED, fmt.Errorf("unknown runner workload status: %v", status)
	}
}

func runnerContainerRole(role runnerv1.ContainerRole) (runnersv1.ContainerRole, error) {
	switch role {
	case runnerv1.ContainerRole_CONTAINER_ROLE_UNSPECIFIED:
		return runnersv1.ContainerRole_CONTAINER_ROLE_UNSPECIFIED, fmt.Errorf("runner returned unspecified container role")
	case runnerv1.ContainerRole_CONTAINER_ROLE_MAIN:
		return runnersv1.ContainerRole_CONTAINER_ROLE_MAIN, nil
	case runnerv1.ContainerRole_CONTAINER_ROLE_SIDECAR:
		return runnersv1.ContainerRole_CONTAINER_ROLE_SIDECAR, nil
	case runnerv1.ContainerRole_CONTAINER_ROLE_INIT:
		return runnersv1.ContainerRole_CONTAINER_ROLE_INIT, nil
	default:
		return runnersv1.ContainerRole_CONTAINER_ROLE_UNSPECIFIED, fmt.Errorf("unknown runner container role: %v", role)
	}
}

func runnerContainerStatus(status runnerv1.ContainerStatus) (runnersv1.ContainerStatus, error) {
	switch status {
	case runnerv1.ContainerStatus_CONTAINER_STATUS_UNSPECIFIED:
		return runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING, nil
	case runnerv1.ContainerStatus_CONTAINER_STATUS_RUNNING:
		return runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING, nil
	case runnerv1.ContainerStatus_CONTAINER_STATUS_TERMINATED:
		return runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED, nil
	case runnerv1.ContainerStatus_CONTAINER_STATUS_WAITING:
		return runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING, nil
	default:
		return runnersv1.ContainerStatus_CONTAINER_STATUS_UNSPECIFIED, fmt.Errorf("unknown runner container status: %v", status)
	}
}

func mapRunnerContainers(containers []*runnerv1.WorkloadContainer) ([]*runnersv1.Container, error) {
	if len(containers) == 0 {
		return nil, nil
	}
	result := make([]*runnersv1.Container, 0, len(containers))
	for _, container := range containers {
		if container == nil {
			return nil, fmt.Errorf("runner returned nil workload container")
		}
		role, err := runnerContainerRole(container.GetRole())
		if err != nil {
			return nil, err
		}
		status, err := runnerContainerStatus(container.GetStatus())
		if err != nil {
			return nil, err
		}
		result = append(result, &runnersv1.Container{
			ContainerId:  container.GetContainerId(),
			Name:         container.GetName(),
			Role:         role,
			Image:        container.GetImage(),
			Status:       status,
			Reason:       container.Reason,
			Message:      container.Message,
			ExitCode:     container.ExitCode,
			RestartCount: container.GetRestartCount(),
			StartedAt:    container.StartedAt,
			FinishedAt:   container.FinishedAt,
		})
	}
	return result, nil
}

func buildContainers(request *runnerv1.StartWorkloadRequest, resp *runnerv1.StartWorkloadResponse) []*runnersv1.Container {
	containerInfo := resp.GetContainers()
	if containerInfo == nil {
		return nil
	}
	mainSpec := request.Main
	containers := []*runnersv1.Container{}
	if containerInfo.GetMain() != "" {
		container := &runnersv1.Container{
			ContainerId: containerInfo.GetMain(),
			Role:        runnersv1.ContainerRole_CONTAINER_ROLE_MAIN,
			Status:      runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING,
		}
		container.Name = mainSpec.GetName()
		container.Image = mainSpec.GetImage()
		containers = append(containers, container)
	}
	sidecarSpecs := make(map[string]*runnerv1.ContainerSpec, len(request.Sidecars))
	for _, sidecar := range request.Sidecars {
		sidecarSpecs[sidecar.GetName()] = sidecar
	}
	for _, sidecar := range containerInfo.GetSidecars() {
		if sidecar == nil || sidecar.GetId() == "" {
			log.Printf("reconciler: warn: skipping sidecar with missing id")
			continue
		}
		container := &runnersv1.Container{
			ContainerId: sidecar.GetId(),
			Name:        sidecar.GetName(),
			Role:        runnersv1.ContainerRole_CONTAINER_ROLE_SIDECAR,
			Status:      runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING,
		}
		if spec, ok := sidecarSpecs[sidecar.GetName()]; ok && spec != nil {
			container.Image = spec.GetImage()
		}
		containers = append(containers, container)
	}
	return containers
}

func attachZitiEnrollmentEnv(container *runnerv1.ContainerSpec, jwt string) {
	container.Env = append(container.Env, &runnerv1.EnvVar{Name: assembler.ZitiEnrollmentTokenEnvVar, Value: jwt})
}

func failureSummary(failure *runnerv1.WorkloadFailure) string {
	if failure == nil {
		return "unknown failure"
	}
	if failure.GetMessage() != "" {
		return failure.GetMessage()
	}
	return failure.GetCode()
}

func attachZitiEnrollmentToken(request *runnerv1.StartWorkloadRequest, jwt string) error {
	for _, container := range request.InitContainers {
		if container.Name == assembler.ZitiEnrollContainerName {
			attachZitiEnrollmentEnv(container, jwt)
			return nil
		}
	}
	for _, container := range request.Sidecars {
		if container.Name == assembler.ZitiEnrollContainerName {
			attachZitiEnrollmentEnv(container, jwt)
			return nil
		}
	}
	return fmt.Errorf("missing ziti enroll container")
}
