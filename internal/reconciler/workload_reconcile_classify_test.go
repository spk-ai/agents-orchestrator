package reconciler

import (
	"testing"
	"time"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestClassifyStartingContainersInitImagePullFailure(t *testing.T) {
	now := time.Now().UTC()
	workload := &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1", CreatedAt: timestamppb.New(now.Add(-startGracePeriod - time.Second))}}
	containers := []*runnersv1.Container{
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_INIT, runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING, "ImagePullBackOff", "", 0, nil),
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_MAIN, runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING, "", "", 0, nil),
	}

	ready, failure, err := classifyStartingContainers(containers, workload, now)
	if err != nil {
		t.Fatalf("classify starting containers: %v", err)
	}
	if ready {
		t.Fatal("expected workload to be not ready")
	}
	if failure == nil {
		t.Fatal("expected failure")
	}
	if failure.reason != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_IMAGE_PULL_FAILED {
		t.Fatalf("unexpected failure reason: %v", failure.reason)
	}
}

func TestClassifyStartingContainersInitImagePullWithinGrace(t *testing.T) {
	now := time.Now().UTC()
	workload := &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1", CreatedAt: timestamppb.New(now)}}
	containers := []*runnersv1.Container{
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_INIT, runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING, "ImagePullBackOff", "", 0, nil),
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_MAIN, runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING, "", "", 0, nil),
	}

	ready, failure, err := classifyStartingContainers(containers, workload, now)
	if err != nil {
		t.Fatalf("classify starting containers: %v", err)
	}
	if ready {
		t.Fatal("expected workload to be not ready")
	}
	if failure != nil {
		t.Fatalf("unexpected failure: %+v", failure)
	}
}

func TestClassifyStartingContainersInitConfigInvalid(t *testing.T) {
	now := time.Now().UTC()
	workload := &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1", CreatedAt: timestamppb.New(now)}}
	containers := []*runnersv1.Container{
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_INIT, runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING, "CreateContainerConfigError", "bad init", 0, nil),
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_MAIN, runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING, "", "", 0, nil),
	}

	ready, failure, err := classifyStartingContainers(containers, workload, now)
	if err != nil {
		t.Fatalf("classify starting containers: %v", err)
	}
	if ready {
		t.Fatal("expected workload to be not ready")
	}
	if failure == nil {
		t.Fatal("expected failure")
	}
	if failure.reason != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CONFIG_INVALID {
		t.Fatalf("unexpected failure reason: %v", failure.reason)
	}
	// The record names the check, the container and its state, then the
	// runtime's own detail.
	if failure.message != `start check: init container "" cannot be created (state=WAITING reason=CreateContainerConfigError restarts=0): bad init` {
		t.Fatalf("unexpected failure message: %s", failure.message)
	}
}

func TestClassifyStartingContainersTerminatedInitConfigInvalid(t *testing.T) {
	now := time.Now().UTC()
	exitCode := int32(1)
	workload := &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1", CreatedAt: timestamppb.New(now)}}
	containers := []*runnersv1.Container{
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_INIT, runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED, "InvalidImageName", "invalid image reference", 0, &exitCode),
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_MAIN, runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING, "", "", 0, nil),
	}

	ready, failure, err := classifyStartingContainers(containers, workload, now)
	if err != nil {
		t.Fatalf("classify starting containers: %v", err)
	}
	if ready {
		t.Fatal("expected workload to be not ready")
	}
	if failure == nil {
		t.Fatal("expected failure")
	}
	if failure.reason != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CONFIG_INVALID {
		t.Fatalf("unexpected failure reason: %v", failure.reason)
	}
	if failure.message != `start check: init container "" cannot be created (state=TERMINATED reason=InvalidImageName exit=1 restarts=0): invalid image reference` {
		t.Fatalf("unexpected failure message: %s", failure.message)
	}
}

func TestClassifyStartingContainersMainConfigInvalid(t *testing.T) {
	now := time.Now().UTC()
	workload := &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1", CreatedAt: timestamppb.New(now.Add(-startGracePeriod - time.Second))}}
	containers := []*runnersv1.Container{
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_MAIN, runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING, "CreateContainerError", "bad main", 0, nil),
	}

	ready, failure, err := classifyStartingContainers(containers, workload, now)
	if err != nil {
		t.Fatalf("classify starting containers: %v", err)
	}
	if ready {
		t.Fatal("expected workload to be not ready")
	}
	if failure == nil {
		t.Fatal("expected failure")
	}
	if failure.reason != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CONFIG_INVALID {
		t.Fatalf("unexpected failure reason: %v", failure.reason)
	}
}

func TestClassifyStartingContainersMainCrashloop(t *testing.T) {
	now := time.Now().UTC()
	workload := &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1", CreatedAt: timestamppb.New(now.Add(-startGracePeriod - time.Second))}}
	containers := []*runnersv1.Container{
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_MAIN, runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING, crashLoopBackoffFlag, "crashloop", crashloopThreshold, nil),
	}

	ready, failure, err := classifyStartingContainers(containers, workload, now)
	if err != nil {
		t.Fatalf("classify starting containers: %v", err)
	}
	if ready {
		t.Fatal("expected workload to be not ready")
	}
	if failure == nil {
		t.Fatal("expected failure")
	}
	if failure.reason != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CRASHLOOP {
		t.Fatalf("unexpected failure reason: %v", failure.reason)
	}
}

func TestClassifyStartingContainersMainTerminated(t *testing.T) {
	now := time.Now().UTC()
	exitCode := int32(1)
	workload := &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1", CreatedAt: timestamppb.New(now.Add(-startGracePeriod - time.Second))}}
	containers := []*runnersv1.Container{
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_MAIN, runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED, "Error", "", 0, &exitCode),
	}

	ready, failure, err := classifyStartingContainers(containers, workload, now)
	if err != nil {
		t.Fatalf("classify starting containers: %v", err)
	}
	if ready {
		t.Fatal("expected workload to be not ready")
	}
	if failure == nil {
		t.Fatal("expected failure")
	}
	if failure.reason != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_START_FAILED {
		t.Fatalf("unexpected failure reason: %v", failure.reason)
	}
}

func TestClassifyStartingContainersReadyWithRunningInitSidecar(t *testing.T) {
	now := time.Now().UTC()
	workload := &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1", CreatedAt: timestamppb.New(now.Add(-startGracePeriod - time.Second))}}
	containers := []*runnersv1.Container{
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_INIT, runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING, "", "", 0, nil),
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_MAIN, runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING, "", "", 0, nil),
	}

	ready, failure, err := classifyStartingContainers(containers, workload, now)
	if err != nil {
		t.Fatalf("classify starting containers: %v", err)
	}
	if !ready {
		t.Fatal("expected workload to be ready")
	}
	if failure != nil {
		t.Fatalf("unexpected failure: %+v", failure)
	}
}

func TestClassifyStartingContainersMainWaitingWithRunningInitSidecar(t *testing.T) {
	now := time.Now().UTC()
	workload := &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1", CreatedAt: timestamppb.New(now.Add(-startGracePeriod - time.Second))}}
	containers := []*runnersv1.Container{
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_INIT, runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING, "", "", 0, nil),
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_MAIN, runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING, "ContainerCreating", "", 0, nil),
	}

	ready, failure, err := classifyStartingContainers(containers, workload, now)
	if err != nil {
		t.Fatalf("classify starting containers: %v", err)
	}
	if ready {
		t.Fatal("expected workload to be not ready")
	}
	if failure != nil {
		t.Fatalf("unexpected failure: %+v", failure)
	}
}

func TestClassifyStartingContainersMainTerminatedWithRunningInitSidecar(t *testing.T) {
	now := time.Now().UTC()
	exitCode := int32(1)
	workload := &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1", CreatedAt: timestamppb.New(now.Add(-startGracePeriod - time.Second))}}
	containers := []*runnersv1.Container{
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_INIT, runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING, "", "", 0, nil),
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_MAIN, runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED, "Error", "main failed", 0, &exitCode),
	}

	ready, failure, err := classifyStartingContainers(containers, workload, now)
	if err != nil {
		t.Fatalf("classify starting containers: %v", err)
	}
	if ready {
		t.Fatal("expected workload to be not ready")
	}
	if failure == nil {
		t.Fatal("expected failure")
	}
	if failure.reason != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_START_FAILED {
		t.Fatalf("unexpected failure reason: %v", failure.reason)
	}
}

func TestClassifyStartingContainersInitRestartFailure(t *testing.T) {
	now := time.Now().UTC()
	exitCode := int32(1)
	workload := &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1", CreatedAt: timestamppb.New(now.Add(-startGracePeriod - time.Second))}}
	containers := []*runnersv1.Container{
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_INIT, runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED, "init failed", "", initRetryThreshold, &exitCode),
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_MAIN, runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING, "", "", 0, nil),
	}

	ready, failure, err := classifyStartingContainers(containers, workload, now)
	if err != nil {
		t.Fatalf("classify starting containers: %v", err)
	}
	if ready {
		t.Fatal("expected workload to be not ready")
	}
	if failure == nil {
		t.Fatal("expected failure")
	}
	if failure.reason != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_START_FAILED {
		t.Fatalf("unexpected failure reason: %v", failure.reason)
	}
}

func TestClassifyStartingContainersReady(t *testing.T) {
	now := time.Now().UTC()
	exitCode := int32(0)
	workload := &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1", CreatedAt: timestamppb.New(now.Add(-startGracePeriod - time.Second))}}
	containers := []*runnersv1.Container{
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_INIT, runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED, "", "", 0, &exitCode),
		makeContainer(runnersv1.ContainerRole_CONTAINER_ROLE_MAIN, runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING, "", "", 0, nil),
	}

	ready, failure, err := classifyStartingContainers(containers, workload, now)
	if err != nil {
		t.Fatalf("classify starting containers: %v", err)
	}
	if !ready {
		t.Fatal("expected workload to be ready")
	}
	if failure != nil {
		t.Fatalf("unexpected failure: %+v", failure)
	}
}

func TestClassifyRunningContainersErrorsOnNil(t *testing.T) {
	failure, err := classifyRunningContainers([]*runnersv1.Container{nil})
	if err == nil {
		t.Fatal("expected error")
	}
	if failure != nil {
		t.Fatalf("unexpected failure: %+v", failure)
	}
}

func makeContainer(role runnersv1.ContainerRole, status runnersv1.ContainerStatus, reason, message string, restartCount int32, exitCode *int32) *runnersv1.Container {
	return &runnersv1.Container{
		Role:         role,
		Status:       status,
		Reason:       stringPtr(reason),
		Message:      stringPtr(message),
		RestartCount: restartCount,
		ExitCode:     exitCode,
	}
}
