package reconciler

import (
	"context"
	"strings"
	"testing"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type testClock struct{ now time.Time }

func (c *testClock) Now() time.Time { return c.now }

func newTestFailedWorkloads(cfg FailedWorkloadConfig, clock *testClock) *failedWorkloads {
	f := newFailedWorkloads(cfg)
	f.now = clock.Now
	return f
}

func TestFailedWorkloadRetentionDecisions(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 10, 3, 18, 33, 43, 0, time.UTC)}
	failedAt := clock.now
	for _, disabled := range []FailedWorkloadConfig{{}, {Retention: time.Minute}, {RetentionMax: 1}} {
		if newTestFailedWorkloads(disabled, clock).decide("a", true, failedAt) {
			t.Fatalf("retention %+v must keep current behaviour (remove at once)", disabled)
		}
	}
	var nilFailures *failedWorkloads
	if capture, _, _ := nilFailures.beginCapture("a", true); nilFailures.decide("a", true, failedAt) || capture {
		t.Fatal("an unconfigured reconciler retains nothing and captures nothing")
	}

	f := newTestFailedWorkloads(FailedWorkloadConfig{Retention: 30 * time.Minute, RetentionMax: 1}, clock)
	if !f.decide("a", true, failedAt) {
		t.Fatal("first failure not retained")
	}
	if f.decide("b", true, failedAt) {
		t.Fatal("FAILED_WORKLOAD_RETENTION_MAX exceeded")
	}
	clock.now = failedAt.Add(29 * time.Minute)
	if !f.decide("a", true, failedAt) {
		t.Fatal("retained Pod released inside its window")
	}
	// A restart re-admits the Pod for the rest of its window, not a new one.
	restarted := newTestFailedWorkloads(FailedWorkloadConfig{Retention: 30 * time.Minute, RetentionMax: 1}, clock)
	if !restarted.decide("a", true, failedAt) {
		t.Fatal("restart lost a retained Pod")
	}
	clock.now = failedAt.Add(30 * time.Minute)
	if restarted.decide("a", true, failedAt) || f.decide("a", true, failedAt) {
		t.Fatal("Pod retained past its window")
	}
	// The expired slot is free again, but only for a failure still inside its
	// own window.
	if f.decide("b", true, failedAt) {
		t.Fatal("expired failure retained")
	}
	if !f.decide("c", true, clock.now.Add(-time.Minute)) {
		t.Fatal("freed slot not reused")
	}
	if f.decide("c", false, clock.now) {
		t.Fatal("Pod that is gone stays retained")
	}
	if !f.decide("d", true, clock.now) {
		t.Fatal("slot of a vanished Pod not freed")
	}
	f.release("d", "a newer start is wanted")
	if f.decide("d", true, clock.now) {
		t.Fatal("released Pod retained")
	}
	f.forget("d")
	if !f.decide("d", true, clock.now) {
		t.Fatal("forget must clear a release")
	}
	// Without a known failure time the window starts when first seen.
	unknown := newTestFailedWorkloads(FailedWorkloadConfig{Retention: time.Minute, RetentionMax: 1}, clock)
	if !unknown.decide("e", true, time.Time{}) {
		t.Fatal("unknown failure time not retained")
	}
	clock.now = clock.now.Add(time.Minute)
	if unknown.decide("e", true, time.Time{}) {
		t.Fatal("unknown failure time retained forever")
	}
	// Evidence is captured when retention starts and again before removal;
	// removal waits while a capture is in flight, and an unstored capture is
	// retried.
	capture, busy, previous := unknown.beginCapture("e", false)
	if !capture || busy {
		t.Fatal("initial capture refused")
	}
	if capture, busy, _ := unknown.beginCapture("e", true); capture || !busy {
		t.Fatal("concurrent capture must make removal wait")
	}
	unknown.endCapture("e", false, false, previous)
	capture, _, previous = unknown.beginCapture("e", false)
	if !capture {
		t.Fatal("unstored capture not retried")
	}
	unknown.endCapture("e", false, true, previous)
	if capture, busy, _ := unknown.beginCapture("e", false); capture || busy {
		t.Fatal("initial evidence captured twice")
	}
	capture, _, previous = unknown.beginCapture("e", true)
	if !capture {
		t.Fatal("final capture skipped after the initial one")
	}
	unknown.endCapture("e", true, true, previous)
	if capture, busy, _ := unknown.beginCapture("e", true); capture || busy {
		t.Fatal("final evidence captured twice")
	}
}

type retentionFixture struct {
	*preparedControllerFixture
	clock     *testClock
	events    []string
	tails     map[string]string
	tailCalls int
	w         *runnersv1.Workload
}

// newRetentionFixture starts an agent workload, then lets the runner report
// it FAILED without a reason, as k8s-runner does for a Failed Pod phase.
func newRetentionFixture(t *testing.T, sandbox bool, cfg FailedWorkloadConfig) *retentionFixture {
	t.Helper()
	f := &retentionFixture{preparedControllerFixture: newPreparedControllerFixture(t, sandbox), clock: &testClock{now: time.Now().UTC()}}
	w, err := f.start()
	if err != nil {
		t.Fatal(err)
	}
	f.w = w
	f.r.failures = newTestFailedWorkloads(cfg, f.clock)
	exit := func(code int32) *int32 { return &code }
	f.native.inspectPreparedWorkload = func(_ context.Context, req *runnerv1.InspectPreparedWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.InspectPreparedWorkloadResponse, error) {
		f.inspections++
		if f.binding == nil {
			return nil, status.Error(codes.NotFound, "pod absent")
		}
		return &runnerv1.InspectPreparedWorkloadResponse{Binding: proto.Clone(f.binding).(*runnerv1.WorkloadBinding), Activated: true, ResourceVersion: "124",
			Workload: &runnerv1.InspectWorkloadResponse{Id: f.binding.WorkloadId, Containers: []*runnerv1.WorkloadContainer{
				{Name: "ziti-enroll", Role: runnerv1.ContainerRole_CONTAINER_ROLE_INIT, Status: runnerv1.ContainerStatus_CONTAINER_STATUS_TERMINATED, ExitCode: exit(0), Reason: stringPtr("Completed")},
				{Name: "main", Role: runnerv1.ContainerRole_CONTAINER_ROLE_MAIN, Status: runnerv1.ContainerStatus_CONTAINER_STATUS_TERMINATED, ExitCode: exit(3), Reason: stringPtr("Error"), Message: stringPtr("fatal: token=" + testOpaqueAuth)},
			}}}, nil
	}
	f.tails = map[string]string{
		"ziti-enroll": "enrolled identity\n",
		"main":        "starting agent\nAuthorization: Bearer " + testOpaqueAuth + "\npanic: boom while using " + testJWT + "\n",
	}
	f.native.tailWorkloadLogs = func(_ context.Context, req *runnerv1.TailWorkloadLogsRequest, _ ...grpc.CallOption) (*runnerv1.TailWorkloadLogsResponse, error) {
		f.tailCalls++
		if req.WorkloadId != f.w.Meta.Id || req.MaxBytes == 0 || req.MaxBytes > MaxFailedWorkloadEvidenceLogBytes {
			t.Fatalf("unbounded or misaddressed tail request %+v", req)
		}
		return &runnerv1.TailWorkloadLogsResponse{Data: []byte(f.tails[req.ContainerName]), MaxBytes: req.MaxBytes}, nil
	}
	update := f.registry.updateWorkload
	f.registry.updateWorkload = func(ctx context.Context, req *runnersv1.UpdateWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
		record := f.preparedControllerFixture.w
		switch {
		case req.FailureReason != nil && req.Status == nil:
			f.events = append(f.events, "reason")
			record.FailureReason, record.FailureMessage = req.FailureReason, req.FailureMessage
			return &runnersv1.UpdateWorkloadResponse{Workload: proto.Clone(record).(*runnersv1.Workload)}, nil
		case len(req.Containers) > 0 && req.Containers[0].OutputTail != nil:
			f.events = append(f.events, "evidence@"+preparedPhaseName(record))
			record.Containers = req.Containers
			return &runnersv1.UpdateWorkloadResponse{Workload: proto.Clone(record).(*runnersv1.Workload)}, nil
		}
		return update(ctx, req, opts...)
	}
	remove := f.native.removePreparedWorkload
	f.native.removePreparedWorkload = func(ctx context.Context, req *runnerv1.RemovePreparedWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.RemovePreparedWorkloadResponse, error) {
		f.events = append(f.events, "remove")
		return remove(ctx, req, opts...)
	}
	record := f.preparedControllerFixture.w
	record.Status = runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED
	record.RemovedAt = timestamppb.New(f.clock.now)
	w.Status, w.RemovedAt = record.Status, record.RemovedAt
	return f
}

func (f *retentionFixture) stop() error {
	return f.r.stopPreparedWorkload(context.Background(), f.native, f.w)
}

func (f *retentionFixture) record() *runnersv1.Workload {
	return f.preparedControllerFixture.w
}

func (f *retentionFixture) assertEvidence() {
	f.t.Helper()
	containers := f.record().GetContainers()
	if len(containers) != 2 {
		f.t.Fatalf("expected the final status of both containers, got %v", containers)
	}
	main := containers[1]
	if main.GetName() != "main" || main.GetExitCode() != 3 || main.GetStatus() != runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED {
		f.t.Fatalf("final main status not recorded: %v", main)
	}
	tail := main.GetOutputTail()
	if !strings.Contains(tail, "panic: boom") || !strings.Contains(tail, "[REDACTED:jwt]") || !strings.Contains(tail, "Bearer [REDACTED]") {
		f.t.Fatalf("main output not captured and redacted: %q", tail)
	}
	for _, container := range containers {
		for _, secret := range []string{testJWT, testOpaqueAuth} {
			if strings.Contains(container.GetOutputTail(), secret) || strings.Contains(container.GetMessage(), secret) {
				f.t.Fatalf("credential persisted in %s evidence", container.GetName())
			}
		}
	}
	if containers[0].GetOutputTail() != "enrolled identity\n" {
		f.t.Fatalf("init output: %q", containers[0].GetOutputTail())
	}
}

func TestFailedPodIsRetainedThenRemovedWithEvidence(t *testing.T) {
	f := newRetentionFixture(t, false, FailedWorkloadConfig{Retention: 30 * time.Minute, RetentionMax: 1, EvidenceLogBytes: 64 * 1024})
	for i := 0; i < 2; i++ {
		if err := f.stop(); err != nil {
			t.Fatal(err)
		}
		f.clock.now = f.clock.now.Add(10 * time.Minute)
	}
	record := f.record()
	if f.removals != 0 || record.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVE || record.RemovalConfirmedAt != nil || f.binding == nil {
		t.Fatal("retained Pod was removed or its admission released")
	}
	// The reason is recorded at once, from the containers the runner left.
	if record.GetFailureReason() != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_START_FAILED ||
		!strings.Contains(record.GetFailureMessage(), `runner reported the Pod failed: main container "main" exited (state=TERMINATED reason=Error exit=3 restarts=0)`) ||
		strings.Contains(record.GetFailureMessage(), testOpaqueAuth) {
		t.Fatalf("failure reason not recorded precisely: %s %q", record.GetFailureReason(), record.GetFailureMessage())
	}
	// Evidence is readable while the Pod is kept, captured once.
	if strings.Join(f.events, ",") != "reason,evidence@ACTIVE" || f.tailCalls != 2 {
		t.Fatalf("unexpected writes while retained: %v (tails %d)", f.events, f.tailCalls)
	}
	f.assertEvidence()
	f.clock.now = f.clock.now.Add(10 * time.Minute)
	f.tails["main"] += "after retention: still failing\n"
	if err := f.stop(); err != nil {
		t.Fatal(err)
	}
	// And refreshed immediately before removal begins.
	if strings.Join(f.events, ",") != "reason,evidence@ACTIVE,evidence@ACTIVE,remove" {
		t.Fatalf("evidence must be stored before removal begins: %v", f.events)
	}
	if !strings.HasSuffix(f.record().GetContainers()[1].GetOutputTail(), "after retention: still failing\n") {
		t.Fatal("final evidence not refreshed before removal")
	}
	if f.removals != 1 || f.record().RemovalConfirmedAt == nil || f.binding != nil {
		t.Fatal("expired retention did not remove the Pod")
	}
	f.assertEvidence()
	if f.r.failures.isHeld(f.w.Meta.Id) {
		t.Fatal("removed workload still occupies a retention slot")
	}
}

func TestFailedPodBeyondRetentionLimitIsRemovedWithEvidence(t *testing.T) {
	f := newRetentionFixture(t, false, FailedWorkloadConfig{Retention: 30 * time.Minute, RetentionMax: 1, EvidenceLogBytes: 64 * 1024})
	if !f.r.failures.decide(uuid.NewString(), true, f.clock.now) {
		t.Fatal("setup: first slot")
	}
	if err := f.stop(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(f.events, ",") != "reason,evidence@ACTIVE,remove" || f.record().RemovalConfirmedAt == nil {
		t.Fatalf("unexpected sequence %v", f.events)
	}
	f.assertEvidence()
}

func TestRetentionDisabledStillCapturesEvidenceBeforeRemoval(t *testing.T) {
	f := newRetentionFixture(t, false, FailedWorkloadConfig{EvidenceLogBytes: 64 * 1024})
	if err := f.stop(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(f.events, ",") != "reason,evidence@ACTIVE,remove" || f.record().RemovalConfirmedAt == nil {
		t.Fatalf("unexpected sequence %v", f.events)
	}
	f.assertEvidence()
}

func TestSandboxFailuresAreNotRetained(t *testing.T) {
	f := newRetentionFixture(t, true, FailedWorkloadConfig{Retention: 30 * time.Minute, RetentionMax: 1, EvidenceLogBytes: 64 * 1024})
	if err := f.stop(); err != nil {
		t.Fatal(err)
	}
	if f.removals != 1 || !strings.Contains(strings.Join(f.events, ","), "evidence@ACTIVE,remove") {
		t.Fatalf("sandbox retained or removed without evidence: %v", f.events)
	}
}

func TestDeletingARetainedPodEndsRetention(t *testing.T) {
	f := newRetentionFixture(t, false, FailedWorkloadConfig{Retention: 30 * time.Minute, RetentionMax: 1, EvidenceLogBytes: 64 * 1024})
	if err := f.stop(); err != nil || f.removals != 0 {
		t.Fatalf("not retained: %v", err)
	}
	// An operator who is done investigating deletes the Pod.
	f.binding = nil
	if err := f.stop(); err != nil {
		t.Fatal(err)
	}
	if f.record().RemovalConfirmedAt == nil {
		t.Fatal("removal not confirmed after the Pod was deleted")
	}
	// The evidence captured when retention started is kept, not replaced by
	// notes about a Pod that can no longer be read.
	f.assertEvidence()
}

func TestPodGoneBeforeAnyCaptureRecordsWhy(t *testing.T) {
	f := newRetentionFixture(t, false, FailedWorkloadConfig{EvidenceLogBytes: 64 * 1024})
	f.binding = nil
	if err := f.stop(); err != nil {
		t.Fatal(err)
	}
	if f.record().RemovalConfirmedAt == nil {
		t.Fatal("removal not confirmed")
	}
	if !strings.Contains(f.record().GetFailureMessage(), "the Pod was already gone") {
		t.Fatalf("reason does not say the Pod was gone: %q", f.record().GetFailureMessage())
	}
}

func TestRunnerWithoutLogTailsRecordsWhy(t *testing.T) {
	f := newRetentionFixture(t, false, FailedWorkloadConfig{EvidenceLogBytes: 64 * 1024})
	f.native.tailWorkloadLogs = nil
	if err := f.stop(); err != nil {
		t.Fatal(err)
	}
	for _, container := range f.record().GetContainers() {
		if container.GetOutputTail() != "[output unavailable: runner does not implement TailWorkloadLogs]" {
			t.Fatalf("unexpected tail %q", container.GetOutputTail())
		}
	}
	if f.record().GetContainers()[1].GetExitCode() != 3 {
		t.Fatal("container statuses must still be recorded")
	}
}

func TestReleaseFailedRetentionForNewerStarts(t *testing.T) {
	clock := &testClock{now: time.Now().UTC()}
	r := &Reconciler{failures: newTestFailedWorkloads(FailedWorkloadConfig{Retention: time.Hour, RetentionMax: 4}, clock)}
	agentID := uuid.New()
	failedAt := clock.now.Add(-time.Minute)
	workload := func(instance uuid.UUID) *runnersv1.Workload {
		return &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: uuid.NewString()}, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
			AgentInstanceId: stringPtr(instance.String()), RemovedAt: timestamppb.New(failedAt)}
	}
	updated, waiting, stopped := workload(uuid.New()), workload(uuid.New()), workload(uuid.New())
	for _, w := range []*runnersv1.Workload{updated, waiting, stopped} {
		if !r.failures.decide(w.Meta.Id, true, failedAt) {
			t.Fatal("setup: retain")
		}
	}
	desired := []AgentInstanceTarget{
		{AgentID: agentID, AgentInstanceID: uuid.MustParse(updated.GetAgentInstanceId())},
		{AgentID: uuid.New(), AgentInstanceID: uuid.MustParse(waiting.GetAgentInstanceId())},
	}
	r.releaseFailedRetention(desired, []*runnersv1.Workload{updated, waiting, stopped},
		map[uuid.UUID]struct{}{uuid.MustParse(stopped.GetAgentInstanceId()): {}},
		map[uuid.UUID]time.Time{agentID: clock.now, desired[1].AgentID: failedAt.Add(-time.Second)})
	if r.failures.decide(updated.Meta.Id, true, failedAt) {
		t.Fatal("an agent changed after the failure must end retention")
	}
	if r.failures.decide(stopped.Meta.Id, true, failedAt) {
		t.Fatal("a paused or terminated instance must end retention")
	}
	// Waiting work alone does not: a failed start always leaves it waiting.
	if !r.failures.decide(waiting.Meta.Id, true, failedAt) {
		t.Fatal("waiting work ended retention")
	}
}

// After retention the instance is retried: removal is confirmed, and the
// start backoff, measured from the failure, has long elapsed.
func TestInstanceRetriesWhenRetentionEnds(t *testing.T) {
	now := time.Now().UTC()
	agentID, instanceID := uuid.New(), uuid.New()
	failure := makeDecisionWorkload("retained", runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED, now.Add(-31*time.Minute), now.Add(-30*time.Minute))
	fixture := startDecisionFixture{t: t, agentInstanceID: instanceID, active: []*runnersv1.Workload{failure}, latest: []*runnersv1.Workload{failure}, failed: []*runnersv1.Workload{failure}}
	r := newTestReconciler(Config{Runners: &fakeRunnersClient{listWorkloadsByAgentInstance: fixture.list}})
	target := AgentInstanceTarget{AgentID: agentID, AgentInstanceID: instanceID}
	updatedAt := map[uuid.UUID]time.Time{agentID: now.Add(-time.Hour)}
	failure.RemovalConfirmedAt = nil
	if start, err := r.shouldStartWorkload(context.Background(), target, now, updatedAt); err != nil || start {
		t.Fatalf("retained failure must block a second start: %v %v", start, err)
	}
	failure.RemovalConfirmedAt = timestamppb.New(now)
	if start, err := r.shouldStartWorkload(context.Background(), target, now, updatedAt); err != nil || !start {
		t.Fatalf("instance not retried after retention: %v %v", start, err)
	}
}

func TestStartDeadlineRecordsWhichCheckFailed(t *testing.T) {
	var requests []*runnersv1.UpdateWorkloadRequest
	r := &Reconciler{runners: &fakeRunnersClient{updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
		requests = append(requests, req)
		return &runnersv1.UpdateWorkloadResponse{}, nil
	}}}
	w := &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: uuid.NewString()}, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		Preparation: &runnersv1.PreparedWorkloadLifecycle{Phase: runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_BOUND}}
	r.failStartDeadline(context.Background(), w, "preparation stopped in phase "+preparedPhaseName(w))
	if len(requests) != 1 || requests[0].GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED ||
		requests[0].GetFailureReason() != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_START_FAILED ||
		requests[0].GetFailureMessage() != "start deadline: preparation stopped in phase BOUND" {
		t.Fatalf("unexpected requests %v", requests)
	}
	w.Status = runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING
	r.failStartDeadline(context.Background(), w, "late")
	if len(requests) != 1 {
		t.Fatal("a stop in progress must keep its status")
	}
}
