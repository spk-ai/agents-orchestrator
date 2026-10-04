package reconciler

import (
	"context"
	"log"
	"sync"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// DefaultFailedWorkloadRetentionMax bounds how many failed Pods are kept at
// once when retention is enabled without an explicit limit.
const DefaultFailedWorkloadRetentionMax = 1

// retainedPodInspectInterval spaces re-inspections of a retained Pod: both
// reconcile loops visit it every tick, and each inspection costs the runner
// rate-limited Kubernetes reads. An operator deleting the Pod is noticed
// within this interval.
const retainedPodInspectInterval = 30 * time.Second

// FailedWorkloadConfig controls what happens to a failed workload's Pod
// before removal. Retention 0 removes failed workloads as soon as they are
// judged failed, which was the only behaviour before retention existed.
type FailedWorkloadConfig struct {
	// Retention keeps a failed agent workload's Pod this long after it failed.
	Retention time.Duration
	// RetentionMax is the most failed Pods kept at once; further failures are
	// removed immediately (after their evidence is captured).
	RetentionMax int
	// EvidenceLogBytes is the newest output kept per container in the
	// record's evidence; 0 records statuses without output.
	EvidenceLogBytes int
}

// failedWorkloads owns retention decisions and evidence de-duplication for
// this process. Decisions are re-derived from durable state: the failure time
// is the record's removed_at (set when it became FAILED) and the Pod's
// presence is inspected each time, so a restart re-admits retained Pods for
// the remainder of their window instead of restarting it.
//
// A retained workload is FAILED with its removal unconfirmed. That keeps its
// capacity slot and blocks a replacement start for its agent instance (see
// shouldStartWorkload); the replacement starts after removal is confirmed,
// with the start backoff measured from the failure. Only the reconciler that
// holds the leader lease acts on these decisions.
// @see runners::internal/server/workloads
type failedWorkloads struct {
	retention        time.Duration
	retentionMax     int
	evidenceLogBytes int
	now              func() time.Time

	mu        sync.Mutex
	held      map[string]time.Time
	inspected map[string]time.Time
	firstSeen map[string]time.Time
	released  map[string]string
	reported  map[string]bool
	captured  map[string]captureState
}

// captureState orders a workload's evidence captures: one when retention
// starts, so evidence is readable at once and survives the Pod being deleted
// early, and a final one before removal.
type captureState int

const (
	captureNone captureState = iota
	captureInFlight
	captureInitialStored
	captureFinalStored
)

func newFailedWorkloads(cfg FailedWorkloadConfig) *failedWorkloads {
	return &failedWorkloads{
		retention:        cfg.Retention,
		retentionMax:     cfg.RetentionMax,
		evidenceLogBytes: cfg.EvidenceLogBytes,
		now:              time.Now,
		held:             map[string]time.Time{},
		inspected:        map[string]time.Time{},
		firstSeen:        map[string]time.Time{},
		released:         map[string]string{},
		reported:         map[string]bool{},
		captured:         map[string]captureState{},
	}
}

func (f *failedWorkloads) enabled() bool {
	return f != nil && f.retention > 0 && f.retentionMax > 0
}

// decide reports whether the failed workload keeps its Pod now. eligible says
// whether this workload can be retained at all (an agent workload whose Pod
// is still present); failedAt starts its window, or, when unknown (zero), the
// first time this process saw the failure.
func (f *failedWorkloads) decide(workloadID string, eligible bool, failedAt time.Time) bool {
	if !f.enabled() {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()
	for id, until := range f.held {
		if !now.Before(until) {
			delete(f.held, id)
			log.Printf("reconciler: retention of failed workload %s ended at %s; removing it", id, until.UTC().Format(time.RFC3339))
		}
	}
	_, wasHeld := f.held[workloadID]
	if reason, ok := f.released[workloadID]; ok {
		if wasHeld {
			delete(f.held, workloadID)
			log.Printf("reconciler: releasing retained failed workload %s early: %s", workloadID, reason)
		}
		return false
	}
	if !eligible {
		if wasHeld {
			delete(f.held, workloadID)
			log.Printf("reconciler: retained failed workload %s no longer has a Pod to keep", workloadID)
		}
		return false
	}
	if failedAt.IsZero() {
		if _, ok := f.firstSeen[workloadID]; !ok {
			f.firstSeen[workloadID] = now
		}
		failedAt = f.firstSeen[workloadID]
	}
	until := failedAt.Add(f.retention)
	if !now.Before(until) {
		return false
	}
	if wasHeld {
		return true
	}
	if len(f.held) >= f.retentionMax {
		if !f.reported[workloadID] {
			f.reported[workloadID] = true
			log.Printf("reconciler: not retaining failed workload %s: %d failed Pod(s) already retained (FAILED_WORKLOAD_RETENTION_MAX=%d)", workloadID, len(f.held), f.retentionMax)
		}
		return false
	}
	f.held[workloadID] = until
	log.Printf("reconciler: retaining failed workload %s Pod until %s for investigation; its capacity stays reserved and its agent instance is not restarted until then",
		workloadID, until.UTC().Format(time.RFC3339))
	return true
}

// recentlyInspected reports whether a retained Pod was inspected within
// retainedPodInspectInterval; markInspected records an inspection.
func (f *failedWorkloads) recentlyInspected(workloadID string) bool {
	if f == nil {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	at, ok := f.inspected[workloadID]
	_, held := f.held[workloadID]
	return ok && held && f.now().Sub(at) < retainedPodInspectInterval
}

func (f *failedWorkloads) markInspected(workloadID string) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inspected[workloadID] = f.now()
}

func (f *failedWorkloads) isHeld(workloadID string) bool {
	if f == nil {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	_, held := f.held[workloadID]
	return held
}

// release ends a workload's retention early. It is a no-op when the workload
// is not retained, and is remembered so a later decision does not re-admit it.
func (f *failedWorkloads) release(workloadID, reason string) {
	if !f.enabled() || workloadID == "" {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.released[workloadID]; !ok {
		f.released[workloadID] = reason
	}
}

// beginCapture reports whether this caller should capture evidence for
// workloadID now (final: the capture before removal), and whether another
// caller is still capturing it -- in which case removal must wait, or it would
// delete the Pod mid-read. previous is handed back to endCapture. This
// de-duplicates the two reconcile loops and a capture Runners did not store.
func (f *failedWorkloads) beginCapture(workloadID string, final bool) (capture, busy bool, previous captureState) {
	if f == nil {
		return false, false, captureNone
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	previous = f.captured[workloadID]
	switch {
	case previous == captureInFlight:
		return false, true, previous
	case final && previous == captureFinalStored, !final && previous >= captureInitialStored:
		return false, false, previous
	}
	f.captured[workloadID] = captureInFlight
	return true, false, previous
}

// endCapture records the outcome of a capture. One that could not be stored
// is retried by the next attempt.
func (f *failedWorkloads) endCapture(workloadID string, final, stored bool, previous captureState) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case !stored:
		f.captured[workloadID] = previous
	case final:
		f.captured[workloadID] = captureFinalStored
	default:
		f.captured[workloadID] = captureInitialStored
	}
}

// forget drops everything known about a workload once its removal is
// confirmed.
func (f *failedWorkloads) forget(workloadID string) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.held, workloadID)
	delete(f.inspected, workloadID)
	delete(f.firstSeen, workloadID)
	delete(f.released, workloadID)
	delete(f.reported, workloadID)
	delete(f.captured, workloadID)
}

// workloadFailedAt is when a workload became FAILED: Runners sets removed_at
// (the end of metering) with the first terminal status. Zero when unknown;
// updated_at is no substitute, because evidence writes move it.
func workloadFailedAt(w *runnersv1.Workload) time.Time {
	if removedAt := w.GetRemovedAt(); removedAt != nil && removedAt.CheckValid() == nil {
		return removedAt.AsTime().UTC()
	}
	return time.Time{}
}

// failedPodObservation is one inspection of a failed prepared workload's Pod.
type failedPodObservation struct {
	present    bool
	containers []*runnersv1.Container
	err        error
}

func (r *Reconciler) observeFailedPod(ctx context.Context, runner runnerv1.RunnerServiceClient, w *runnersv1.Workload) failedPodObservation {
	response, err := runner.InspectPreparedWorkload(ctx, &runnerv1.InspectPreparedWorkloadRequest{Expected: proto.Clone(w.Preparation.Binding).(*runnerv1.WorkloadBinding)})
	if err != nil {
		return failedPodObservation{err: err}
	}
	if !samePreparedBinding(w.Preparation.Binding, response.GetBinding()) || response.GetWorkload().GetId() != w.Meta.Id {
		return failedPodObservation{err: status.Error(codes.FailedPrecondition, "inspection returned a different incarnation")}
	}
	containers, err := mapRunnerContainers(response.GetWorkload().GetContainers())
	if err != nil {
		return failedPodObservation{err: err}
	}
	return failedPodObservation{present: !response.GetRemovalPending(), containers: containers}
}

// holdFailedWorkload runs before a FAILED prepared workload whose Pod may
// still exist begins removal. It records a reason when the failure arrived
// without one (a runner reports a Failed Pod phase with no detail), keeps the
// Pod while retention allows, and otherwise captures evidence so removal can
// proceed. It reports true while the Pod is retained, or while another loop
// is still capturing its evidence.
func (r *Reconciler) holdFailedWorkload(ctx context.Context, runner runnerv1.RunnerServiceClient, w *runnersv1.Workload) bool {
	reasonMissing := w.FailureReason == nil || w.GetFailureReason() == runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_UNSPECIFIED
	if !reasonMissing && r.failures.recentlyInspected(w.Meta.Id) && r.failures.decide(w.Meta.Id, true, workloadFailedAt(w)) {
		return true
	}
	observation := r.observeFailedPod(ctx, runner, w)
	r.failures.markInspected(w.Meta.Id)
	if reasonMissing {
		failure := diagnoseFailedContainers("runner reported the Pod failed", observation.containers)
		if observation.containers == nil {
			failure = diagnoseFailedContainers("runner reported the Pod failed", w.GetContainers())
			failure.message = boundFailureMessage(failure.message + "; " + inspectionNote(observation.err))
		}
		r.recordFailureReason(ctx, w, failure)
	}
	if r.failures == nil {
		return false
	}
	eligible := observation.present && w.GetOwnerKind() == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE
	if observation.err != nil && status.Code(observation.err) != codes.NotFound {
		// Presence is unknown, not refuted: keep a retained Pod for its window
		// rather than removing it on a transient inspection error.
		eligible = r.failures.isHeld(w.Meta.Id)
	}
	var observed []*runnersv1.Container
	if observation.err == nil {
		observed = observation.containers
		if observed == nil {
			observed = []*runnersv1.Container{}
		}
	}
	if r.failures.decide(w.Meta.Id, eligible, workloadFailedAt(w)) {
		// Evidence is readable while the Pod is kept, and survives an
		// operator deleting it early.
		if !evidenceCaptured(w) {
			r.captureFailureEvidence(ctx, runner, w, observed, observation.err, false)
		}
		return true
	}
	// Removal waits for a capture another loop has in flight.
	return r.captureFailureEvidence(ctx, runner, w, observed, observation.err, true)
}

// recordFailureReason adds a reason to a record that is already FAILED,
// without changing its status.
func (r *Reconciler) recordFailureReason(ctx context.Context, w *runnersv1.Workload, failure *workloadFailure) {
	reason := failure.reason
	message := boundFailureMessage(failure.message)
	log.Printf("reconciler: workload %s failed (%s): %s", w.GetMeta().GetId(), reason, message)
	response, err := r.runners.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{Id: w.GetMeta().GetId(), FailureReason: &reason, FailureMessage: stringPtr(message)})
	if err != nil {
		log.Printf("reconciler: record failure reason for workload %s: %v", w.GetMeta().GetId(), err)
		return
	}
	if updated := response.GetWorkload(); updated != nil {
		w.FailureReason, w.FailureMessage = updated.FailureReason, updated.FailureMessage
	}
}

// releaseFailedRetention ends retention early where keeping the failed Pod
// would block work that is now wanted: its agent instance was paused or
// terminated, or its agent or environment changed after the failure, which
// is the explicit signal for a newer start (shouldStartWorkload bypasses the
// start backoff on it too). Waiting work alone does not end retention: a
// failed start always leaves its message waiting.
func (r *Reconciler) releaseFailedRetention(desired []AgentInstanceTarget, actual []*runnersv1.Workload, stopRequests map[uuid.UUID]struct{}, agentUpdatedAt map[uuid.UUID]time.Time) {
	if !r.failures.enabled() {
		return
	}
	agents := make(map[uuid.UUID]uuid.UUID, len(desired))
	for _, target := range desired {
		agents[target.AgentInstanceID] = target.AgentID
	}
	for _, w := range actual {
		if w.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
			continue
		}
		instanceID, err := uuid.Parse(workloadAgentInstanceID(w))
		if err != nil {
			continue
		}
		if _, ok := stopRequests[instanceID]; ok {
			r.failures.release(w.GetMeta().GetId(), "its agent instance is paused or terminated")
			continue
		}
		agentID, ok := agents[instanceID]
		if !ok {
			continue
		}
		failedAt := workloadFailedAt(w)
		if updatedAt, ok := agentUpdatedAt[agentID]; ok && !failedAt.IsZero() && updatedAt.After(failedAt) {
			r.failures.release(w.GetMeta().GetId(), "its agent or environment changed after the failure, so a newer start is wanted")
		}
	}
}
