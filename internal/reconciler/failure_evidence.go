package reconciler

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Failure evidence is written to the workload record itself: the final
// container statuses and, per container, Container.output_tail -- the newest
// redacted output read through the runner's bounded TailWorkloadLogs before
// the Pod is removed. Runners keeps the tails out of list responses, so they
// do not grow the pages this process reads every cycle; operators read them
// with GetWorkload. It is captured immediately before removal begins, whether
// or not the Pod was retained first, and also when retention starts.
// @see api::proto/agynio/api/runners/v1/runners
// @see runners::internal/server/workloads
// @see k8s-runner::internal/server/streaming
const (
	// DefaultFailedWorkloadEvidenceLogBytes is the per-container tail kept
	// when the operator does not choose one.
	DefaultFailedWorkloadEvidenceLogBytes = 64 * 1024
	// MaxFailedWorkloadEvidenceLogBytes matches Runners' per-container bound.
	MaxFailedWorkloadEvidenceLogBytes = 256 * 1024
	// evidenceWorkloadBytes stays under Runners' per-workload bound, with room
	// for the notes and truncation markers added here.
	evidenceWorkloadBytes = 960 * 1024
	// evidenceReadSlack is read beyond the kept bytes so that the line a
	// runner-side cut started inside can be dropped whole.
	evidenceReadSlack = 4 * 1024
	evidenceTailLines = 2000
	evidenceTimeout   = 20 * time.Second
	evidenceCallLimit = 8 * time.Second
	// failureMessageBytes bounds failure_message, which Runners also copies
	// into workload notifications. It never carries container output.
	failureMessageBytes = 2048
	evidenceNoteBytes   = 256

	evidenceTruncatedMarker = "[agyn: earlier output truncated]\n"
)

// secretPatterns are replaced before anything leaves this process. Evidence
// is over-redacted on purpose: a lost token-shaped word costs less than a
// leaked credential in a record that outlives the workload.
var secretPatterns = []struct {
	pattern     *regexp.Regexp
	replacement string
}{
	{regexp.MustCompile(`-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[\s\S]*?(-----END [A-Z0-9 ]*PRIVATE KEY-----|$)`), "[REDACTED:private-key]"},
	// JWTs, including OpenZiti enrollment tokens, and any other three-segment
	// base64url token.
	{regexp.MustCompile(`eyJ[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\.?[A-Za-z0-9_-]*`), "[REDACTED:jwt]"},
	{regexp.MustCompile(`[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}`), "[REDACTED:token]"},
	// Provider key formats: OpenAI/Anthropic, GitHub, Slack, AWS, Google,
	// Stripe, Doppler, Hugging Face, npm.
	{regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{16,}`), "[REDACTED:api-key]"},
	{regexp.MustCompile(`\b(gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})`), "[REDACTED:api-key]"},
	{regexp.MustCompile(`\bxox[abposr]-[A-Za-z0-9-]{10,}`), "[REDACTED:api-key]"},
	{regexp.MustCompile(`\b(AKIA|ASIA)[0-9A-Z]{16}\b`), "[REDACTED:api-key]"},
	{regexp.MustCompile(`\bAIza[0-9A-Za-z_-]{35}`), "[REDACTED:api-key]"},
	{regexp.MustCompile(`\b[rs]k_(live|test)_[A-Za-z0-9]{16,}`), "[REDACTED:api-key]"},
	{regexp.MustCompile(`\bdp\.(st|pt|ct|sa|scim|audit)\.[A-Za-z0-9_.-]{16,}`), "[REDACTED:api-key]"},
	{regexp.MustCompile(`\b(hf|npm)_[A-Za-z0-9]{20,}`), "[REDACTED:api-key]"},
	// Credentials in URLs and authorization headers, whatever the scheme.
	{regexp.MustCompile(`(?i)\b([a-z][a-z0-9+.-]*://[^/\s:@'"]+:)[^@\s/'"]+@`), "${1}[REDACTED]@"},
	{regexp.MustCompile(`(?i)\b((?:proxy-)?authorization["']?\s*[:=]\s*["']?)((?:bearer|basic|token|digest|negotiate)\s+)?[^"'\r\n,;\[]{4,}`), "${1}${2}[REDACTED]"},
	{regexp.MustCompile(`(?i)\b(bearer|basic|token)(\s+)[A-Za-z0-9._~+/=-]{8,}`), "${1}${2}[REDACTED]"},
	// key=value, key: value and "key": "value" for secret-looking keys.
	{regexp.MustCompile(`(?i)([A-Za-z0-9_.-]*(?:api[_-]?key|apikey|secret|token|passw(?:or)?d|private[_-]?key|access[_-]?key|credentials?)[A-Za-z0-9_.-]*["']?\s*[:=]\s*["']?)([^\s"',;&}\]\[]{4,})`), "${1}[REDACTED]"},
}

// redactSecrets replaces credential-shaped substrings. It also forces valid
// UTF-8: protobuf strings must be, and container output need not be.
func redactSecrets(value string) string {
	value = strings.ToValidUTF8(value, "�")
	for _, secret := range secretPatterns {
		value = secret.pattern.ReplaceAllString(value, secret.replacement)
	}
	return value
}

// boundEvidenceText keeps the newest maxBytes of already redacted text,
// starting at a line boundary when one is available and never inside a rune.
func boundEvidenceText(value string, maxBytes int) (string, bool) {
	if maxBytes <= 0 || len(value) <= maxBytes {
		return value, false
	}
	start := len(value) - maxBytes
	for start < len(value) && !utf8.RuneStart(value[start]) {
		start++
	}
	tail := value[start:]
	if newline := strings.IndexByte(tail, '\n'); newline >= 0 && newline+1 < len(tail) {
		tail = tail[newline+1:]
	}
	return tail, true
}

// evidenceOutput turns one raw TailWorkloadLogs read into a stored tail.
// The first line of a read the runner cut may hold the end of a credential
// whose recognisable start was cut away, so it is dropped before redaction.
func evidenceOutput(data []byte, runnerTruncated bool, maxBytes int) string {
	text := strings.ToValidUTF8(string(data), "�")
	truncated := runnerTruncated
	if runnerTruncated {
		if newline := strings.IndexByte(text, '\n'); newline >= 0 {
			text = text[newline+1:]
		} else if space := strings.IndexAny(text, " \t"); space >= 0 {
			text = text[space+1:]
		} else {
			text = ""
		}
	}
	text, cut := boundEvidenceText(redactSecrets(text), maxBytes)
	truncated = truncated || cut
	if text == "" && !truncated {
		return "[no output]"
	}
	if truncated {
		return evidenceTruncatedMarker + text
	}
	return text
}

func evidenceNote(format string, args ...any) string {
	note, _ := boundEvidenceText(redactSecrets(fmt.Sprintf(format, args...)), evidenceNoteBytes)
	return "[output unavailable: " + note + "]"
}

// boundFailureMessage keeps failure_message short: it is human-readable
// detail, and Runners publishes it with every workload notification.
func boundFailureMessage(message string) string {
	message = redactSecrets(strings.TrimSpace(message))
	if len(message) <= failureMessageBytes {
		return message
	}
	end := failureMessageBytes - len("...")
	for end > 0 && !utf8.RuneStart(message[end]) {
		end--
	}
	return message[:end] + "..."
}

func containerRoleName(role runnersv1.ContainerRole) string {
	switch role {
	case runnersv1.ContainerRole_CONTAINER_ROLE_MAIN:
		return "main"
	case runnersv1.ContainerRole_CONTAINER_ROLE_INIT:
		return "init"
	case runnersv1.ContainerRole_CONTAINER_ROLE_SIDECAR:
		return "sidecar"
	default:
		return "unknown-role"
	}
}

// containerState summarises the status fields a failure decision read.
func containerState(container *runnersv1.Container) string {
	parts := []string{"state=" + strings.TrimPrefix(container.GetStatus().String(), "CONTAINER_STATUS_")}
	if reason := strings.TrimSpace(container.GetReason()); reason != "" {
		parts = append(parts, "reason="+reason)
	}
	if container.ExitCode != nil {
		parts = append(parts, fmt.Sprintf("exit=%d", container.GetExitCode()))
	}
	parts = append(parts, fmt.Sprintf("restarts=%d", container.GetRestartCount()))
	return strings.Join(parts, " ")
}

// containerFailure names the check that failed, the container that failed it
// and the state it was in, so the record says why without the Pod.
func containerFailure(reason runnersv1.WorkloadFailureReason, check, what string, container *runnersv1.Container) *workloadFailure {
	message := fmt.Sprintf("%s: %s container %q %s (%s)", check, containerRoleName(container.GetRole()), container.GetName(), what, containerState(container))
	if detail := strings.TrimSpace(container.GetMessage()); detail != "" {
		message += ": " + detail
	}
	return &workloadFailure{reason: reason, message: boundFailureMessage(message)}
}

// diagnoseFailedContainers explains a failure that was recorded without a
// reason -- the runner reports a Failed Pod phase with no detail -- from the
// containers it left behind. It prefers the container that most plausibly
// caused the failure.
func diagnoseFailedContainers(check string, containers []*runnersv1.Container) *workloadFailure {
	var mainRan bool
	for _, container := range containers {
		if container.GetRole() == runnersv1.ContainerRole_CONTAINER_ROLE_MAIN && container.StartedAt != nil {
			mainRan = true
		}
	}
	for _, container := range containers {
		switch {
		case container == nil:
			continue
		case isConfigInvalidFailure(container):
			return containerFailure(runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CONFIG_INVALID, check, "cannot be created", container)
		case isImagePullFailure(container):
			return containerFailure(runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_IMAGE_PULL_FAILED, check, "cannot pull its image", container)
		case container.GetReason() == crashLoopBackoffFlag:
			return containerFailure(runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CRASHLOOP, check, "is crash looping", container)
		}
	}
	for _, role := range []runnersv1.ContainerRole{runnersv1.ContainerRole_CONTAINER_ROLE_INIT, runnersv1.ContainerRole_CONTAINER_ROLE_MAIN, runnersv1.ContainerRole_CONTAINER_ROLE_SIDECAR} {
		for _, container := range containers {
			if container.GetRole() != role || container.GetStatus() != runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED || container.GetExitCode() == 0 {
				continue
			}
			reason := runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_START_FAILED
			if mainRan {
				reason = runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_RUNTIME_LOST
			}
			return containerFailure(reason, check, "exited", container)
		}
	}
	if len(containers) == 0 {
		return &workloadFailure{reason: runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_RUNTIME_LOST, message: check + ": no container status was available"}
	}
	summary := make([]string, 0, len(containers))
	for _, container := range containers {
		summary = append(summary, fmt.Sprintf("%s %q (%s)", containerRoleName(container.GetRole()), container.GetName(), containerState(container)))
	}
	return &workloadFailure{reason: runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_START_FAILED,
		message: boundFailureMessage(check + ": no container exited with an error; containers: " + strings.Join(summary, "; "))}
}

func evidenceCaptured(w *runnersv1.Workload) bool {
	for _, container := range w.GetContainers() {
		if container.OutputTail != nil {
			return true
		}
	}
	return false
}

// captureFailureEvidence persists the container statuses and their newest
// output of a failed workload; final marks the capture before removal. It is
// best effort: a runner that cannot be read must not keep a failed workload's
// capacity, so each container records why it holds no output instead. A Pod
// that can no longer be read never overwrites evidence already stored. It
// reports true when removal must wait because another loop is capturing.
func (r *Reconciler) captureFailureEvidence(ctx context.Context, runner runnerv1.RunnerServiceClient, w *runnersv1.Workload, observed []*runnersv1.Container, inspectErr error, final bool) bool {
	workloadID := w.GetMeta().GetId()
	if workloadID == "" {
		return false
	}
	capture, busy, previous := r.failures.beginCapture(workloadID, final)
	if !capture {
		return busy
	}
	stored := false
	defer func() { r.failures.endCapture(workloadID, final, stored, previous) }()
	if observed == nil && evidenceCaptured(w) {
		stored = true
		return false
	}
	containers := observed
	if containers == nil {
		containers = w.GetContainers()
	}
	if len(containers) == 0 {
		log.Printf("reconciler: no container evidence for failed workload %s: %s", workloadID, inspectionNote(inspectErr))
		stored = true
		return false
	}
	snapshot := make([]*runnersv1.Container, 0, len(containers))
	for _, container := range containers {
		clone := proto.Clone(container).(*runnersv1.Container)
		if clone.Message != nil {
			clone.Message = stringPtr(redactSecrets(clone.GetMessage()))
		}
		snapshot = append(snapshot, clone)
	}
	budget := r.failures.evidenceLogBytes
	if budget > evidenceWorkloadBytes/len(snapshot) {
		budget = evidenceWorkloadBytes / len(snapshot)
	}
	// Its own deadline: a reconcile cycle near its end must not turn evidence
	// into timeout notes. Removal after it still uses the cycle's context.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), evidenceTimeout)
	defer cancel()
	unsupported := false
	for _, container := range snapshot {
		switch {
		case budget <= 0:
			container.OutputTail = stringPtr("[output not captured: FAILED_WORKLOAD_EVIDENCE_LOG_BYTES is 0]")
		case observed == nil:
			container.OutputTail = stringPtr(evidenceNote("%s", inspectionNote(inspectErr)))
		case unsupported:
			container.OutputTail = stringPtr(evidenceNote("runner does not implement TailWorkloadLogs"))
		default:
			tail, err := r.tailContainerOutput(ctx, runner, workloadID, container, budget)
			if status.Code(err) == codes.Unimplemented {
				unsupported = true
				container.OutputTail = stringPtr(evidenceNote("runner does not implement TailWorkloadLogs"))
				continue
			}
			if err != nil {
				container.OutputTail = stringPtr(evidenceNote("%s: %s", status.Code(err), status.Convert(err).Message()))
				continue
			}
			container.OutputTail = stringPtr(tail)
		}
	}
	summary := evidenceSummary(snapshot)
	response, err := r.runners.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{Id: workloadID, Containers: snapshot})
	if err != nil {
		// Removal still proceeds; the next attempt, if any, captures again.
		log.Printf("reconciler: store failure evidence for workload %s: %v; evidence: %s", workloadID, err, summary)
		return false
	}
	stored = true
	if !evidenceCaptured(response.GetWorkload()) {
		// Runners that predate Container.output_tail drop it silently. Keep the
		// redacted output where an operator can still find it.
		log.Printf("reconciler: Runners did not store output tails for failed workload %s (older Runners); evidence: %s", workloadID, summary)
		for _, container := range snapshot {
			tail, _ := boundEvidenceText(container.GetOutputTail(), 4*1024)
			log.Printf("reconciler: failed workload %s container %q output (redacted, newest %d bytes):\n%s", workloadID, container.GetName(), len(tail), tail)
		}
		return false
	}
	log.Printf("reconciler: stored failure evidence for workload %s: %s", workloadID, summary)
	return false
}

func (r *Reconciler) tailContainerOutput(ctx context.Context, runner runnerv1.RunnerServiceClient, workloadID string, container *runnersv1.Container, budget int) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, evidenceCallLimit)
	defer cancel()
	request := budget + evidenceReadSlack
	if request > MaxFailedWorkloadEvidenceLogBytes {
		request = MaxFailedWorkloadEvidenceLogBytes
	}
	// A restarted container that is running again says why it restarted only
	// in its previous instance; a waiting one is served that instance anyway.
	previous := container.GetStatus() == runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING && container.GetRestartCount() > 0
	response, err := runner.TailWorkloadLogs(ctx, &runnerv1.TailWorkloadLogsRequest{
		WorkloadId:    workloadID,
		ContainerName: container.GetName(),
		MaxBytes:      uint32(request),
		TailLines:     evidenceTailLines,
		Previous:      previous,
	})
	if err != nil {
		return "", err
	}
	if len(response.GetData()) > request {
		return "", status.Error(codes.Internal, "runner exceeded the requested log bound")
	}
	output := evidenceOutput(response.GetData(), response.GetTruncated(), budget)
	if previous {
		output = "[agyn: previous container instance]\n" + output
	}
	return output, nil
}

func inspectionNote(err error) string {
	if err == nil {
		return "the Pod could not be inspected"
	}
	if status.Code(err) == codes.NotFound {
		return "the Pod was already gone"
	}
	return fmt.Sprintf("the Pod could not be inspected: %s", status.Code(err))
}

func evidenceSummary(containers []*runnersv1.Container) string {
	parts := make([]string, 0, len(containers))
	for _, container := range containers {
		output := container.GetOutputTail()
		described := fmt.Sprintf("%d bytes of output", len(output))
		if strings.HasPrefix(output, "[output ") || output == "[no output]" {
			described, _ = boundEvidenceText(output, 160)
		}
		parts = append(parts, fmt.Sprintf("%s %q (%s; %s)", containerRoleName(container.GetRole()), container.GetName(), containerState(container), described))
	}
	return strings.Join(parts, ", ")
}
