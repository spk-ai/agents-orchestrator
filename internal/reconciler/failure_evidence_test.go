package reconciler

import (
	"strings"
	"testing"
	"unicode/utf8"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Fixtures are synthetic: shaped like real credentials, valid for nothing.
const (
	testJWT        = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ3b3JrbG9hZCIsImp0aSI6IjEyMyJ9.c2lnbmF0dXJlLWJ5dGVzLWZvci10ZXN0"
	testOpenAIKey  = "sk-proj-AbCdEfGhIjKlMnOpQrStUvWxYz0123456789"
	testAnthropic  = "sk-ant-api03-AbCdEfGhIjKlMnOpQrStUvWx-0123456789"
	testGitHub     = "ghp_AbCdEfGhIjKlMnOpQrStUvWxYz0123456789"
	testSlack      = "xoxb-123456789012-123456789012-AbCdEfGhIjKl"
	testAWS        = "AKIAABCDEFGHIJKLMNOP"
	testGoogle     = "AIzaSyAbCdEfGhIjKlMnOpQrStUvWxYz012345678"
	testDoppler    = "dp.st.dev.AbCdEfGhIjKlMnOpQrStUvWxYz0123"
	testOpaqueAuth = "Zm9vYmFyYmF6cXV4cXV1eA"
)

func TestRedactSecretsRemovesCredentialShapes(t *testing.T) {
	for _, test := range []struct {
		name   string
		input  string
		secret string
		marker string
	}{
		{name: "jwt", input: "enroll with " + testJWT + " now", secret: testJWT, marker: "[REDACTED:jwt]"},
		{name: "jwt in json", input: `{"jwt":"` + testJWT + `"}`, secret: testJWT},
		{name: "bearer", input: "Authorization: Bearer " + testOpaqueAuth, secret: testOpaqueAuth, marker: "Authorization: Bearer [REDACTED]"},
		{name: "custom scheme", input: `"authorization": "Custom ` + testOpaqueAuth + `"`, secret: testOpaqueAuth},
		{name: "bare header", input: "Proxy-Authorization: " + testOpaqueAuth, secret: testOpaqueAuth, marker: "Proxy-Authorization: [REDACTED]"},
		{name: "bearer in text", input: "retrying with bearer " + testOpaqueAuth, secret: testOpaqueAuth, marker: "bearer [REDACTED]"},
		{name: "openai", input: "OPENAI_API_KEY " + testOpenAIKey, secret: testOpenAIKey, marker: "[REDACTED:api-key]"},
		{name: "anthropic", input: "using key " + testAnthropic, secret: testAnthropic, marker: "[REDACTED:api-key]"},
		{name: "github", input: "token=" + testGitHub, secret: testGitHub},
		{name: "slack", input: "slack " + testSlack, secret: testSlack, marker: "[REDACTED:api-key]"},
		{name: "aws", input: "aws " + testAWS + " key", secret: testAWS, marker: "[REDACTED:api-key]"},
		{name: "google", input: "google " + testGoogle, secret: testGoogle, marker: "[REDACTED:api-key]"},
		{name: "doppler", input: "DOPPLER_TOKEN=" + testDoppler, secret: testDoppler},
		{name: "url credentials", input: "dial https://agent:" + testOpaqueAuth + "@proxy.local/v1", secret: testOpaqueAuth, marker: "agent:[REDACTED]@"},
		{name: "key value", input: "client_secret=" + testOpaqueAuth + " ok", secret: testOpaqueAuth, marker: "client_secret=[REDACTED]"},
		{name: "json key", input: `{"api_key": "` + testOpaqueAuth + `"}`, secret: testOpaqueAuth, marker: `"api_key": "[REDACTED]`},
		{name: "password", input: "PASSWORD: " + testOpaqueAuth, secret: testOpaqueAuth},
		{name: "private key", input: "-----BEGIN RSA PRIVATE KEY-----\nMIIEow" + testOpaqueAuth + "\n-----END RSA PRIVATE KEY-----\ntrailer", secret: testOpaqueAuth, marker: "[REDACTED:private-key]\ntrailer"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := redactSecrets(test.input)
			if strings.Contains(got, test.secret) {
				t.Fatalf("secret survived redaction: %q", got)
			}
			if test.marker != "" && !strings.Contains(got, test.marker) {
				t.Fatalf("expected %q in %q", test.marker, got)
			}
		})
	}
}

func TestRedactSecretsKeepsOrdinaryOutput(t *testing.T) {
	for _, line := range []string{
		"agynd: listening on 127.0.0.1:8080",
		"workload 9ab8ade8-8c1e-4f0e-9b7a-1f2e3d4c5b6a started",
		"Error: Cannot find module '@modelcontextprotocol/server-memory'",
		"panic: runtime error: index out of range [3] with length 3",
		"ziti: dial tcp ziti-controller-client.ziti.svc.cluster.local:443: connect: connection refused",
		"exit status 1",
	} {
		if got := redactSecrets(line); got != line {
			t.Fatalf("ordinary output changed:\n got %q\nwant %q", got, line)
		}
	}
}

func TestRedactSecretsForcesValidUTF8(t *testing.T) {
	got := redactSecrets("ok \xff\xfe bytes")
	if !utf8.ValidString(got) || !strings.Contains(got, "ok") {
		t.Fatalf("invalid output %q", got)
	}
}

func TestEvidenceOutputKeepsNewestBoundedOutput(t *testing.T) {
	var lines []string
	for i := 0; i < 400; i++ {
		lines = append(lines, strings.Repeat("x", 20)+" line "+string(rune('a'+i%26)))
	}
	lines = append(lines, "final error: boom")
	raw := strings.Join(lines, "\n") + "\n"
	const budget = 1024
	got := evidenceOutput([]byte(raw), false, budget, "")
	if !strings.HasPrefix(got, evidenceTruncatedMarker) {
		t.Fatalf("truncation not marked: %q", got[:40])
	}
	if len(got) > budget {
		t.Fatalf("stored %d bytes, budget %d", len(got), budget)
	}
	body := strings.TrimPrefix(got, evidenceTruncatedMarker)
	if !strings.HasSuffix(body, "final error: boom\n") {
		t.Fatalf("newest output lost: %q", body[len(body)-40:])
	}
	if !strings.HasPrefix(body, strings.Repeat("x", 20)) {
		t.Fatalf("kept output must start at a line boundary: %q", body[:30])
	}
}

func TestEvidenceOutputSmallAndEmpty(t *testing.T) {
	if got := evidenceOutput([]byte("hello\n"), false, 1024, ""); got != "hello\n" {
		t.Fatalf("got %q", got)
	}
	if got := evidenceOutput(nil, false, 1024, ""); got != "[no output]" {
		t.Fatalf("got %q", got)
	}
	if got := evidenceOutput([]byte("hello\n"), false, 1024, evidencePreviousHeader); got != evidencePreviousHeader+"hello\n" {
		t.Fatalf("got %q", got)
	}
}

// The stored value, markers included, never exceeds the bound Runners
// enforces per container, whatever the budget.
func TestEvidenceOutputBoundIncludesMarkers(t *testing.T) {
	line := strings.Repeat("y", 99) + "\n"
	small := []byte(strings.Repeat(line, 40))
	for _, budget := range []int{1, 8, 30, 40, 64, 80, 1024} {
		for _, header := range []string{"", evidencePreviousHeader} {
			for _, data := range [][]byte{small, []byte("short\n"), nil} {
				for _, cut := range []bool{false, true} {
					got := evidenceOutput(data, cut, budget, header)
					if len(got) > budget || !utf8.ValidString(got) {
						t.Fatalf("budget %d header %q cut %v: stored %d bytes", budget, header, cut, len(got))
					}
				}
			}
		}
	}
	// At Runners' own bound: a full body plus both markers still fits.
	big := []byte(strings.Repeat(line, MaxFailedWorkloadEvidenceLogBytes/len(line)+10))
	got := evidenceOutput(big, true, MaxFailedWorkloadEvidenceLogBytes, evidencePreviousHeader)
	if len(got) > MaxFailedWorkloadEvidenceLogBytes || !strings.HasPrefix(got, evidencePreviousHeader+evidenceTruncatedMarker) || !strings.HasSuffix(got, "y\n") {
		t.Fatalf("markers or newest output lost: %q ... %q", got[:80], got[len(got)-10:])
	}
}

// A runner-side cut can start inside a credential whose recognisable prefix
// was cut away. That first partial line is dropped before anything is kept.
func TestEvidenceOutputDropsLineCutByRunner(t *testing.T) {
	partial := testJWT[10:]
	raw := partial + " trailing words\nnext line\n"
	got := evidenceOutput([]byte(raw), true, 1024, "")
	if strings.Contains(got, partial[len(partial)-20:]) {
		t.Fatalf("partial credential kept: %q", got)
	}
	if got != evidenceTruncatedMarker+"next line\n" {
		t.Fatalf("got %q", got)
	}
	// Without any newline the leading partial word is dropped instead.
	got = evidenceOutput([]byte(partial+" tail"), true, 1024, "")
	if strings.Contains(got, partial[len(partial)-20:]) || !strings.HasSuffix(got, "tail") {
		t.Fatalf("got %q", got)
	}
}

// Redaction runs on the whole read before the stored bound is applied, so a
// credential straddling the bound cannot leave a recognisable fragment.
func TestEvidenceOutputRedactsBeforeBounding(t *testing.T) {
	raw := "start\n" + strings.Repeat("a", 30) + " key " + testJWT + " end\nlast\n"
	for budget := 8; budget < len(raw); budget++ {
		got := evidenceOutput([]byte(raw), false, budget, "")
		for i := 0; i+12 <= len(testJWT); i += 4 {
			if strings.Contains(got, testJWT[i:i+12]) {
				t.Fatalf("budget %d leaked a credential fragment: %q", budget, got)
			}
		}
		if len(got) > budget {
			t.Fatalf("budget %d exceeded: %q", budget, got)
		}
	}
}

func TestBoundEvidenceTextNeverSplitsRunes(t *testing.T) {
	text := strings.Repeat("é", 100)
	for budget := 1; budget < 40; budget++ {
		got, cut := boundEvidenceText(text, budget)
		if !cut || !utf8.ValidString(got) || len(got) > budget {
			t.Fatalf("budget %d: %q cut=%v", budget, got, cut)
		}
	}
}

func TestBoundFailureMessageIsShortAndRedacted(t *testing.T) {
	message := boundFailureMessage("start check: token=" + testOpaqueAuth + " " + strings.Repeat("é", 4000))
	if len(message) > failureMessageBytes || !utf8.ValidString(message) || strings.Contains(message, testOpaqueAuth) || !strings.HasSuffix(message, "...") {
		t.Fatalf("unbounded or unredacted message (%d bytes)", len(message))
	}
}

func TestDiagnoseFailedContainersNamesTheFailingCheck(t *testing.T) {
	exit := func(code int32) *int32 { return &code }
	started := timestamppb.Now()
	for _, test := range []struct {
		name       string
		containers []*runnersv1.Container
		reason     runnersv1.WorkloadFailureReason
		contains   []string
	}{
		{
			name: "init exited",
			containers: []*runnersv1.Container{
				{Name: "ziti-enroll", Role: runnersv1.ContainerRole_CONTAINER_ROLE_INIT, Status: runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED, Reason: stringPtr("Error"), ExitCode: exit(1), Message: stringPtr("enroll: " + testJWT)},
				{Name: "main", Role: runnersv1.ContainerRole_CONTAINER_ROLE_MAIN, Status: runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING, Reason: stringPtr("PodInitializing")},
			},
			reason:   runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_START_FAILED,
			contains: []string{"runner report", `init container "ziti-enroll" exited`, "exit=1", "[REDACTED:jwt]"},
		},
		{
			name: "main exited after running",
			containers: []*runnersv1.Container{
				{Name: "main", Role: runnersv1.ContainerRole_CONTAINER_ROLE_MAIN, Status: runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED, ExitCode: exit(137), Reason: stringPtr("OOMKilled"), StartedAt: started},
			},
			reason:   runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_RUNTIME_LOST,
			contains: []string{`main container "main" exited`, "reason=OOMKilled", "exit=137"},
		},
		{
			name: "mcp image",
			containers: []*runnersv1.Container{
				{Name: "mcp-memory", Role: runnersv1.ContainerRole_CONTAINER_ROLE_SIDECAR, Status: runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING, Reason: stringPtr("ImagePullBackOff")},
			},
			reason:   runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_IMAGE_PULL_FAILED,
			contains: []string{`sidecar container "mcp-memory" cannot pull its image`},
		},
		{
			name: "nothing exited",
			containers: []*runnersv1.Container{
				{Name: "main", Role: runnersv1.ContainerRole_CONTAINER_ROLE_MAIN, Status: runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING, Reason: stringPtr("PodInitializing")},
			},
			reason:   runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_START_FAILED,
			contains: []string{"no container exited with an error", `main "main" (state=WAITING reason=PodInitializing restarts=0)`},
		},
		{
			name:     "no statuses",
			reason:   runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_RUNTIME_LOST,
			contains: []string{"no container status was available"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			failure := diagnoseFailedContainers("runner report", test.containers)
			if failure.reason != test.reason {
				t.Fatalf("reason %s, want %s", failure.reason, test.reason)
			}
			for _, part := range test.contains {
				if !strings.Contains(failure.message, part) {
					t.Fatalf("%q missing from %q", part, failure.message)
				}
			}
			if strings.Contains(failure.message, testJWT) {
				t.Fatalf("credential in failure message: %q", failure.message)
			}
		})
	}
}
