package reconciler

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const checkedStackLabel = "agyn.io/checked-volume-stack"

type checkedRegistryConfig struct {
	DSN               string `json:"dsn"`
	Schema            string `json:"schema"`
	RunID             string `json:"runId"`
	Token             string `json:"token"`
	RunnerID          string `json:"runnerId"`
	OrganizationID    string `json:"organizationId"`
	PreparedWorkloads bool   `json:"preparedWorkloads,omitempty"`
	VolumeMigration   bool   `json:"volumeMigration,omitempty"`
}

type checkedStackProcess struct {
	cmd     *exec.Cmd
	done    chan struct{}
	err     error
	crashed bool
}

func startCheckedStackProcess(t *testing.T, cmd *exec.Cmd) *checkedStackProcess {
	t.Helper()
	if err := cmd.Start(); err != nil {
		t.Fatal("fixture process could not start")
	}
	p := &checkedStackProcess{cmd: cmd, done: make(chan struct{})}
	go func() { p.err = cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		select {
		case <-p.done:
		default:
			_ = p.cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-p.done:
			case <-time.After(15 * time.Second):
				_ = p.cmd.Process.Kill()
				<-p.done
				t.Error("fixture process required forced cleanup")
			}
		}
		if p.err != nil && !p.crashed {
			t.Errorf("fixture process exited unsuccessfully: %v", p.err)
		}
	})
	return p
}

func (p *checkedStackProcess) wait(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-p.done:
	case <-ctx.Done():
		t.Fatal("fixture process did not finish before deadline")
	}
	if p.err != nil {
		t.Fatalf("fixture process exited unsuccessfully: %v", p.err)
	}
}

func (p *checkedStackProcess) kill(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
		t.Fatal("fixture process was not alive at the crash barrier")
	default:
	}
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatal("fixture crash signal failed")
	}
	select {
	case <-p.done:
	case <-time.After(15 * time.Second):
		t.Fatal("fixture process death was not confirmed")
	}
	state, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !state.Signaled() || state.Signal() != syscall.SIGKILL {
		t.Fatal("fixture process did not terminate by SIGKILL")
	}
	p.crashed = true
	t.Logf("confirmed fixture process SIGKILL pid=%d", p.cmd.Process.Pid)
}

func checkedStackJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal("fixture JSON encoding failed")
	}
	file, err := os.OpenFile(path+".tmp", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal("fixture file creation failed")
	}
	_, writeErr := file.Write(data)
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatal("fixture file write failed")
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		t.Fatal("fixture file publication failed")
	}
}

func checkedStackSecret(t *testing.T) string {
	t.Helper()
	data := make([]byte, 32)
	if _, err := rand.Read(data); err != nil {
		t.Fatal("fixture randomness unavailable")
	}
	return hex.EncodeToString(data)
}

func checkedStackDocker(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	// Never include command output/arguments in errors: the database bootstrap
	// uses a private env file, and arbitrary Docker metadata can contain secrets.
	data, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("fixture Docker %s failed", args[0])
	}
	return data, nil
}

type checkedStackDatabase struct {
	config checkedRegistryConfig
	id     string
	name   string
}

type checkedContainerReceipt struct {
	ID      string
	Labels  map[string]string
	Tmpfs   map[string]string
	Mounts  []struct{ Type, Destination string }
	Ports   map[string][]struct{ HostIP, HostPort string }
	Running bool
}

func (d *checkedStackDatabase) inspect(ctx context.Context) (*checkedContainerReceipt, error) {
	data, err := checkedStackDocker(ctx, "inspect", d.id, "--format",
		`{"id":{{json .Id}},"labels":{{json .Config.Labels}},"tmpfs":{{json .HostConfig.Tmpfs}},"mounts":{{json .Mounts}},"ports":{{json .NetworkSettings.Ports}},"running":{{json .State.Running}}}`)
	if err != nil {
		return nil, err
	}
	var receipt checkedContainerReceipt
	if err := json.Unmarshal(data, &receipt); err != nil || receipt.ID != d.id || receipt.Labels[checkedStackLabel] != d.config.RunID {
		return nil, fmt.Errorf("database container ownership not confirmed")
	}
	if receipt.Tmpfs["/var/lib/postgresql/data"] == "" {
		return nil, fmt.Errorf("database must use memory-backed fixture storage")
	}
	for _, mount := range receipt.Mounts {
		if mount.Type != "tmpfs" {
			return nil, fmt.Errorf("database acquired external backing storage")
		}
	}
	return &receipt, nil
}

func newCheckedStackDatabase(t *testing.T, ctx context.Context, image string) *checkedStackDatabase {
	t.Helper()
	if !regexp.MustCompile(`^[^\s]+@sha256:[a-f0-9]{64}$`).MatchString(image) {
		t.Fatal("explicit digest-pinned PostgreSQL fixture image required")
	}
	if host := os.Getenv("DOCKER_HOST"); host != "" && !strings.HasPrefix(host, "unix://") {
		t.Fatal("fixture requires a local Unix-socket Docker daemon")
	}
	endpoint, err := checkedStackDocker(ctx, "context", "inspect", "--format", "{{.Endpoints.docker.Host}}")
	if err != nil || !strings.HasPrefix(strings.TrimSpace(string(endpoint)), "unix://") {
		t.Fatal("fixture Docker context must use a local Unix socket")
	}
	if _, err := checkedStackDocker(ctx, "image", "inspect", image, "--format", "{{.Id}}"); err != nil {
		t.Fatal("reviewed PostgreSQL fixture image must already exist locally")
	}
	runID := uuid.New()
	d := &checkedStackDatabase{config: checkedRegistryConfig{
		RunID: runID.String(), Schema: "checked_" + hex.EncodeToString(runID[:]),
		Token: checkedStackSecret(t), RunnerID: uuid.NewString(), OrganizationID: testOrganizationID,
	}}
	d.name = "agyn-checked-volume-" + runID.String()
	password := checkedStackSecret(t)
	envPath := filepath.Join(t.TempDir(), "postgres.env")
	if err := os.WriteFile(envPath, []byte("POSTGRES_USER=postgres\nPOSTGRES_DB=runners_controller_acceptance\nPOSTGRES_PASSWORD="+password+"\n"), 0600); err != nil {
		t.Fatal("private database bootstrap file unavailable")
	}
	// Register cleanup before the possibly ambiguous Docker create reply. An
	// exact name plus run label is still required before any removal attempt.
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if d.id == "" {
			data, err := checkedStackDocker(cleanup, "ps", "-aq", "--no-trunc", "--filter", "name=^/"+d.name+"$")
			if err != nil {
				t.Error("database creation outcome unconfirmed; cleanup inventory failed")
				return
			}
			if strings.TrimSpace(string(data)) == "" {
				t.Log("no matching database container observed after unsuccessful creation")
				return
			}
			d.id = strings.TrimSpace(string(data))
		}
		if !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(d.id) {
			t.Error("database creation identity ambiguous; cleanup refused")
			return
		}
		if _, err := d.inspect(cleanup); err != nil {
			t.Error("database ownership/backing storage unconfirmed; cleanup refused")
			return
		}
		if _, err := checkedStackDocker(cleanup, "rm", "-f", d.id); err != nil {
			t.Error("owned database container cleanup failed")
			return
		}
		data, err := checkedStackDocker(cleanup, "ps", "-aq", "--filter", "id="+d.id)
		if err != nil || strings.TrimSpace(string(data)) != "" {
			t.Error("database container absence unconfirmed")
			return
		}
		t.Logf("confirmed disposable PostgreSQL container absent id=%s run=%s", d.id, d.config.RunID)
	})
	data, err := checkedStackDocker(ctx, "run", "-d", "--pull=never", "--name", d.name, "--label", checkedStackLabel+"="+d.config.RunID,
		"--cpus=1", "--memory=512m", "--memory-swap=512m", "--pids-limit=128",
		"--tmpfs", "/var/lib/postgresql/data:rw,size=256m", "--publish", "127.0.0.1::5432", "--env-file", envPath, image)
	if err != nil {
		t.Fatal(err)
	}
	id := strings.TrimSpace(string(data))
	if !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(id) {
		t.Fatal("database creation receipt missing")
	}
	d.id = id
	receipt, err := d.inspect(ctx)
	if err != nil || !receipt.Running || len(receipt.Ports["5432/tcp"]) != 1 || len(receipt.Ports) != 1 || receipt.Ports["5432/tcp"][0].HostIP != "127.0.0.1" {
		t.Fatal("database must expose exactly one loopback PostgreSQL port")
	}
	address := net.JoinHostPort("127.0.0.1", receipt.Ports["5432/tcp"][0].HostPort)
	d.config.DSN = (&url.URL{Scheme: "postgres", User: url.UserPassword("postgres", password), Host: address,
		Path: "/runners_controller_acceptance", RawQuery: "sslmode=disable"}).String()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	for {
		if _, err := checkedStackDocker(ctx, "exec", d.id, "pg_isready", "-h", "127.0.0.1", "-U", "postgres", "-d", "runners_controller_acceptance"); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("database startup deadline expired")
		case <-deadline.C:
			t.Fatal("database did not become ready")
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Logf("disposable PostgreSQL ready id=%s run=%s schema=%s", d.id, d.config.RunID, d.config.Schema)
	return d
}

type checkedRegistryProcess struct {
	process *checkedStackProcess
	address string
	client  runnersv1.RunnersServiceClient
}

func startCheckedRegistry(t *testing.T, ctx context.Context, binary string, cfg checkedRegistryConfig) *checkedRegistryProcess {
	t.Helper()
	path := filepath.Join(t.TempDir(), "registry.json")
	checkedStackJSON(t, path, cfg)
	cmd := exec.Command(binary)
	cmd.Env = []string{"CHECKED_VOLUME_STACK_TEST=trusted-local", "CHECKED_REGISTRY_CONFIG_FILE=" + path}
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal("registry readiness pipe unavailable")
	}
	p := startCheckedStackProcess(t, cmd)
	line := make(chan []byte, 1)
	go func() {
		reader := bufio.NewReaderSize(stdout, 4096)
		data, prefix, err := reader.ReadLine()
		if err != nil || prefix {
			line <- nil
			return
		}
		line <- data
	}()
	var ready struct{ Address, Schema, RunID, RunnerID string }
	select {
	case data := <-line:
		if json.Unmarshal(data, &ready) != nil {
			t.Fatal("registry readiness receipt missing")
		}
	case <-p.done:
		t.Fatal("registry exited before readiness")
	case <-ctx.Done():
		t.Fatal("registry readiness deadline expired")
	}
	host, _, err := net.SplitHostPort(ready.Address)
	if err != nil || host != "127.0.0.1" || ready.Schema != cfg.Schema || ready.RunID != cfg.RunID || ready.RunnerID != cfg.RunnerID {
		t.Fatal("registry readiness identity mismatch")
	}
	conn, err := grpc.NewClient(ready.Address, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			return invoke(metadata.AppendToOutgoingContext(ctx, "x-checked-volume-fixture-token", cfg.Token), method, req, reply, cc, opts...)
		}))
	if err != nil {
		t.Fatal("registry client construction failed")
	}
	t.Cleanup(func() { _ = conn.Close() })
	t.Logf("real registry ready pid=%d schema=%s", p.cmd.Process.Pid, cfg.Schema)
	return &checkedRegistryProcess{process: p, address: ready.Address, client: runnersv1.NewRunnersServiceClient(conn)}
}

func (d *checkedStackDatabase) assertVolume(t *testing.T, ctx context.Context, client runnersv1.RunnersServiceClient, id string) *runnersv1.Volume {
	t.Helper()
	if _, err := d.inspect(ctx); err != nil {
		t.Fatal("independent database reader requires the owned container")
	}
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id || !regexp.MustCompile(`^checked_[a-f0-9]{32}$`).MatchString(d.config.Schema) {
		t.Fatal("database probe requires canonical fixture identities")
	}
	// psql is an independent connection, not the server's response or pool.
	query := `SELECT json_build_object('status',status,'revision',lifecycle_revision,'bound',bound_instance,'intent',removal_intent,'owner',owner_id,'runner',runner_id,'organization',organization_id,'anchor',resource_anchor,'reservation',anchor_reservation,'absence',anchored_removal_observation,'adoption',to_jsonb(volumes)->'anchor_adoption')::text FROM "` + d.config.Schema + `".volumes WHERE id = '` + id + `'`
	data, err := checkedStackDocker(ctx, "exec", d.id, "psql", "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "runners_controller_acceptance", "-c", query)
	if err != nil {
		t.Fatal("independent volume read failed")
	}
	var stored struct {
		Status, Owner, Runner, Organization string
		Revision                            uint64
		Bound, Intent                       json.RawMessage
		Anchor, Reservation, Absence        json.RawMessage
		Adoption                            json.RawMessage
	}
	if json.Unmarshal(data, &stored) != nil {
		t.Fatal("independent volume read returned invalid JSON")
	}
	resp, err := client.GetVolume(ctx, &runnersv1.GetVolumeRequest{Id: id})
	if err != nil || validateCheckedVolume(resp.GetVolume()) != nil {
		t.Fatalf("read checked volume: %v", err)
	}
	v := resp.Volume
	if stored.Status != strings.ToLower(strings.TrimPrefix(v.Status.String(), "VOLUME_STATUS_")) || stored.Revision != v.LifecycleRevision ||
		stored.Owner != v.OwnerId || stored.Runner != v.RunnerId || stored.Organization != v.OrganizationId {
		t.Fatal("registry response differs from independently persisted identity/revision/status")
	}
	for _, value := range []struct {
		raw     json.RawMessage
		message proto.Message
	}{
		{stored.Bound, v.BoundInstance}, {stored.Intent, v.RemovalIntent},
		{stored.Anchor, v.ResourceAnchor}, {stored.Reservation, v.AnchorReservation}, {stored.Absence, v.AnchoredRemovalObservation},
		{stored.Adoption, v.AnchorAdoption},
	} {
		if string(value.raw) == "null" {
			if value.message.ProtoReflect().IsValid() {
				t.Fatal("registry response invented an unpersisted binding/intent")
			}
			continue
		}
		expected := value.message.ProtoReflect().New().Interface()
		if err := protojson.Unmarshal(value.raw, expected); err != nil || !proto.Equal(expected, value.message) {
			t.Fatal("registry binding/intent differs from the independent database read")
		}
	}
	return v
}
