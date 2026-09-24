package reconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/volumemigration"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type volumeMigrationConfig struct {
	RegistryAddress, RegistryToken, RunnerAddress, RunnerToken, Barrier string
	Plan                                                                json.RawMessage
}

type volumeMigrationResult struct {
	Stage     string
	PID       int
	Migration json.RawMessage
	Error     string
}

func TestVolumeMigrationProcess(t *testing.T) {
	path := os.Getenv("VOLUME_MIGRATION_CONFIG_FILE")
	if path == "" {
		t.Skip("volume migration subprocess helper")
	}
	if os.Getenv("PREPARED_STACK_TEST") != "trusted-local" || !filepath.IsAbs(path) {
		t.Fatal("explicit trusted local configuration required")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		t.Fatal("private fixture configuration required")
	}
	data, err := os.ReadFile(path)
	var cfg volumeMigrationConfig
	if err != nil || len(data) > 1<<20 || json.Unmarshal(data, &cfg) != nil {
		t.Fatal("invalid migration fixture configuration")
	}
	plan := &runnersv1.BeginVolumeAnchorMigrationRequest{}
	if protojson.Unmarshal(cfg.Plan, plan) != nil {
		t.Fatal("invalid migration plan")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	connect := func(address, header, token string) *grpc.ClientConn {
		host, _, err := net.SplitHostPort(address)
		if err != nil || host != "127.0.0.1" || len(token) != 64 {
			t.Fatal("private loopback transport required")
		}
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(
			func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				return invoke(metadata.AppendToOutgoingContext(ctx, header, token), method, req, reply, cc, opts...)
			}))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	result := volumeMigrationResult{PID: os.Getpid()}
	c := volumemigration.Coordinator{Registry: runnersv1.NewRunnersServiceClient(connect(cfg.RegistryAddress, "x-checked-volume-fixture-token", cfg.RegistryToken)),
		Native: runnerv1.NewRunnerServiceClient(connect(cfg.RunnerAddress, "x-prepared-runner-fixture-token", cfg.RunnerToken)),
		Checkpoint: func(stage string, m *runnersv1.VolumeAnchorMigration) error {
			if stage != cfg.Barrier {
				return nil
			}
			result.Stage, result.Migration = stage, mustMigrationJSON(t, m)
			checkedStackJSON(t, filepath.Join(filepath.Dir(path), "reached.json"), result)
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(20 * time.Millisecond):
					if _, err := os.Stat(filepath.Join(filepath.Dir(path), "release.json")); err == nil {
						return nil
					} else if !os.IsNotExist(err) {
						return err
					}
				}
			}
		}}
	m, err := c.Run(ctx, plan)
	if m != nil {
		result.Migration = mustMigrationJSON(t, m)
	}
	if err != nil {
		result.Error = err.Error()
	}
	checkedStackJSON(t, filepath.Join(filepath.Dir(path), "result.json"), result)
}

func mustMigrationJSON(t *testing.T, m proto.Message) json.RawMessage {
	t.Helper()
	data, err := protojson.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

type volumeMigrationProcess struct {
	process            *checkedStackProcess
	directory, barrier string
}

func startVolumeMigrator(t *testing.T, cfg volumeMigrationConfig) *volumeMigrationProcess {
	t.Helper()
	directory := t.TempDir()
	path := filepath.Join(directory, "migration.json")
	checkedStackJSON(t, path, cfg)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-test.run=^TestVolumeMigrationProcess$", "-test.timeout=3m")
	cmd.Env = []string{"PREPARED_STACK_TEST=trusted-local", "VOLUME_MIGRATION_CONFIG_FILE=" + path}
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return &volumeMigrationProcess{process: startCheckedStackProcess(t, cmd), directory: directory, barrier: cfg.Barrier}
}

func (p *volumeMigrationProcess) read(t *testing.T, ctx context.Context, name string) volumeMigrationResult {
	t.Helper()
	for {
		data, err := os.ReadFile(filepath.Join(p.directory, name))
		if err == nil {
			var r volumeMigrationResult
			if json.Unmarshal(data, &r) != nil || r.PID != p.process.cmd.Process.Pid {
				t.Fatal("migration subprocess receipt mismatch")
			}
			return r
		}
		if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("migration checkpoint deadline expired")
		case <-p.process.done:
			t.Fatal(fmt.Sprintf("migration process exited before %s", name))
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (p *volumeMigrationProcess) finish(t *testing.T, ctx context.Context) *runnersv1.VolumeAnchorMigration {
	t.Helper()
	p.process.wait(t, ctx)
	r := p.read(t, ctx, "result.json")
	m := &runnersv1.VolumeAnchorMigration{}
	if r.Error != "" || protojson.Unmarshal(r.Migration, m) != nil || !m.Complete {
		t.Fatalf("migration did not complete: %s", r.Error)
	}
	return m
}
