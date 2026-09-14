package reconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type checkedControllerConfig struct {
	RegistryAddress, RegistryToken, RunnerAddress string
	RunnerID, OrganizationID, OwnerID, AgentID    string
	ThreadID, DefinitionID, VolumeID              string
	Mode, Barrier                                 string
	Sandbox                                       bool
}

type checkedControllerOperation struct {
	Operation, Code, ID string
	Revision            uint64
	NativeState         string
}

type checkedControllerResult struct {
	Operations     []checkedControllerOperation
	CreatedRecords int
	DeletedSandbox int
	ReconcileError bool
}

type checkedControllerBarrier struct {
	Stage    string
	PID      int
	VolumeID string
	Result   checkedControllerResult
}

// Invoked only by the live parent test as a separate OS process. The registry
// and runner clients are real loopback gRPC; only Agents metadata is a fixture.
func TestCheckedVolumeControllerProcess(t *testing.T) {
	path := os.Getenv("CHECKED_CONTROLLER_CONFIG_FILE")
	if path == "" {
		t.Skip("live controller subprocess helper")
	}
	if os.Getenv("CHECKED_VOLUME_STACK_TEST") != "trusted-local" || !filepath.IsAbs(path) {
		t.Fatal("explicit trusted-local child configuration required")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		t.Fatal("private child configuration required")
	}
	data, err := os.ReadFile(path)
	var cfg checkedControllerConfig
	if err != nil || len(data) > 1<<20 || json.Unmarshal(data, &cfg) != nil {
		t.Fatal("invalid child configuration")
	}
	for _, address := range []string{cfg.RegistryAddress, cfg.RunnerAddress} {
		host, _, err := net.SplitHostPort(address)
		if err != nil || host != "127.0.0.1" {
			t.Fatal("fixture clients must use loopback")
		}
	}
	for _, value := range []string{cfg.RunnerID, cfg.OrganizationID, cfg.OwnerID, cfg.AgentID, cfg.ThreadID, cfg.DefinitionID, cfg.VolumeID} {
		if parsed, err := uuid.Parse(value); err != nil || parsed.String() != value {
			t.Fatal("canonical child fixture identities required")
		}
	}
	if cfg.Barrier != "" && cfg.Barrier != "before-begin" && cfg.Barrier != "after-begin" && cfg.Barrier != "after-native" && cfg.Barrier != "after-confirm" {
		t.Fatal("unsupported fixture barrier")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	directory := filepath.Dir(path)
	var mu sync.Mutex
	result := checkedControllerResult{}
	barrier := func(stage string) error {
		if cfg.Barrier != stage {
			return nil
		}
		mu.Lock()
		snapshot := result
		snapshot.Operations = append([]checkedControllerOperation(nil), result.Operations...)
		mu.Unlock()
		checkedStackJSON(t, filepath.Join(directory, "reached.json"), checkedControllerBarrier{Stage: stage, PID: os.Getpid(), VolumeID: cfg.VolumeID, Result: snapshot})
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			if _, err := os.Stat(filepath.Join(directory, "release.json")); err == nil {
				return nil
			} else if !os.IsNotExist(err) {
				return fmt.Errorf("fixture release receipt unavailable")
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
		}
	}
	registry, err := grpc.NewClient(cfg.RegistryAddress, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			operation := checkedControllerOperation{Operation: method}
			begin, confirm := false, false
			if update, ok := req.(*runnersv1.UpdateVolumeCheckedRequest); ok {
				operation.ID, operation.Revision = update.Id, update.ExpectedRevision
				switch {
				case update.GetBeginRemoval() != nil:
					operation.Operation, begin = "begin", update.Id == cfg.VolumeID
				case update.GetBind() != nil:
					operation.Operation = "bind"
				case update.GetConfirmRemoval() != nil:
					operation.Operation, confirm = "confirm", update.Id == cfg.VolumeID
				case update.GetReopen() != nil:
					operation.Operation = "reopen"
				case update.GetFailProvisioning() != nil:
					operation.Operation = "fail"
				}
			}
			if begin {
				if err := barrier("before-begin"); err != nil {
					return err
				}
			}
			err := invoke(metadata.AppendToOutgoingContext(ctx, "x-checked-volume-fixture-token", cfg.RegistryToken), method, req, reply, cc, opts...)
			operation.Code = status.Code(err).String()
			mu.Lock()
			result.Operations = append(result.Operations, operation)
			mu.Unlock()
			if begin && err == nil {
				if err := barrier("after-begin"); err != nil {
					return err
				}
			}
			if confirm && err == nil {
				return barrier("after-confirm")
			}
			return err
		}))
	if err != nil {
		t.Fatal("child registry client unavailable")
	}
	defer registry.Close()
	native, err := grpc.NewClient(cfg.RunnerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			err := invoke(ctx, method, req, reply, cc, opts...)
			if remove, ok := req.(*runnerv1.RemoveVolumeCheckedRequest); ok {
				operation := checkedControllerOperation{Operation: "native-remove", ID: remove.Expected.VolumeKey, Code: status.Code(err).String()}
				if response, ok := reply.(*runnerv1.RemoveVolumeCheckedResponse); ok {
					operation.NativeState = response.State.String()
				}
				mu.Lock()
				result.Operations = append(result.Operations, operation)
				mu.Unlock()
				if err == nil && remove.Expected.VolumeKey == cfg.VolumeID {
					return barrier("after-native")
				}
			}
			return err
		}))
	if err != nil {
		t.Fatal("child native client unavailable")
	}
	defer native.Close()
	sandbox := &agentsv1.Sandbox{
		Meta: &agentsv1.EntityMeta{Id: cfg.OwnerID}, OrganizationId: cfg.OrganizationID,
		OwnerId: "fixture-sandbox-user", Status: agentsv1.SandboxStatus_SANDBOX_STATUS_TERMINATED,
	}
	agents := &testutil.FakeAgentsClient{
		GetVolumeFunc: func(_ context.Context, req *agentsv1.GetVolumeRequest, _ ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error) {
			if req.Id != cfg.DefinitionID {
				return nil, status.Error(codes.NotFound, "other fixture definition")
			}
			return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{Meta: &agentsv1.EntityMeta{Id: cfg.DefinitionID}, Persistent: true, Ttl: stringPtr("1h")}}, nil
		},
		GetSandboxFunc: func(_ context.Context, req *agentsv1.GetSandboxRequest, _ ...grpc.CallOption) (*agentsv1.GetSandboxResponse, error) {
			if !cfg.Sandbox || req.GetId() != cfg.OwnerID {
				return nil, status.Error(codes.NotFound, "other fixture sandbox")
			}
			return &agentsv1.GetSandboxResponse{Sandbox: sandbox}, nil
		},
		DeleteSandboxFunc: func(_ context.Context, req *agentsv1.DeleteSandboxRequest, _ ...grpc.CallOption) (*agentsv1.DeleteSandboxResponse, error) {
			if !cfg.Sandbox || req.Id != cfg.OwnerID {
				t.Fatal("controller attempted to finalize a different sandbox")
			}
			result.DeletedSandbox++
			return &agentsv1.DeleteSandboxResponse{}, nil
		},
	}
	r := newTestReconciler(Config{
		Agents: agents, Runners: runnersv1.NewRunnersServiceClient(registry),
		RunnerDialer: &fakeRunnerDialer{dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			if id != cfg.RunnerID {
				return nil, fmt.Errorf("controller dialed a different fixture runner")
			}
			return runnerv1.NewRunnerServiceClient(native), nil
		}},
	})
	switch cfg.Mode {
	case "create":
		var records []volumeRecord
		if cfg.Sandbox {
			records, err = r.createSandboxVolumeRecords(ctx, &assembler.SandboxAssembleResult{
				OrganizationID: cfg.OrganizationID,
				Request:        &runnerv1.StartWorkloadRequest{AdditionalProperties: map[string]string{assembler.LabelKeyPrefix + assembler.LabelSandboxID: cfg.OwnerID}},
				PersistentVolumes: []assembler.PersistentVolumeInfo{{
					ID: uuid.MustParse(cfg.DefinitionID), AgentInstanceID: uuid.MustParse(cfg.OwnerID),
					Volume: &agentsv1.Volume{Size: "1Mi"}, Spec: &runnerv1.VolumeSpec{},
				}},
			}, cfg.RunnerID)
		} else {
			records, err = r.createVolumeRecords(ctx, []volumeRecord{{id: cfg.VolumeID, volumeID: cfg.DefinitionID, sizeGB: "0.0009765625"}},
				cfg.RunnerID, AgentInstanceTarget{AgentID: uuid.MustParse(cfg.AgentID), AgentInstanceID: uuid.MustParse(cfg.OwnerID), ThreadID: uuid.MustParse(cfg.ThreadID)}, cfg.OrganizationID)
		}
		result.CreatedRecords = len(records)
	case "volumes":
		err = r.reconcileVolumes(ctx)
	case "cleanup":
		if cfg.Sandbox {
			err = r.reconcileSandbox(ctx, sandbox, time.Now())
		} else {
			err = r.reconcileVolumes(ctx)
		}
	default:
		t.Fatal("unsupported child reconciliation mode")
	}
	result.ReconcileError = err != nil
	checkedStackJSON(t, filepath.Join(directory, "result.json"), result)
}

type checkedControllerProcess struct {
	process   *checkedStackProcess
	directory string
	config    checkedControllerConfig
}

func startCheckedController(t *testing.T, cfg checkedControllerConfig) *checkedControllerProcess {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal("controller test executable unavailable")
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "controller.json")
	checkedStackJSON(t, path, cfg)
	log, err := os.OpenFile(filepath.Join(directory, "process.log"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal("private controller log unavailable")
	}
	t.Cleanup(func() { _ = log.Close() })
	cmd := exec.Command(binary, "-test.run=^TestCheckedVolumeControllerProcess$", "-test.count=1", "-test.timeout=2m")
	cmd.Env = []string{"CHECKED_VOLUME_STACK_TEST=trusted-local", "CHECKED_CONTROLLER_CONFIG_FILE=" + path}
	cmd.Stdout, cmd.Stderr = log, log
	return &checkedControllerProcess{process: startCheckedStackProcess(t, cmd), directory: directory, config: cfg}
}

func (p *checkedControllerProcess) awaitBarrier(t *testing.T, ctx context.Context) checkedControllerResult {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		data, err := os.ReadFile(filepath.Join(p.directory, "reached.json"))
		if err == nil {
			var reached checkedControllerBarrier
			if json.Unmarshal(data, &reached) != nil || reached.Stage != p.config.Barrier || reached.PID != p.process.cmd.Process.Pid || reached.VolumeID != p.config.VolumeID {
				t.Fatal("controller barrier identity mismatch")
			}
			select {
			case <-p.process.done:
				t.Fatal("controller stopped before barrier observation")
			default:
			}
			t.Logf("controller barrier pid=%d stage=%s result=%+v", reached.PID, reached.Stage, reached.Result)
			return reached.Result
		}
		if !os.IsNotExist(err) {
			t.Fatal("controller barrier receipt unavailable")
		}
		select {
		case <-p.process.done:
			t.Fatalf("controller exited before %s barrier", p.config.Barrier)
		case <-ctx.Done():
			t.Fatal("controller barrier deadline expired")
		case <-ticker.C:
		}
	}
}

func (p *checkedControllerProcess) finish(t *testing.T, ctx context.Context) checkedControllerResult {
	t.Helper()
	p.process.wait(t, ctx)
	data, err := os.ReadFile(filepath.Join(p.directory, "result.json"))
	var result checkedControllerResult
	if err != nil || json.Unmarshal(data, &result) != nil {
		t.Fatal("controller result receipt missing")
	}
	t.Logf("controller receipt pid=%d mode=%s barrier=%s result=%+v", p.process.cmd.Process.Pid, p.config.Mode, p.config.Barrier, result)
	return result
}

func (r checkedControllerResult) count(operation, code string) int {
	count := 0
	for _, item := range r.Operations {
		if item.Operation == operation && (code == "" || item.Code == code) {
			count++
		}
	}
	return count
}
