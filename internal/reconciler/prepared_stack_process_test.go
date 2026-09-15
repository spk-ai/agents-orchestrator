package reconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
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
	"google.golang.org/protobuf/proto"
)

const preparedStackLabel = "agyn.io/prepared-controller-test"

type preparedControllerConfig struct {
	RegistryAddress, RegistryToken, RunnerAddress, RunnerToken string
	RunnerID, OrganizationID, OwnerID, AgentID, ThreadID       string
	HumanOwnerID                                               string
	DefinitionID, WorkloadID, RunID, Image, Mode, Barrier      string
	Sandbox                                                    bool
	Turn                                                       int
}

type preparedControllerResult struct {
	Operations []checkedControllerOperation
	Workload   *runnersv1.Workload
	Binding    *runnerv1.WorkloadBinding
	Volume     *runnersv1.Volume
	Error      bool
	ErrorCode  string
}

type preparedControllerBarrier struct {
	Stage, WorkloadID string
	PID               int
	Result            preparedControllerResult
}

func (c preparedControllerConfig) request() (*runnersv1.CreateWorkloadRequest, *runnerv1.StartWorkloadRequest, []assembler.PersistentVolumeInfo) {
	labels := map[string]string{preparedStackLabel: c.RunID, assembler.LabelManagedBy: assembler.ManagedByValue}
	additional := map[string]string{}
	kind := runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE
	agentID, threadID := c.AgentID, c.ThreadID
	if c.Sandbox {
		kind, agentID, threadID = runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX, "", ""
		labels[assembler.LabelSandboxID], labels[assembler.LabelSandboxOwnerID] = c.OwnerID, c.HumanOwnerID
		additional[assembler.LabelKeyPrefix+assembler.LabelSandboxID] = c.OwnerID
	} else {
		labels[assembler.LabelInstanceID], labels[assembler.LabelAgentID], labels[assembler.LabelThreadID] = c.OwnerID, agentID, threadID
	}
	info := assembler.PersistentVolumeInfo{ID: uuid.MustParse(c.DefinitionID), AgentInstanceID: uuid.MustParse(c.OwnerID),
		Volume: &agentsv1.Volume{Size: "1Mi", Persistent: true},
		Spec:   &runnerv1.VolumeSpec{Name: "workspace", Kind: runnerv1.VolumeKind_VOLUME_KIND_NAMED, PersistentName: "workspace-" + c.OwnerID, Size: "1Mi"}}
	// Fixed, credential-free workload. Every turn verifies all earlier effects
	// before appending once; heartbeats let another task prove forward progress.
	program := fmt.Sprintf(`const fs=require('fs'); const owner=%q, turn=%d;
if(fs.readFileSync('/fixture-marker','utf8')!==%q) process.exit(70);
const marker='/workspace/owner', path='/workspace/turns';
if(turn===1) { if(fs.existsSync(marker)||fs.existsSync(path)) process.exit(71); fs.writeFileSync(marker,owner,{flag:'wx'}); }
if(fs.readFileSync(marker,'utf8')!==owner) process.exit(72);
const prior=turn===1?'':fs.readFileSync(path,'utf8');
const expected=Array.from({length:turn-1},(_,i)=>owner+':'+(i+1)+'\n').join('');
if(prior!==expected) process.exit(73);
fs.appendFileSync(path,owner+':'+turn+'\n');
let heartbeat=0; function report(){ console.log(JSON.stringify({owner,turn,entries:fs.readFileSync(path,'utf8'),heartbeat:++heartbeat})); }
report(); setInterval(report,250); process.on('SIGTERM',()=>process.exit(0)); setTimeout(()=>process.exit(74),180000);`, c.OwnerID, c.Turn, c.RunID)
	return &runnersv1.CreateWorkloadRequest{Id: c.WorkloadID, RunnerId: c.RunnerID, OrganizationId: c.OrganizationID,
			OwnerKind: kind, OwnerId: c.OwnerID, AgentId: agentID, ThreadId: preparedStackRegistryThread(c), Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING},
		&runnerv1.StartWorkloadRequest{WorkloadId: c.WorkloadID, Labels: labels,
			AdditionalProperties: additional,
			Capabilities:         []string{"compute-resources"},
			Main: &runnerv1.ContainerSpec{Name: "main", Image: c.Image, Entrypoint: "node", Cmd: []string{"-e", program},
				Mounts:           []*runnerv1.VolumeMount{{Volume: "workspace", MountPath: "/workspace"}},
				InlineFileMounts: []*runnerv1.InlineFileMount{{Path: "/fixture-marker"}},
				Resources:        &runnerv1.ComputeResources{RequestsCpu: "50m", RequestsMemory: "64Mi", LimitsCpu: "250m", LimitsMemory: "128Mi"}},
			InlineFiles: map[string][]byte{"/fixture-marker": []byte(c.RunID)}, Volumes: []*runnerv1.VolumeSpec{info.Spec}},
		[]assembler.PersistentVolumeInfo{info}
}

func preparedStackRegistryThread(c preparedControllerConfig) string {
	if c.Sandbox {
		return ""
	}
	return c.OwnerID
}

// A real OS child invokes the production shared lifecycle with real registry
// and runner gRPC clients. Agent assembly/model execution/overlay auth are not
// under test here; neither a registry response nor a native receipt is faked.
func TestPreparedControllerProcess(t *testing.T) {
	path := os.Getenv("PREPARED_CONTROLLER_CONFIG_FILE")
	if path == "" {
		t.Skip("live prepared controller subprocess helper")
	}
	if os.Getenv("PREPARED_STACK_TEST") != "trusted-local" || !filepath.IsAbs(path) {
		t.Fatal("explicit trusted-local child configuration required")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		t.Fatal("private child configuration required")
	}
	data, err := os.ReadFile(path)
	var cfg preparedControllerConfig
	if err != nil || len(data) > 1<<20 || json.Unmarshal(data, &cfg) != nil {
		t.Fatal("invalid child configuration")
	}
	for _, address := range []string{cfg.RegistryAddress, cfg.RunnerAddress} {
		host, _, err := net.SplitHostPort(address)
		if err != nil || host != "127.0.0.1" {
			t.Fatal("fixture clients must use loopback")
		}
	}
	for _, id := range []string{cfg.RunnerID, cfg.OrganizationID, cfg.OwnerID, cfg.AgentID, cfg.ThreadID, cfg.DefinitionID, cfg.WorkloadID, cfg.RunID, cfg.HumanOwnerID} {
		if !preparedUUID(id) {
			t.Fatal("canonical child fixture identities required")
		}
	}
	if !regexp.MustCompile(`^[^\s]+@sha256:[a-f0-9]{64}$`).MatchString(cfg.Image) || cfg.Turn < 1 || cfg.Turn > 3 {
		t.Fatal("bounded pinned probe required")
	}
	if !slices.Contains([]string{"", "reserved", "anchors-bound", "preparing", "prepared", "bound", "activating", "activated", "active", "removing", "native-absent", "anchor-pending", "anchor-absent", "removed", "observed", "recovery-volume", "recovered-binding", "volume-intent", "volume-pending", "volume-absent", "volume-confirmed"}, cfg.Barrier) {
		t.Fatal("unsupported fixture barrier")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	result := preparedControllerResult{}
	directory := filepath.Dir(path)
	barrier := func(stage string) error {
		if stage == "" || cfg.Barrier != stage {
			return nil
		}
		checkedStackJSON(t, filepath.Join(directory, "reached.json"), preparedControllerBarrier{Stage: stage, PID: os.Getpid(), WorkloadID: cfg.WorkloadID, Result: result})
		for {
			if _, err := os.Stat(filepath.Join(directory, "release.json")); err == nil {
				return nil
			} else if !os.IsNotExist(err) {
				return fmt.Errorf("fixture release receipt unavailable")
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	nativePending := false
	connect := func(address, header, token string) *grpc.ClientConn {
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(
			func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				err := invoke(metadata.AppendToOutgoingContext(ctx, header, token), method, req, reply, cc, opts...)
				op := checkedControllerOperation{Operation: method, Code: status.Code(err).String()}
				stage := ""
				if err == nil {
					switch response := reply.(type) {
					case *runnersv1.CreateAnchoredWorkloadResponse:
						result.Workload, stage = proto.Clone(response.Workload).(*runnersv1.Workload), "reserved"
					case *runnersv1.BindWorkloadResourceAnchorsResponse:
						result.Workload, stage = proto.Clone(response.Workload).(*runnersv1.Workload), "anchors-bound"
					case *runnersv1.UpdateAnchoredWorkloadResponse:
						result.Workload = proto.Clone(response.Workload).(*runnersv1.Workload)
						stage = map[runnersv1.PreparedWorkloadPhase]string{
							1: "reserved", 2: "preparing", 3: "bound", 4: "activating", 5: "active", 6: "removing", 7: "removed",
						}[result.Workload.Preparation.Phase]
						if cfg.Mode == "stop" && req.(*runnersv1.UpdateAnchoredWorkloadRequest).Operation.GetBind() != nil {
							stage = "recovered-binding"
						}
					case *runnersv1.UpdateVolumeCheckedResponse:
						result.Volume = proto.Clone(response.Volume).(*runnersv1.Volume)
						if cfg.Mode == "stop" {
							stage = "recovery-volume"
						} else if cfg.Mode == "retire" {
							stage = "volume-intent"
							if req.(*runnersv1.UpdateVolumeCheckedRequest).GetConfirmAnchoredRemoval() != nil {
								stage = "volume-confirmed"
							}
						}
					case *runnerv1.RemoveVolumeAnchoredResponse:
						op.NativeState = response.State.String()
						if response.State == runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING {
							stage = "volume-pending"
						}
						if response.State == runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT {
							stage = "volume-absent"
						}
					case *runnerv1.ObserveWorkloadPreparationResponse:
						result.Binding, stage = proto.Clone(response.Binding).(*runnerv1.WorkloadBinding), "observed"
					case *runnerv1.PrepareAnchoredWorkloadResponse:
						result.Binding, stage = proto.Clone(response.Binding).(*runnerv1.WorkloadBinding), "prepared"
					case *runnerv1.ActivateWorkloadResponse:
						result.Binding, stage = proto.Clone(response.Binding).(*runnerv1.WorkloadBinding), "activated"
					case *runnerv1.RemovePreparedWorkloadResponse:
						op.NativeState = response.State.String()
						nativePending = response.State == runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_PENDING
						if response.State == runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_ABSENT {
							stage = "native-absent"
						}
					case *runnerv1.RemoveWorkloadAnchorResponse:
						op.NativeState = response.State.String()
						nativePending = response.State == runnerv1.ResourceAnchorRemovalState_RESOURCE_ANCHOR_REMOVAL_STATE_PENDING
						if nativePending {
							stage = "anchor-pending"
						} else if response.State == runnerv1.ResourceAnchorRemovalState_RESOURCE_ANCHOR_REMOVAL_STATE_ABSENT {
							stage = "anchor-absent"
						}
					}
				}
				result.Operations = append(result.Operations, op)
				if err == nil {
					return barrier(stage)
				}
				return err
			}))
		if err != nil {
			t.Fatal("fixture client unavailable")
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	registry := runnersv1.NewRunnersServiceClient(connect(cfg.RegistryAddress, "x-checked-volume-fixture-token", cfg.RegistryToken))
	native := runnerv1.NewRunnerServiceClient(connect(cfg.RunnerAddress, "x-prepared-runner-fixture-token", cfg.RunnerToken))
	r := &Reconciler{runners: registry, agents: &testutil.FakeAgentsClient{
		GetSandboxFunc: func(_ context.Context, req *agentsv1.GetSandboxRequest, _ ...grpc.CallOption) (*agentsv1.GetSandboxResponse, error) {
			if !cfg.Sandbox || req.GetId() != cfg.OwnerID {
				return nil, status.Error(codes.NotFound, "other fixture sandbox")
			}
			return &agentsv1.GetSandboxResponse{Sandbox: &agentsv1.Sandbox{Meta: &agentsv1.EntityMeta{Id: cfg.OwnerID}, OrganizationId: cfg.OrganizationID, OwnerId: cfg.HumanOwnerID}}, nil
		},
	}}
	switch cfg.Mode {
	case "start":
		metadata, request, infos := cfg.request()
		var records, created []volumeRecord
		records, err = buildVolumeRecords(infos)
		if err == nil {
			if cfg.Sandbox {
				created, err = r.createSandboxVolumeRecords(ctx, &assembler.SandboxAssembleResult{Request: request, OrganizationID: cfg.OrganizationID, PersistentVolumes: infos}, cfg.RunnerID)
			} else {
				created, err = r.createVolumeRecords(ctx, records, cfg.RunnerID,
					AgentInstanceTarget{AgentID: uuid.MustParse(cfg.AgentID), AgentInstanceID: uuid.MustParse(cfg.OwnerID), ThreadID: uuid.MustParse(cfg.ThreadID)}, cfg.OrganizationID)
			}
		}
		if err == nil {
			result.Workload, err = r.startPreparedWorkload(ctx, native, metadata, request, infos, created)
		}
	case "health", "stop":
		var response *runnersv1.GetWorkloadResponse
		response, err = registry.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: cfg.WorkloadID})
		if err == nil {
			w := response.Workload
			if cfg.Mode == "health" {
				err = r.handlePreparedRunnerWorkload(ctx, native, w)
			} else {
				for {
					nativePending = false
					err = r.stopPreparedWorkload(ctx, native, w)
					if err == nil || !nativePending && status.Code(err) != codes.Aborted {
						break
					}
					select {
					case <-ctx.Done():
						err = ctx.Err()
					case <-time.After(100 * time.Millisecond):
					}
					if ctx.Err() != nil {
						break
					}
				}
			}
			result.Workload = w
		}
	case "retire":
		_, _, infos := cfg.request()
		for {
			var volume *runnersv1.GetVolumeResponse
			volume, err = registry.GetVolume(ctx, &runnersv1.GetVolumeRequest{Id: infos[0].Key()})
			if err != nil {
				break
			}
			result.Volume = volume.Volume
			var done bool
			done, err = r.advanceVolumeRemoval(ctx, native, volume.Volume)
			if done || err != nil && status.Code(err) != codes.Aborted {
				break
			}
			select {
			case <-ctx.Done():
				err = ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
			if ctx.Err() != nil {
				break
			}
		}
	default:
		t.Fatal("unsupported prepared child mode")
	}
	result.Error, result.ErrorCode = err != nil, status.Code(err).String()
	checkedStackJSON(t, filepath.Join(directory, "result.json"), result)
}

type preparedControllerProcess struct {
	process   *checkedStackProcess
	cfg       preparedControllerConfig
	directory string
}

func startPreparedController(t *testing.T, cfg preparedControllerConfig) *preparedControllerProcess {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "controller.json")
	checkedStackJSON(t, path, cfg)
	cmd := exec.Command(binary, "-test.run=^TestPreparedControllerProcess$", "-test.count=1", "-test.timeout=3m")
	cmd.Env = []string{"PREPARED_STACK_TEST=trusted-local", "PREPARED_CONTROLLER_CONFIG_FILE=" + path}
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return &preparedControllerProcess{process: startCheckedStackProcess(t, cmd), cfg: cfg, directory: directory}
}

func (p *preparedControllerProcess) awaitBarrier(t *testing.T, ctx context.Context) preparedControllerBarrier {
	t.Helper()
	for {
		data, err := os.ReadFile(filepath.Join(p.directory, "reached.json"))
		if err == nil {
			var barrier preparedControllerBarrier
			if len(data) > 1<<20 || json.Unmarshal(data, &barrier) != nil || barrier.Stage != p.cfg.Barrier || barrier.WorkloadID != p.cfg.WorkloadID || barrier.PID != p.process.cmd.Process.Pid {
				t.Fatal("prepared crash barrier identity mismatch")
			}
			select {
			case <-p.process.done:
				t.Fatal("controller exited at barrier")
			default:
			}
			return barrier
		}
		if !os.IsNotExist(err) {
			t.Fatal("prepared barrier unavailable")
		}
		select {
		case <-p.process.done:
			result := p.finish(t, ctx)
			t.Fatalf("controller exited before %s: error=%t code=%s operations=%+v", p.cfg.Barrier, result.Error, result.ErrorCode, result.Operations)
		case <-ctx.Done():
			t.Fatal("prepared barrier observation deadline expired; no restart authorized")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (p *preparedControllerProcess) release(t *testing.T) {
	t.Helper()
	checkedStackJSON(t, filepath.Join(p.directory, "release.json"), map[string]string{"stage": p.cfg.Barrier})
}

func (p *preparedControllerProcess) finish(t *testing.T, ctx context.Context) preparedControllerResult {
	t.Helper()
	p.process.wait(t, ctx)
	data, err := os.ReadFile(filepath.Join(p.directory, "result.json"))
	var result preparedControllerResult
	if err != nil || len(data) > 1<<20 || json.Unmarshal(data, &result) != nil {
		t.Fatal("prepared controller result unavailable")
	}
	t.Logf("prepared controller pid=%d mode=%s workload=%s error=%t code=%s operations=%d", p.process.cmd.Process.Pid, p.cfg.Mode, p.cfg.WorkloadID, result.Error, result.ErrorCode, len(result.Operations))
	return result
}
