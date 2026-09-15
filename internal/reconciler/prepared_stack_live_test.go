package reconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

type preparedProbe struct {
	Owner, Entries  string
	Turn, Heartbeat int
}

func (f *preparedStackNamespace) gated(t *testing.T, ctx context.Context, b *runnerv1.WorkloadBinding) {
	t.Helper()
	for i := 0; i < 8; i++ {
		pod, err := f.kube.CoreV1().Pods(f.ns.Name).Get(ctx, "workload-"+b.WorkloadId, metav1.GetOptions{})
		if err != nil || string(pod.UID) != b.InstanceUid {
			t.Fatal("prepared Pod changed during gated observation")
		}
		gate := false
		for _, g := range pod.Spec.SchedulingGates {
			if g.Name == "agyn.io/workload-binding" {
				gate = true
			}
		}
		if !gate || pod.Spec.NodeName != "" || len(pod.Status.ContainerStatuses) != 0 || len(pod.Status.InitContainerStatuses) != 0 {
			t.Fatal("execution observed before durable activation")
		}
		if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
			t.Fatal("fixture workload has service-account credentials")
		}
		select {
		case <-ctx.Done():
			t.Fatal("gated observation expired")
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Logf("eight independent gated observations workload=%s podUID=%s", b.WorkloadId, b.InstanceUid)
}

func (f *preparedStackNamespace) probe(t *testing.T, ctx context.Context, cfg preparedControllerConfig, b *runnerv1.WorkloadBinding, after int) preparedProbe {
	t.Helper()
	var proof preparedProbe
	expected := ""
	for i := 1; i <= cfg.Turn; i++ {
		expected += fmt.Sprintf("%s:%d\n", cfg.OwnerID, i)
	}
	err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 60*time.Second, true, func(ctx context.Context) (bool, error) {
		pod, err := f.kube.CoreV1().Pods(f.ns.Name).Get(ctx, "workload-"+b.WorkloadId, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if string(pod.UID) != b.InstanceUid {
			return false, fmt.Errorf("probe Pod was replaced")
		}
		if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			return false, fmt.Errorf("probe exited: phase=%s", pod.Status.Phase)
		}
		if pod.Status.Phase != corev1.PodRunning {
			return false, nil
		}
		lines := int64(10)
		data, err := f.kube.CoreV1().Pods(f.ns.Name).GetLogs(pod.Name, &corev1.PodLogOptions{Container: "main", TailLines: &lines}).DoRaw(ctx)
		if err != nil {
			return false, err
		}
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			var p preparedProbe
			if json.Unmarshal([]byte(line), &p) != nil {
				return false, fmt.Errorf("invalid model-free execution proof")
			}
			if p.Owner != cfg.OwnerID || p.Turn != cfg.Turn || p.Entries != expected {
				return false, fmt.Errorf("workspace owner or exact effects changed")
			}
			if p.Heartbeat > proof.Heartbeat {
				proof = p
			}
		}
		return proof.Heartbeat > after, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return proof
}

func (f *preparedStackNamespace) absent(t *testing.T, ctx context.Context, b *runnerv1.WorkloadBinding) {
	t.Helper()
	if _, err := f.kube.CoreV1().Pods(f.ns.Name).Get(ctx, "workload-"+b.WorkloadId, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("independent Pod absence unconfirmed")
	}
	for _, v := range b.Volumes {
		claim, err := f.kube.CoreV1().PersistentVolumeClaims(f.ns.Name).Get(ctx, v.InstanceId, metav1.GetOptions{})
		if err != nil || string(claim.UID) != v.InstanceUid || claim.DeletionTimestamp != nil {
			t.Fatal("idle workspace not retained")
		}
		f.anchor(t, ctx, v.Anchor)
		for _, hold := range claim.Finalizers {
			if hold == "agyn.io/workload-"+b.InstanceUid {
				t.Fatal("absent Pod retained its workspace hold")
			}
		}
	}
}

func assertPreparedNoRedispatch(t *testing.T, result preparedControllerResult) {
	t.Helper()
	for _, op := range result.Operations {
		switch op.Operation {
		case runnerv1.RunnerService_PrepareWorkload_FullMethodName, runnerv1.RunnerService_PrepareAnchoredWorkload_FullMethodName,
			runnerv1.RunnerService_ReserveResourceAnchor_FullMethodName, runnerv1.RunnerService_ActivateWorkload_FullMethodName,
			runnerv1.RunnerService_StartWorkload_FullMethodName, runnersv1.RunnersService_CreateWorkload_FullMethodName,
			runnersv1.RunnersService_CreatePreparedWorkload_FullMethodName, runnersv1.RunnersService_CreateAnchoredWorkload_FullMethodName,
			runnersv1.RunnersService_BindWorkloadResourceAnchors_FullMethodName:
			t.Fatalf("recovery redispatched execution: %s", op.Operation)
		}
	}
}

// Real PostgreSQL, registry/native RPC servers and SIGKILLed controller children.
// The fixed Node program stands in for an agent. This does not prove A2A,
// daemon inbox recovery, provider credentials, Ziti authorization or node fencing.
func TestLivePreparedExecutionStack(t *testing.T) {
	if os.Getenv("PREPARED_STACK_TEST") != "trusted-local" {
		t.Skip("explicit prepared execution acceptance opt-in required")
	}
	kubeconfig, registryBinary, nativeBinary, chart := os.Getenv("RETENTION_KUBECONFIG"), os.Getenv("CHECKED_REGISTRY_BINARY"), os.Getenv("PREPARED_RUNNER_BINARY"), os.Getenv("PREPARED_RUNNER_CHART")
	for _, path := range []string{kubeconfig, registryBinary, nativeBinary, chart} {
		info, err := os.Stat(path)
		if !filepath.IsAbs(path) || err != nil || !info.Mode().IsRegular() {
			t.Fatal("absolute reviewed fixture files required")
		}
	}
	for _, sandbox := range []bool{false, true} {
		name := "agent-instance"
		if sandbox {
			name = "sandbox"
		}
		t.Run(name, func(t *testing.T) {
			stackT := t
			// Nineteen sequential scenarios include native GC and process replacement.
			// Keep the independent 120-second controller-operation bounds unchanged.
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
			defer cancel()
			live := newPreparedStackNamespace(t, ctx, kubeconfig, chart)
			nativeProcess, nativeAddress := startPreparedNative(t, ctx, nativeBinary, live)
			database := newCheckedStackDatabase(t, ctx, os.Getenv("CHECKED_POSTGRES_IMAGE"))
			database.config.PreparedWorkloads = true
			registry := startCheckedRegistry(t, ctx, registryBinary, database.config)
			agentID, definitionID := uuid.NewString(), uuid.NewString()
			newConfig := func() preparedControllerConfig {
				owner := uuid.NewString()
				return preparedControllerConfig{RegistryAddress: registry.address, RegistryToken: database.config.Token, RunnerAddress: nativeAddress, RunnerToken: live.token,
					RunnerID: database.config.RunnerID, OrganizationID: database.config.OrganizationID, OwnerID: owner, ThreadID: uuid.NewString(), AgentID: agentID, DefinitionID: definitionID,
					WorkloadID: uuid.NewString(), HumanOwnerID: uuid.NewString(), RunID: live.run, Image: os.Getenv("PREPARED_NODE_IMAGE"), Mode: "start", Turn: 1, Sandbox: sandbox}
			}
			state := func(t *testing.T, cfg preparedControllerConfig, phase runnersv1.PreparedWorkloadPhase) *runnersv1.Workload {
				t.Helper()
				w := database.assertPreparedWorkload(t, ctx, registry.client, cfg.WorkloadID)
				if w.Preparation.Phase != phase {
					t.Fatalf("unexpected persisted phase: got %s want %s", w.Preparation.Phase, phase)
				}
				return w
			}
			run := func(t *testing.T, cfg preparedControllerConfig) preparedControllerResult {
				t.Helper()
				cfg.Barrier = ""
				result := startPreparedController(t, cfg).finish(t, ctx)
				if cfg.Mode == "start" && result.Binding != nil {
					live.track(t, ctx, result.Binding)
				}
				if result.Error {
					t.Fatalf("prepared controller failed code=%s operations=%+v", result.ErrorCode, result.Operations)
				}
				if cfg.Mode == "start" {
					if !cfg.Sandbox && (cfg.ThreadID == cfg.OwnerID || result.Workload.ThreadId != cfg.OwnerID || result.Workload.Preparation.Resources.Workload.IdentityLabels["thread-id"] != cfg.ThreadID) {
						t.Fatal("registry instance alias replaced the native inbox thread")
					}
					live.track(t, ctx, result.Workload.GetPreparation().GetBinding())
				} else {
					assertPreparedNoRedispatch(t, result)
				}
				return result
			}
			stop := func(t *testing.T, cfg preparedControllerConfig, b *runnerv1.WorkloadBinding) {
				t.Helper()
				cfg.Mode = "stop"
				run(t, cfg)
				w := state(t, cfg, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED)
				if w.RemovalConfirmedAt == nil || !samePreparedBinding(w.Preparation.Binding, b) {
					t.Fatal("exact persisted removal missing")
				}
				live.absent(t, ctx, b)
				live.anchorAbsent(t, ctx, b.Anchor)
				v := database.assertVolume(t, ctx, registry.client, b.Volumes[0].VolumeKey)
				if v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE || !proto.Equal(v.BoundInstance, b.Volumes[0]) {
					t.Fatal("retirement changed durable workspace identity")
				}
			}
			blocked := func(t *testing.T, cfg preparedControllerConfig) {
				t.Helper()
				cfg.WorkloadID = uuid.NewString()
				metadata, _, infos := cfg.request()
				_, err := registry.client.CreateAnchoredWorkload(ctx, &runnersv1.CreateAnchoredWorkloadRequest{Preparation: &runnersv1.CreatePreparedWorkloadRequest{Workload: metadata, BackendId: "kubernetes-namespace/v1/" + live.ns.Name + "/" + string(live.ns.UID), VolumeIds: []string{infos[0].Key()}}})
				if status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("same-owner admission not held: %v", err)
				}
				if _, err := registry.client.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: cfg.WorkloadID}); status.Code(err) != codes.NotFound {
					t.Fatal("rejected admission persisted a new workload")
				}
			}
			followup := func(t *testing.T, cfg preparedControllerConfig, first *runnerv1.WorkloadBinding, turn int) {
				t.Helper()
				cfg.Mode, cfg.Barrier, cfg.WorkloadID, cfg.Turn = "start", "", uuid.NewString(), turn
				result := run(t, cfg)
				second := result.Workload.Preparation.Binding
				if first.InstanceUid == second.InstanceUid || !proto.Equal(first.Volumes[0], second.Volumes[0]) {
					t.Fatal("follow-up did not reuse only the exact workspace")
				}
				live.probe(t, ctx, cfg, second, 0)
				stop(t, cfg, second)
			}
			t.Run("fixture-boundaries", func(t *testing.T) {
				conn, err := grpc.NewClient(nativeAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				if _, err := runnerv1.NewRunnerServiceClient(conn).ListVolumes(ctx, &runnerv1.ListVolumesRequest{}); status.Code(err) != codes.Unauthenticated {
					t.Fatal("anonymous native RPC accepted")
				}
				if _, err := live.runner.StartWorkload(ctx, &runnerv1.StartWorkloadRequest{}); status.Code(err) != codes.PermissionDenied {
					t.Fatal("legacy native start exposed")
				}
				if _, err := registry.client.CreateWorkload(ctx, &runnersv1.CreateWorkloadRequest{}); status.Code(err) != codes.PermissionDenied {
					t.Fatal("legacy registry start exposed in prepared mode")
				}
				conn2, err := grpc.NewClient(registry.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					t.Fatal(err)
				}
				defer conn2.Close()
				if _, err := runnersv1.NewRunnersServiceClient(conn2).ListRunners(ctx, &runnersv1.ListRunnersRequest{}); status.Code(err) != codes.Unauthenticated {
					t.Fatal("anonymous registry RPC accepted")
				}
			})
			t.Run("parallel-durable-followup", func(t *testing.T) {
				first := newConfig()
				first.Barrier = "bound"
				child := startPreparedController(t, first)
				barrier := child.awaitBarrier(t, ctx)
				binding := barrier.Result.Workload.Preparation.Binding
				live.track(t, ctx, binding)
				state(t, first, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_BOUND)
				live.gated(t, ctx, binding)
				other := newConfig()
				second := run(t, other).Workload.Preparation.Binding
				if second.Volumes[0].InstanceUid == binding.Volumes[0].InstanceUid {
					t.Fatal("parallel owners share a workspace")
				}
				before := live.probe(t, ctx, other, second, 0)
				blocked(t, first)
				child.release(t)
				result := child.finish(t, ctx)
				if result.Error {
					t.Fatalf("bound start failed: %+v", result.Operations)
				}
				live.probe(t, ctx, first, binding, 0)
				stop(t, first, binding)
				followup(t, first, binding, 2)
				live.probe(t, ctx, other, second, before.Heartbeat)
				stop(t, other, second)
				t.Log("same agent, parallel isolated owners, held same-owner admission, physical release, new Pod/same PVC and exact prior effects verified")
			})
			t.Run("activation-ack-crash-and-three-process-replacement", func(t *testing.T) {
				cfg := newConfig()
				cfg.Barrier = "activated"
				child := startPreparedController(t, cfg)
				barrier := child.awaitBarrier(t, ctx)
				binding := barrier.Result.Binding
				live.track(t, ctx, binding)
				state(t, cfg, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVATING)
				before := live.probe(t, ctx, cfg, binding, 0)
				child.process.kill(t)
				registry.process.kill(t)
				nativeProcess.kill(t)
				nativeProcess, nativeAddress = startPreparedNative(stackT, ctx, nativeBinary, live)
				registry = startCheckedRegistry(stackT, ctx, registryBinary, database.config)
				cfg.RegistryAddress, cfg.RunnerAddress, cfg.Mode, cfg.Barrier = registry.address, nativeAddress, "health", ""
				run(t, cfg)
				w := state(t, cfg, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVE)
				if w.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING || !samePreparedBinding(w.Preparation.Binding, binding) {
					t.Fatal("lost ACK recovery did not persist exact running workload")
				}
				live.probe(t, ctx, cfg, binding, before.Heartbeat)
				run(t, cfg)
				stop(t, cfg, binding)
				followup(t, cfg, binding, 2)
				t.Log("controller, registry and native runner SIGKILL confirmed; fresh processes recovered activation without prepare/activate redispatch or repeated workspace effect")
			})
			t.Run("cancellation-during-in-flight-prepare", func(t *testing.T) {
				cfg := newConfig()
				cfg.Barrier = "prepared"
				child := startPreparedController(t, cfg)
				barrier := child.awaitBarrier(t, ctx)
				binding := barrier.Result.Binding
				live.track(t, ctx, binding)
				state(t, cfg, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_PREPARING)
				live.gated(t, ctx, binding)
				cancelCfg := cfg
				cancelCfg.Mode, cancelCfg.Barrier = "stop", ""
				result := startPreparedController(t, cancelCfg).finish(t, ctx)
				if result.Error {
					t.Fatalf("in-flight preparation could not be observed and retired: %s", result.ErrorCode)
				}
				assertPreparedNoRedispatch(t, result)
				w := state(t, cfg, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED)
				if !samePreparedBinding(w.Preparation.Binding, binding) || w.RemovalConfirmedAt == nil {
					t.Fatal("cancellation lost its exact observed binding/removal")
				}
				live.absent(t, ctx, binding)
				child.release(t)
				result = child.finish(t, ctx)
				if !result.Error {
					t.Fatal("canceled preparation reported successful startup")
				}
				for _, op := range result.Operations {
					if op.Operation == runnerv1.RunnerService_ActivateWorkload_FullMethodName {
						t.Fatal("canceled preparation activated")
					}
				}
				stop(t, cfg, binding)
				followup(t, cfg, binding, 1)
				t.Log("cancellation discovered and retired the exact unbound Pod; late prepare response could not reactivate it; next turn verified no prior effects")
			})
			for _, stage := range []string{"bound", "activating"} {
				t.Run("cancel-before-execution-"+stage, func(t *testing.T) {
					cfg := newConfig()
					cfg.Barrier = stage
					child := startPreparedController(t, cfg)
					barrier := child.awaitBarrier(t, ctx)
					binding := barrier.Result.Workload.Preparation.Binding
					live.track(t, ctx, binding)
					phase := runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_BOUND
					if stage == "activating" {
						phase = runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVATING
					}
					state(t, cfg, phase)
					live.gated(t, ctx, binding)
					stop(t, cfg, binding)
					child.release(t)
					result := child.finish(t, ctx)
					if !result.Error {
						t.Fatal("retired startup returned success")
					}
					for _, op := range result.Operations {
						if op.Operation == runnerv1.RunnerService_ActivateWorkload_FullMethodName && op.Code == "OK" {
							t.Fatal("retired Pod activation succeeded")
						}
					}
					state(t, cfg, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED)
					live.absent(t, ctx, binding)
					followup(t, cfg, binding, 1)
					t.Logf("retirement at %s excluded execution by the paused startup process; retained workspace has no prior effects", stage)
				})
			}
			t.Run("unknown-prepare-recovery-and-three-process-replacement", func(t *testing.T) {
				cfg := newConfig()
				cfg.Barrier = "prepared"
				child := startPreparedController(t, cfg)
				barrier := child.awaitBarrier(t, ctx)
				binding := barrier.Result.Binding
				live.track(t, ctx, binding)
				child.process.kill(t)
				registry.process.kill(t)
				nativeProcess.kill(t)
				nativeProcess, nativeAddress = startPreparedNative(stackT, ctx, nativeBinary, live)
				registry = startCheckedRegistry(stackT, ctx, registryBinary, database.config)
				cfg.RegistryAddress, cfg.RunnerAddress, cfg.Mode, cfg.Barrier = registry.address, nativeAddress, "stop", ""
				w := state(t, cfg, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_PREPARING)
				if w.Preparation.Binding != nil {
					t.Fatal("lost reply was persisted before recovery")
				}
				live.gated(t, ctx, binding)
				blocked(t, cfg)
				stop(t, cfg, binding)
				followup(t, cfg, binding, 1)
				t.Log("controller/registry/native SIGKILL; fresh production recovery discovered, bound and removed the unexecuted Pod; same-PVC next turn verified no prior effects")
			})
			for _, checkpoint := range []string{"observed", "recovery-volume", "recovered-binding"} {
				t.Run("recovery-crash-"+checkpoint, func(t *testing.T) {
					cfg := newConfig()
					cfg.Barrier = "prepared"
					starter := startPreparedController(t, cfg)
					binding := starter.awaitBarrier(t, ctx).Result.Binding
					live.track(t, ctx, binding)
					starter.process.kill(t)
					cfg.Mode, cfg.Barrier = "stop", checkpoint
					recovery := startPreparedController(t, cfg)
					recovery.awaitBarrier(t, ctx)
					state(t, cfg, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING)
					live.gated(t, ctx, binding)
					blocked(t, cfg)
					recovery.process.kill(t)
					cfg.Barrier = ""
					stop(t, cfg, binding)
					followup(t, cfg, binding, 1)
					t.Logf("recovery SIGKILL after %s retained exact intent/bindings; fresh controller completed removal without redispatch; same-PVC first turn passed", checkpoint)
				})
			}
			t.Run("competing-preparation-recovery", func(t *testing.T) {
				cfg := newConfig()
				cfg.Barrier = "prepared"
				starter := startPreparedController(t, cfg)
				binding := starter.awaitBarrier(t, ctx).Result.Binding
				live.track(t, ctx, binding)
				starter.process.kill(t)
				cfg.Mode, cfg.Barrier = "stop", "observed"
				stale := startPreparedController(t, cfg)
				stale.awaitBarrier(t, ctx)
				state(t, cfg, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING)
				live.gated(t, ctx, binding)
				blocked(t, cfg)
				cfg.Barrier = ""
				stop(t, cfg, binding)
				stale.release(t)
				result := stale.finish(t, ctx)
				if result.Error || result.Workload.GetRemovalConfirmedAt() == nil || !samePreparedBinding(result.Workload.GetPreparation().GetBinding(), binding) {
					t.Fatal("stale recovery did not accept exact competing retirement")
				}
				assertPreparedNoRedispatch(t, result)
				for _, op := range result.Operations {
					if op.Operation == runnerv1.RunnerService_RemovePreparedWorkload_FullMethodName {
						t.Fatal("stale observer repeated already-confirmed native removal")
					}
				}
				followup(t, cfg, binding, 1)
				t.Log("overlapping controller processes converged on the same exact removal; stale observation caused no replay or repeated removal, and the next turn retained the workspace")
			})
			t.Run("missing-preparation-revokes-before-release", func(t *testing.T) {
				cfg := newConfig()
				cfg.Barrier = "preparing"
				starter := startPreparedController(t, cfg)
				starter.awaitBarrier(t, ctx)
				starter.process.kill(t)
				cfg.Mode, cfg.Barrier = "stop", ""
				result := run(t, cfg)
				assertPreparedNoRedispatch(t, result)
				w := state(t, cfg, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED)
				if w.Preparation.Binding != nil || w.RemovalConfirmedAt == nil || w.Preparation.Resources.GetRevocationObservation() == nil {
					t.Fatal("missing preparation did not preserve its distinct revocation evidence")
				}
				live.trackRevocation(t, ctx, w.Preparation.Resources.PreparationRevocation)
				live.anchorAbsent(t, ctx, w.Preparation.Resources.Workload)
				cfg.Mode, cfg.WorkloadID = "start", uuid.NewString()
				first := run(t, cfg).Workload.Preparation.Binding
				live.probe(t, ctx, cfg, first, 0)
				stop(t, cfg, first)
				t.Log("native revocation and independently persisted observation released admission without a fabricated Pod UID; explicit first turn verified no prior effects")
			})
			for _, stage := range []string{"removing", "native-absent", "anchor-pending", "anchor-absent", "removed"} {
				t.Run("removal-crash-"+stage, func(t *testing.T) {
					cfg := newConfig()
					binding := run(t, cfg).Workload.Preparation.Binding
					live.probe(t, ctx, cfg, binding, 0)
					cfg.Mode, cfg.Barrier = "stop", stage
					child := startPreparedController(t, cfg)
					child.awaitBarrier(t, ctx)
					phase := runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING
					if stage == "removed" {
						phase = runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED
					}
					state(t, cfg, phase)
					if stage != "removed" {
						blocked(t, cfg)
					}
					if stage != "removing" {
						live.absent(t, ctx, binding)
					}
					child.process.kill(t)
					cfg.Barrier = ""
					stop(t, cfg, binding)
					followup(t, cfg, binding, 2)
					t.Logf("SIGKILL at %s; SQL/removal recovery and exact-once fixture append across follow-up verified", stage)
				})
			}
			t.Run("unused-reservation-crash", func(t *testing.T) {
				cfg := newConfig()
				cfg.Barrier = "reserved"
				child := startPreparedController(t, cfg)
				child.awaitBarrier(t, ctx)
				child.process.kill(t)
				state(t, cfg, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_RESERVED)
				if _, err := live.kube.CoreV1().Pods(live.ns.Name).Get(ctx, "workload-"+cfg.WorkloadID, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
					t.Fatal("unused reservation created a Pod")
				}
				cfg.Mode = "stop"
				run(t, cfg)
				w := state(t, cfg, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED)
				if w.Preparation.Binding != nil || w.Preparation.RemovalObservation != nil {
					t.Fatal("unused reservation invented native evidence")
				}
			})
			t.Run("cancel-before-preparation-authority", func(t *testing.T) {
				cfg := newConfig()
				cfg.Barrier = "anchors-bound"
				starter := startPreparedController(t, cfg)
				starter.awaitBarrier(t, ctx)
				reserved := state(t, cfg, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_RESERVED)
				for _, a := range append([]*runnerv1.ResourceAnchor{reserved.Preparation.Resources.Workload}, reserved.Preparation.Resources.Volumes...) {
					live.anchor(t, ctx, a)
				}
				if _, err := live.kube.CoreV1().Pods(live.ns.Name).Get(ctx, "workload-"+cfg.WorkloadID, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
					t.Fatal("native Pod preceded preparation authority")
				}
				cancelCfg := cfg
				cancelCfg.Mode = "stop"
				run(t, cancelCfg)
				removed := state(t, cfg, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED)
				if removed.Preparation.Binding != nil || removed.RemovalConfirmedAt == nil {
					t.Fatal("unused anchored reservation fabricated a Pod receipt")
				}
				live.anchorAbsent(t, ctx, reserved.Preparation.Resources.Workload)
				starter.release(t)
				result := starter.finish(t, ctx)
				if !result.Error {
					t.Fatal("canceled reservation still started")
				}
				assertPreparedNoNativePreparation(t, result)
				cfg.WorkloadID, cfg.Barrier = uuid.NewString(), ""
				binding := run(t, cfg).Workload.Preparation.Binding
				if !proto.Equal(binding.Volumes[0].Anchor, reserved.Preparation.Resources.Volumes[0]) {
					t.Fatal("first PVC did not reuse the durable volume owner")
				}
				live.probe(t, ctx, cfg, binding, 0)
				stop(t, cfg, binding)
			})
		})
	}
}

func assertPreparedNoNativePreparation(t *testing.T, result preparedControllerResult) {
	t.Helper()
	for _, op := range result.Operations {
		if op.Operation == runnerv1.RunnerService_PrepareAnchoredWorkload_FullMethodName || op.Operation == runnerv1.RunnerService_PrepareWorkload_FullMethodName || op.Operation == runnerv1.RunnerService_ActivateWorkload_FullMethodName {
			t.Fatalf("canceled reservation dispatched native preparation: %s", op.Operation)
		}
	}
}
