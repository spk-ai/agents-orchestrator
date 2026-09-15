package reconciler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (f *preparedStackNamespace) volumeAnchorAbsent(t *testing.T, ctx context.Context, a *runnerv1.ResourceAnchor) {
	t.Helper()
	if a == nil || a.Kind != runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME || !preparedUUID(a.ResourceId) ||
		a.BackendId != "kubernetes-namespace/v1/"+f.ns.Name+"/"+string(f.ns.UID) {
		t.Fatal("exact fixture volume anchor required")
	}
	if _, err := f.kube.CoreV1().ConfigMaps(f.ns.Name).Get(ctx, "volume-anchor-"+a.ResourceId, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("independent volume anchor absence unconfirmed")
	}
}

func TestLiveAnchoredVolumeRetirementStack(t *testing.T) {
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
		t.Run(map[bool]string{false: "agent", true: "sandbox"}[sandbox], func(t *testing.T) {
			group := t
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
			defer cancel()
			live := newPreparedStackNamespace(t, ctx, kubeconfig, chart)
			nativeProcess, nativeAddress := startPreparedNative(t, ctx, nativeBinary, live)
			database := newCheckedStackDatabase(t, ctx, os.Getenv("CHECKED_POSTGRES_IMAGE"))
			database.config.PreparedWorkloads = true
			registry := startCheckedRegistry(t, ctx, registryBinary, database.config)
			newConfig := func() preparedControllerConfig {
				return preparedControllerConfig{RegistryAddress: registry.address, RegistryToken: database.config.Token, RunnerAddress: nativeAddress, RunnerToken: live.token,
					RunnerID: database.config.RunnerID, OrganizationID: database.config.OrganizationID, OwnerID: uuid.NewString(), ThreadID: uuid.NewString(), AgentID: uuid.NewString(), DefinitionID: uuid.NewString(),
					WorkloadID: uuid.NewString(), HumanOwnerID: uuid.NewString(), RunID: live.run, Image: os.Getenv("PREPARED_NODE_IMAGE"), Mode: "start", Turn: 1, Sandbox: sandbox}
			}
			run := func(t *testing.T, cfg preparedControllerConfig) preparedControllerResult {
				t.Helper()
				cfg.Barrier = ""
				result := startPreparedController(t, cfg).finish(t, ctx)
				if cfg.Mode == "start" && result.Binding != nil {
					live.track(t, ctx, result.Binding)
				}
				if result.Error {
					t.Fatalf("controller %s failed: %s operations=%+v", cfg.Mode, result.ErrorCode, result.Operations)
				}
				if cfg.Mode != "start" {
					assertPreparedNoRedispatch(t, result)
				}
				return result
			}
			stop := func(t *testing.T, cfg preparedControllerConfig, b *runnerv1.WorkloadBinding) {
				t.Helper()
				cfg.Mode = "stop"
				run(t, cfg)
				w := database.assertPreparedWorkload(t, ctx, registry.client, cfg.WorkloadID)
				if w.RemovalConfirmedAt == nil {
					t.Fatal("workload removal unconfirmed")
				}
				live.absent(t, ctx, b)
				live.anchorAbsent(t, ctx, b.Anchor)
			}
			for _, checkpoint := range []string{"volume-intent", "volume-pending", "volume-absent", "volume-confirmed"} {
				if !t.Run(checkpoint, func(t *testing.T) {
					cfg, other := newConfig(), newConfig()
					first, peer := run(t, cfg).Binding, run(t, other).Binding
					live.probe(t, ctx, cfg, first, 0)
					priorPeer := live.probe(t, ctx, other, peer, 0)
					if first.Volumes[0].InstanceUid == peer.Volumes[0].InstanceUid {
						t.Fatal("tasks shared a physical workspace")
					}
					stop(t, cfg, first)
					initial := database.assertVolume(t, ctx, registry.client, first.Volumes[0].VolumeKey)
					cfg.Mode, cfg.Barrier = "retire", checkpoint
					child := startPreparedController(t, cfg)
					barrier := child.awaitBarrier(t, ctx)
					assertPreparedNoRedispatch(t, barrier.Result)
					persisted := database.assertVolume(t, ctx, registry.client, initial.Meta.Id)
					if !persisted.RemovalIntent.GetAnchored() || !proto.Equal(persisted.BoundInstance, initial.BoundInstance) {
						t.Fatal("retirement did not preserve original target")
					}
					if checkpoint != "volume-confirmed" && persisted.AnchoredRemovalObservation != nil {
						t.Fatal("retirement confirmed before registry receipt")
					}
					if checkpoint == "volume-intent" {
						live.absent(t, ctx, first)
					}
					if checkpoint == "volume-absent" || checkpoint == "volume-confirmed" {
						if _, err := live.kube.CoreV1().PersistentVolumeClaims(live.ns.Name).Get(ctx, initial.GetInstanceId(), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
							t.Fatal("native PVC absence unconfirmed")
						}
						live.volumeAnchorAbsent(t, ctx, initial.ResourceAnchor)
					}
					child.process.kill(t)
					if checkpoint == "volume-absent" {
						nativeProcess.kill(t)
						registry.process.kill(t)
						// Replacement services are shared by the remaining checkpoints.
						nativeProcess, nativeAddress = startPreparedNative(group, ctx, nativeBinary, live)
						registry = startCheckedRegistry(group, ctx, registryBinary, database.config)
						cfg.RunnerAddress, cfg.RegistryAddress = nativeAddress, registry.address
						other.RunnerAddress, other.RegistryAddress = nativeAddress, registry.address
					}
					result := run(t, cfg)
					for _, op := range result.Operations {
						if op.Operation == runnerv1.RunnerService_RemoveVolumeBound_FullMethodName || op.Operation == runnerv1.RunnerService_RemoveVolume_FullMethodName {
							t.Fatal("retirement used an old native API")
						}
						if checkpoint == "volume-confirmed" && op.Operation == runnerv1.RunnerService_RemoveVolumeAnchored_FullMethodName {
							t.Fatal("persisted confirmation reissued native retirement")
						}
					}
					finished := database.assertVolume(t, ctx, registry.client, initial.Meta.Id)
					if finished.Status != runnersv1.VolumeStatus_VOLUME_STATUS_DELETED || finished.RemovalIntent.GetId() != persisted.RemovalIntent.GetId() ||
						finished.AnchoredRemovalObservation.GetState() != runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT ||
						!proto.Equal(initial.BoundInstance, finished.BoundInstance) || !proto.Equal(initial.ResourceAnchor, finished.ResourceAnchor) || !proto.Equal(initial.AnchorReservation, finished.AnchorReservation) {
						t.Fatal("retirement lost exact durable history")
					}
					if _, err := live.kube.CoreV1().PersistentVolumeClaims(live.ns.Name).Get(ctx, initial.GetInstanceId(), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
						t.Fatal("retirement retained its native PVC")
					}
					live.volumeAnchorAbsent(t, ctx, initial.ResourceAnchor)
					live.probe(t, ctx, other, peer, priorPeer.Heartbeat)
					stop(t, other, peer)
					t.Log("SIGKILL recovery retained exact intent/PVC/owner identity and persisted native absence; peer effects and workspace preserved")
				}) {
					return
				}
			}
		})
	}
}
