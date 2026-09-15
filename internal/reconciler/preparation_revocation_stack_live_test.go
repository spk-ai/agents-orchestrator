package reconciler

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Production controller/registry/runner code with real process death, SQL and
// Kubernetes identities. Late PVCs are explicit fixture API creates, not a
// claim to simulate an already-admitted API request or to prove node fencing.
func TestLivePreparationRevocationStack(t *testing.T) {
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
		t.Run(fmt.Sprintf("sandbox=%t", sandbox), func(t *testing.T) {
			stackT := t
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
				cfg.RegistryAddress, cfg.RunnerAddress, cfg.Barrier = registry.address, nativeAddress, ""
				result := startPreparedController(t, cfg).finish(t, ctx)
				if result.Error {
					t.Fatalf("controller failed code=%s operations=%+v", result.ErrorCode, result.Operations)
				}
				if cfg.Mode == "start" {
					live.track(t, ctx, result.Workload.GetPreparation().GetBinding())
				} else {
					assertPreparedNoRedispatch(t, result)
				}
				return result
			}
			peerConfig := newConfig()
			peer := run(t, peerConfig).Workload.Preparation.Binding
			peerProof := live.probe(t, ctx, peerConfig, peer, 0)
			for checkpointIndex, checkpoint := range []string{"preparation-revoked", "revocation-recorded", "revocation-observed", "revocation-confirmed"} {
				for _, latePVC := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/late-pvc=%t", checkpoint, latePVC), func(t *testing.T) {
						cfg := newConfig()
						cfg.Barrier = "preparing"
						starter := startPreparedController(t, cfg)
						starter.awaitBarrier(t, ctx)
						w := database.assertPreparedWorkload(t, ctx, registry.client, cfg.WorkloadID)
						if w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_PREPARING || w.Preparation.Binding != nil {
							t.Fatal("startup was not interrupted before native preparation")
						}
						_, _, infos := cfg.request()
						originalVolume := database.assertVolume(t, ctx, registry.client, infos[0].Key())
						if originalVolume.BoundInstance != nil || originalVolume.LifecycleRevision != 2 {
							t.Fatal("original first-provision reservation required")
						}
						starter.process.kill(t)
						cfg.Mode, cfg.Barrier = "stop", checkpoint
						recovery := startPreparedController(t, cfg)
						barrier := recovery.awaitBarrier(t, ctx)
						proof := barrier.Result.Revocation
						if proof == nil || proof.SelectedPodUid != "" {
							t.Fatal("pre-create revocation fabricated a selected Pod UID")
						}
						live.trackRevocation(t, ctx, proof)
						w = database.assertPreparedWorkload(t, ctx, registry.client, cfg.WorkloadID)
						if (w.Preparation.Resources.PreparationRevocation != nil) != (checkpoint != "preparation-revoked") ||
							(w.RemovalConfirmedAt != nil) != (checkpoint == "revocation-confirmed") || w.Preparation.Binding != nil {
							t.Fatal("SQL did not retain the expected separate recovery checkpoint")
						}
						if checkpoint != "revocation-confirmed" {
							metadata, _, _ := cfg.request()
							metadata.Id = uuid.NewString()
							_, err := registry.client.CreateAnchoredWorkload(ctx, &runnersv1.CreateAnchoredWorkloadRequest{Preparation: &runnersv1.CreatePreparedWorkloadRequest{
								Workload: metadata, BackendId: w.Preparation.BackendId, VolumeIds: w.Preparation.VolumeIds}})
							if status.Code(err) != codes.FailedPrecondition {
								t.Fatal("native receipt or observation alone released admission")
							}
						}
						peerProof = live.probe(t, ctx, peerConfig, peer, peerProof.Heartbeat)
						recovery.process.kill(t)
						registry.process.kill(t)
						nativeProcess.kill(t)
						var late *corev1.PersistentVolumeClaim
						if latePVC {
							owner := live.anchor(t, ctx, originalVolume.ResourceAnchor)
							labels := maps.Clone(originalVolume.ResourceAnchor.IdentityLabels)
							labels[preparedStackLabel] = live.run
							encoded, err := protojson.Marshal(originalVolume.ResourceAnchor)
							if err != nil {
								t.Fatal(err)
							}
							late, err = live.kube.CoreV1().PersistentVolumeClaims(live.ns.Name).Create(ctx, &corev1.PersistentVolumeClaim{
								ObjectMeta: metav1.ObjectMeta{Name: infos[0].Spec.PersistentName, Labels: labels, Annotations: map[string]string{"agyn.io/resource-anchor": string(encoded)},
									OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: owner.Name, UID: owner.UID}}},
								Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
									Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Mi")}}}}, metav1.CreateOptions{})
							if err != nil {
								t.Fatal(err)
							}
							live.claims[late.Name] = late.UID
						}
						nativeProcess, nativeAddress = startPreparedNative(stackT, ctx, nativeBinary, live)
						registry = startCheckedRegistry(stackT, ctx, registryBinary, database.config)
						result := run(t, cfg)
						w = database.assertPreparedWorkload(t, ctx, registry.client, cfg.WorkloadID)
						if w.RemovalConfirmedAt == nil || w.InstanceId != nil || w.Preparation.Binding != nil ||
							!proto.Equal(w.Preparation.Resources.PreparationRevocation, proof) || w.Preparation.Resources.RevocationObservation == nil {
							t.Fatal("fresh recovery lost native evidence or fabricated a Pod binding")
						}
						for _, op := range result.Operations {
							if op.Operation == runnerv1.RunnerService_RemovePreparedWorkload_FullMethodName || op.Operation == runnerv1.RunnerService_RemoveWorkloadAnchor_FullMethodName {
								t.Fatal("unbound revocation used ordinary Pod removal")
							}
						}
						live.trackRevocation(t, ctx, proof)
						live.anchorAbsent(t, ctx, proof.WorkloadAnchor)
						if _, err := live.kube.CoreV1().Pods(live.ns.Name).Get(ctx, "workload-"+cfg.WorkloadID, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
							t.Fatal("revoked Pod absence unconfirmed")
						}
						v := database.assertVolume(t, ctx, registry.client, originalVolume.Meta.Id)
						if !proto.Equal(v.ResourceAnchor, originalVolume.ResourceAnchor) || !proto.Equal(v.AnchorReservation, originalVolume.AnchorReservation) {
							t.Fatal("recovery replaced persistent workspace ownership")
						}
						if latePVC && checkpoint != "revocation-confirmed" {
							if v.GetBoundInstance().GetInstanceUid() != string(late.UID) || len(w.Preparation.Resources.RevocationObservation.Volumes) != 1 {
								t.Fatal("late PVC was not bound before cleanup confirmation")
							}
						} else if v.BoundInstance != nil || v.LifecycleRevision != 2 {
							t.Fatal("recovery invented physical state for an original unbound reservation")
						}
						cfg.Mode, cfg.WorkloadID = "start", uuid.NewString()
						first := run(t, cfg).Workload.Preparation.Binding
						if latePVC && first.Volumes[0].InstanceUid != string(late.UID) {
							t.Fatal("explicit follow-up replaced the late workspace")
						}
						live.probe(t, ctx, cfg, first, 0)
						cfg.Mode = "stop"
						run(t, cfg)
						live.absent(t, ctx, first)
						peerProof = live.probe(t, ctx, peerConfig, peer, peerProof.Heartbeat)
						t.Logf("three-process SIGKILL at %s; latePVC=%t; immutable revocation, no replay, retained ownership and peer heartbeat=%d verified", checkpoint, latePVC, peerProof.Heartbeat)
					})
					if t.Failed() {
						return
					}
				}
				if checkpointIndex == 1 {
					// Each bounded probe lives at most three minutes. Start an
					// explicit second peer turn between fault-injection groups.
					peerConfig.Mode = "stop"
					run(t, peerConfig)
					live.absent(t, ctx, peer)
					previousPeer := peer
					peerConfig.Mode, peerConfig.WorkloadID, peerConfig.Turn = "start", uuid.NewString(), 2
					peer = run(t, peerConfig).Workload.Preparation.Binding
					if !proto.Equal(previousPeer.Volumes[0], peer.Volumes[0]) {
						t.Fatal("explicit peer follow-up replaced its workspace")
					}
					peerProof = live.probe(t, ctx, peerConfig, peer, 0)
				}
			}
			peerConfig.Mode = "stop"
			run(t, peerConfig)
			live.absent(t, ctx, peer)
		})
	}
}
