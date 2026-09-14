package reconciler

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
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
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

// Real registry/migrations/PostgreSQL, native runner/Kubernetes and controller
// subprocesses. No models, A2A driver, Pod starts or deployed Agents service.
func TestLiveCheckedVolumeStack(t *testing.T) {
	if os.Getenv("CHECKED_VOLUME_STACK_TEST") != "trusted-local" {
		t.Skip("requires explicit isolated registry/controller/runner acceptance")
	}
	kubeconfig, registryBinary, runnerBinary := os.Getenv("RETENTION_KUBECONFIG"), os.Getenv("CHECKED_REGISTRY_BINARY"), os.Getenv("RETENTION_RUNNER_BINARY")
	for _, path := range []string{kubeconfig, registryBinary, runnerBinary} {
		if !filepath.IsAbs(path) {
			t.Fatal("explicit absolute kubeconfig and reviewed fixture binaries required")
		}
	}
	for _, sandbox := range []bool{false, true} {
		name := "agent"
		if sandbox {
			name = "sandbox"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			live := newRetentionLiveNamespace(t, ctx, kubeconfig)
			native := startRetentionNativeRunner(t, ctx, runnerBinary, kubeconfig, live)
			database := newCheckedStackDatabase(t, ctx, os.Getenv("CHECKED_POSTGRES_IMAGE"))
			registry := startCheckedRegistry(t, ctx, registryBinary, database.config)
			uncredentialed, err := grpc.NewClient(registry.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal("uncredentialed fixture probe unavailable")
			}
			_, probeErr := runnersv1.NewRunnersServiceClient(uncredentialed).ListRunners(ctx, &runnersv1.ListRunnersRequest{})
			_ = uncredentialed.Close()
			if status.Code(probeErr) != codes.Unauthenticated {
				t.Fatal("fixture registry allowed a call without its private credential")
			}
			if _, err := registry.client.GetRunner(ctx, &runnersv1.GetRunnerRequest{Id: database.config.RunnerID}); status.Code(err) != codes.PermissionDenied {
				t.Fatal("fixture registry allowed an out-of-scope RPC")
			}
			cfg := checkedControllerConfig{
				RegistryAddress: registry.address, RegistryToken: database.config.Token, RunnerAddress: live.runnerAddress,
				RunnerID: database.config.RunnerID, OrganizationID: database.config.OrganizationID,
				OwnerID: uuid.NewString(), AgentID: uuid.NewString(), ThreadID: uuid.NewString(), DefinitionID: uuid.NewString(),
				Sandbox: sandbox,
			}
			cfg.VolumeID = uuid.NewSHA1(uuid.NameSpaceOID, []byte(cfg.OwnerID+":"+cfg.DefinitionID)).String()
			run := func(mode string) checkedControllerResult {
				t.Helper()
				next := cfg
				next.Mode, next.Barrier = mode, ""
				return startCheckedController(t, next).finish(t, ctx)
			}
			if result := run("create"); result.ReconcileError || result.CreatedRecords != 1 {
				t.Fatal("real controller did not create exactly one owned registry generation")
			}
			v := database.assertVolume(t, ctx, registry.client, cfg.VolumeID)
			if v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING || v.LifecycleRevision != 1 || v.BoundInstance != nil {
				t.Fatal("new controller generation was not independently persisted")
			}
			claim := live.createClaimForVolume(t, ctx, "pvc-"+cfg.OwnerID, v)
			if result := run("volumes"); result.ReconcileError || result.count("bind", "OK") != 1 {
				t.Fatal("real controller did not bind the native inventory")
			}
			v = database.assertVolume(t, ctx, registry.client, cfg.VolumeID)
			if v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE || v.BoundInstance.InstanceUid != string(claim.UID) || v.GetInstanceId() != claim.Name {
				t.Fatal("registry binding does not name the native PVC")
			}
			target := proto.Clone(v.BoundInstance).(*runnerv1.VolumeListItem)
			boundRevision := v.LifecycleRevision
			workload := func(volume *runnersv1.Volume) *runnersv1.CreateWorkloadRequest {
				return &runnersv1.CreateWorkloadRequest{
					Id: uuid.NewString(), RunnerId: volume.RunnerId, OrganizationId: volume.OrganizationId,
					OwnerKind: volume.OwnerKind, OwnerId: volume.OwnerId, AgentId: volume.AgentId, ThreadId: volume.ThreadId,
					Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
				}
			}
			createWorkload := func(req *runnersv1.CreateWorkloadRequest) {
				t.Helper()
				resp, err := registry.client.CreateWorkload(ctx, req)
				if err != nil || resp.GetWorkload().GetMeta().GetId() != req.Id {
					t.Fatalf("registry admission: %v", err)
				}
			}
			confirmUnusedReservation := func(id string) {
				t.Helper()
				// No StartWorkload is sent in this fixture and its namespace forbids
				// Pods. This retires an unused reservation, not a Pod-removal proof.
				phase := runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED
				resp, err := registry.client.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{
					Id: id, Status: &phase, RemovalConfirmedAt: timestamppb.New(time.Now().Add(-2 * time.Hour)),
				})
				if err != nil || resp.GetWorkload().GetRemovalConfirmedAt() == nil {
					t.Fatalf("unused reservation retirement: %v", err)
				}
			}
			previous := workload(v)
			createWorkload(previous)
			confirmUnusedReservation(previous.Id)

			cfg.Mode, cfg.Barrier = "cleanup", "before-begin"
			stale := startCheckedController(t, cfg)
			stale.awaitBarrier(t, ctx)
			current := workload(v)
			createWorkload(current)
			other := proto.Clone(checkedTestCreateRequest(v)).(*runnersv1.CreateVolumeRequest)
			other.Id, other.OwnerId = uuid.NewString(), uuid.NewString()
			if !sandbox {
				other.ThreadId = uuid.NewString()
			}
			otherCreated, err := registry.client.CreateVolumeChecked(ctx, &runnersv1.CreateVolumeCheckedRequest{Volume: other})
			if err != nil {
				t.Fatalf("unrelated owner creation blocked: %v", err)
			}
			createWorkload(workload(otherCreated.Volume))
			checkedStackJSON(t, filepath.Join(stale.directory, "release.json"), map[string]bool{"release": true})
			result := stale.finish(t, ctx)
			if result.count("begin", "FailedPrecondition") != 1 || result.count("native-remove", "") != 0 || result.count("confirm", "") != 0 || result.DeletedSandbox != 0 {
				t.Fatal("stale idle scan bypassed real admission or reached native deletion")
			}
			v = database.assertVolume(t, ctx, registry.client, cfg.VolumeID)
			if v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE || v.LifecycleRevision != boundRevision || v.RemovalIntent != nil {
				t.Fatal("admission-winning race changed the volume")
			}
			assertCheckedStackClaim(t, ctx, live, claim)
			t.Log("real admission won after the controller idle scan; stale begin was refused; another owner admitted independently")

			failed := runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED
			if _, err := registry.client.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{Id: current.Id, Status: &failed}); err != nil {
				t.Fatal(err)
			}
			if _, err := registry.client.CreateWorkload(ctx, workload(v)); status.Code(err) != codes.FailedPrecondition {
				t.Fatal("billing-ended failure released an unconfirmed reservation")
			}
			confirmUnusedReservation(current.Id)
			cfg.Barrier = "after-begin"
			crashed := startCheckedController(t, cfg)
			crashed.awaitBarrier(t, ctx)
			v = database.assertVolume(t, ctx, registry.client, cfg.VolumeID)
			if v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING || v.RemovalIntent.ConfirmedAt != nil || !proto.Equal(v.RemovalIntent.Expected, target) {
				t.Fatal("begin did not durably pin the original deletion target")
			}
			intent := proto.Clone(v.RemovalIntent).(*runnersv1.VolumeRemovalIntent)
			if _, err := registry.client.CreateWorkload(ctx, workload(v)); status.Code(err) != codes.FailedPrecondition {
				t.Fatal("deletion-winning race admitted a replacement workload")
			}
			assertCheckedStackClaim(t, ctx, live, claim)
			crashed.process.kill(t)
			registry.process.kill(t)
			registry = startCheckedRegistry(t, ctx, registryBinary, database.config)
			cfg.RegistryAddress = registry.address
			v = database.assertVolume(t, ctx, registry.client, cfg.VolumeID)
			if !proto.Equal(v.RemovalIntent, intent) {
				t.Fatal("registry process replacement lost the committed intent")
			}

			cfg.Barrier = "after-native"
			pending := startCheckedController(t, cfg)
			result = pending.awaitBarrier(t, ctx)
			if result.count("native-remove", "OK") != 1 || result.count("confirm", "") != 0 || result.DeletedSandbox != 0 {
				t.Fatal("first native delete acknowledgement finalized the owner")
			}
			assertCheckedNativeState(t, result, runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING)
			v = database.assertVolume(t, ctx, registry.client, cfg.VolumeID)
			if v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING || !proto.Equal(v.RemovalIntent, intent) {
				t.Fatal("native acknowledgement changed the durable pending intent")
			}
			if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
				current, err := live.kube.CoreV1().PersistentVolumeClaims(live.namespace).Get(ctx, claim.Name, metav1.GetOptions{})
				if apierrors.IsNotFound(err) {
					return true, nil
				}
				if err == nil && current.UID != claim.UID {
					t.Fatal("tracked claim was replaced during absence observation")
				}
				return false, err
			}); err != nil {
				t.Fatal(err)
			}
			pending.process.kill(t)
			registry.process.kill(t)
			registry = startCheckedRegistry(t, ctx, registryBinary, database.config)
			cfg.RegistryAddress = registry.address
			v = database.assertVolume(t, ctx, registry.client, cfg.VolumeID)
			if v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING || !proto.Equal(v.RemovalIntent, intent) {
				t.Fatal("process death after native deletion lost the unconfirmed intent")
			}
			cfg.Barrier = "after-confirm"
			confirming := startCheckedController(t, cfg)
			result = confirming.awaitBarrier(t, ctx)
			if result.count("native-remove", "OK") != 1 || result.count("confirm", "OK") != 1 || result.DeletedSandbox != 0 {
				t.Fatal("replacement controller did not persist confirmation before owner finalization")
			}
			assertCheckedNativeState(t, result, runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT)
			v = database.assertVolume(t, ctx, registry.client, cfg.VolumeID)
			if v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_DELETED || v.RemovalIntent.ConfirmedAt == nil || v.RemovalIntent.Id != intent.Id || !proto.Equal(v.RemovalIntent.Expected, target) {
				t.Fatal("deletion confirmation did not preserve the original intent/UID")
			}
			confirmed := proto.Clone(v.RemovalIntent).(*runnersv1.VolumeRemovalIntent)
			confirming.process.kill(t)
			registry.process.kill(t)
			registry = startCheckedRegistry(t, ctx, registryBinary, database.config)
			cfg.RegistryAddress = registry.address
			v = database.assertVolume(t, ctx, registry.client, cfg.VolumeID)
			if v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_DELETED || !proto.Equal(v.RemovalIntent, confirmed) {
				t.Fatal("process death after registry confirmation lost its committed result")
			}
			result = run("cleanup")
			if result.count("native-remove", "") != 0 || result.count("confirm", "") != 0 || result.ReconcileError || sandbox && result.DeletedSandbox != 1 {
				t.Fatal("confirmation recovery repeated deletion or failed to finalize the owner")
			}
			t.Log("registry/controller SIGKILL after begin, native deletion and registry confirmation preserved each committed state; owner finalization resumed without repeating confirmed deletion")

			if result := run("create"); result.ReconcileError || result.CreatedRecords != 1 || result.count("reopen", "OK") != 1 {
				t.Fatal("explicit reopen did not create an owned next generation")
			}
			v = database.assertVolume(t, ctx, registry.client, cfg.VolumeID)
			if v.BoundInstance != nil || v.RemovalIntent != nil || v.RemovedAt != nil || v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING {
				t.Fatal("reopen preserved authority over the deleted physical generation")
			}
			replacement := live.createClaimForVolume(t, ctx, claim.Name, v)
			if replacement.UID == claim.UID {
				t.Fatal("fixture did not create a replacement UID")
			}
			if _, err := native.RemoveVolumeChecked(ctx, &runnerv1.RemoveVolumeCheckedRequest{Expected: target}); status.Code(err) != codes.FailedPrecondition {
				t.Fatal("old target deleted a new physical generation")
			}
			if result := run("volumes"); result.ReconcileError || result.count("bind", "OK") != 1 {
				t.Fatal("replacement generation did not bind through the real registry")
			}
			v = database.assertVolume(t, ctx, registry.client, cfg.VolumeID)
			if v.BoundInstance.InstanceUid != string(replacement.UID) || v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE {
				t.Fatal("replacement generation reused the old binding")
			}
			if result := run("create"); result.ReconcileError || result.CreatedRecords != 0 || result.count("reopen", "") != 0 {
				t.Fatal("open-generation reuse acquired another attempt's compensation ownership")
			}
			assertCheckedStackClaim(t, ctx, live, replacement)
			t.Log("explicit new generation bound a replacement UID; old-target replay was refused and open-generation reuse preserved ownership")
		})
	}
}

func assertCheckedNativeState(t *testing.T, result checkedControllerResult, state runnerv1.VolumeRemovalState) {
	t.Helper()
	for _, operation := range result.Operations {
		if operation.Operation == "native-remove" && operation.NativeState != state.String() {
			t.Fatalf("native state=%s, want %s", operation.NativeState, state)
		}
	}
}

func assertCheckedStackClaim(t *testing.T, ctx context.Context, live *retentionLiveNamespace, expected *corev1.PersistentVolumeClaim) {
	t.Helper()
	current, err := live.kube.CoreV1().PersistentVolumeClaims(live.namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil || current.UID != expected.UID || current.DeletionTimestamp != nil || !reflect.DeepEqual(current.Spec, expected.Spec) || !reflect.DeepEqual(current.Labels, expected.Labels) {
		t.Fatal("native claim identity/specification changed unexpectedly")
	}
}
