package reconciler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

// This seeds only a new owned fixture PVC, writes an explicit completed first
// turn with a credential-free Pod, then removes that Pod before adoption.
func seedMigrationWorkspace(t *testing.T, ctx context.Context, live *preparedStackNamespace, client runnersv1.RunnersServiceClient, cfg preparedControllerConfig, checked bool) (*runnersv1.BeginVolumeAnchorMigrationRequest, *corev1.PersistentVolumeClaim) {
	t.Helper()
	metadata, _, infos := cfg.request()
	info := infos[0]
	records, err := buildVolumeRecords(infos)
	if err != nil || len(records) != 1 {
		t.Fatal("valid fixture volume definition required")
	}
	labels := resourceAnchorLabels(&runnersv1.Workload{OwnerKind: metadata.OwnerKind, OwnerId: cfg.OwnerID, AgentId: metadata.AgentId}, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, info.Key(), cfg.HumanOwnerID)
	labels[preparedStackLabel] = live.run
	storageClass := "local-path"
	claim, err := live.kube.CoreV1().PersistentVolumeClaims(live.ns.Name).Create(ctx, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: info.Spec.PersistentName, Labels: labels},
		Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, StorageClassName: &storageClass,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Mi")}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	live.claims[claim.Name] = claim.UID
	automount := false
	program := fmt.Sprintf(`const fs=require('fs');fs.writeFileSync('/workspace/owner',%q,{flag:'wx'});fs.writeFileSync('/workspace/turns',%q,{flag:'wx'});`, cfg.OwnerID, cfg.OwnerID+":1\n")
	pod, err := live.kube.CoreV1().Pods(live.ns.Name).Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "seed-" + cfg.OwnerID, Labels: map[string]string{preparedStackLabel: live.run}},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: &automount,
			Containers: []corev1.Container{{Name: "seed", Image: cfg.Image, Command: []string{"node", "-e", program}, VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: "/workspace"}},
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
					Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("128Mi")}}}},
			Volumes: []corev1.Volume{{Name: "workspace", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim.Name}}}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 60*time.Second, true, func(ctx context.Context) (bool, error) {
		p, err := live.kube.CoreV1().Pods(live.ns.Name).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if p.UID != pod.UID || p.Status.Phase == corev1.PodFailed {
			return false, fmt.Errorf("original fixture seed failed or changed")
		}
		return p.Status.Phase == corev1.PodSucceeded, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := live.kube.CoreV1().Pods(live.ns.Name).Delete(ctx, pod.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pod.UID}}); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := live.kube.CoreV1().Pods(live.ns.Name).Get(ctx, pod.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}); err != nil {
		t.Fatal(err)
	}
	claim, err = live.kube.CoreV1().PersistentVolumeClaims(live.ns.Name).Get(ctx, claim.Name, metav1.GetOptions{})
	if err != nil || claim.Status.Phase != corev1.ClaimBound {
		t.Fatal("original fixture PVC not bound")
	}
	inventory, err := live.runner.ListVolumes(ctx, &runnerv1.ListVolumesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var previous *runnerv1.VolumeListItem
	for _, v := range inventory.Volumes {
		if v.VolumeKey == info.Key() {
			previous = v
		}
	}
	if previous == nil || previous.InstanceUid != string(claim.UID) || previous.InstanceId != claim.Name || previous.Anchor != nil {
		t.Fatal("native original PVC observation missing")
	}
	raw := &runnersv1.CreateVolumeRequest{Id: info.Key(), VolumeId: cfg.DefinitionID, OwnerKind: metadata.OwnerKind, OwnerId: cfg.OwnerID, ThreadId: metadata.ThreadId, AgentId: metadata.AgentId,
		RunnerId: cfg.RunnerID, OrganizationId: cfg.OrganizationID, SizeGb: records[0].sizeGB, Status: runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING}
	var v *runnersv1.Volume
	if checked {
		created, err := client.CreateVolumeChecked(ctx, &runnersv1.CreateVolumeCheckedRequest{Volume: raw})
		if err != nil {
			t.Fatal(err)
		}
		bound, err := client.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{Id: raw.Id, ExpectedRevision: created.Volume.LifecycleRevision,
			Operation: &runnersv1.UpdateVolumeCheckedRequest_Bind{Bind: &runnersv1.BindVolumeInstance{Instance: previous}}})
		if err != nil {
			t.Fatal(err)
		}
		v = bound.Volume
	} else {
		created, err := client.CreateVolume(ctx, raw)
		if err != nil {
			t.Fatal(err)
		}
		phase := runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE
		bound, err := client.UpdateVolume(ctx, &runnersv1.UpdateVolumeRequest{Id: created.Volume.Meta.Id, Status: &phase, InstanceId: &previous.InstanceId})
		if err != nil {
			t.Fatal(err)
		}
		v = bound.Volume
	}
	return &runnersv1.BeginVolumeAnchorMigrationRequest{Id: uuid.NewString(), OwnerKind: metadata.OwnerKind, OwnerId: cfg.OwnerID, RunnerId: cfg.RunnerID, OrganizationId: cfg.OrganizationID, BackendId: inventory.BackendId,
		Sources: []*runnersv1.VolumeAnchorMigrationSource{{VolumeId: raw.Id, ExpectedRevision: v.LifecycleRevision, Previous: previous}}}, claim
}

func TestLiveVolumeAnchorMigrationStack(t *testing.T) {
	if os.Getenv("PREPARED_STACK_TEST") != "trusted-local" {
		t.Skip("explicit trusted-local migration stack opt-in required")
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
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
			defer cancel()
			live := newPreparedStackNamespace(t, ctx, kubeconfig, chart)
			nativeProcess, nativeAddress := startPreparedNative(t, ctx, nativeBinary, live)
			database := newCheckedStackDatabase(t, ctx, os.Getenv("CHECKED_POSTGRES_IMAGE"))
			database.config.PreparedWorkloads, database.config.VolumeMigration = true, true
			registry := startCheckedRegistry(t, ctx, registryBinary, database.config)
			for i, checkpoint := range []string{"begin", "native-reserved", "reserved", "native-applied", "applied", "native-ready", "ready", "complete"} {
				if !t.Run(checkpoint, func(t *testing.T) {
					cfg := preparedControllerConfig{RegistryAddress: registry.address, RegistryToken: database.config.Token, RunnerAddress: nativeAddress, RunnerToken: live.token,
						RunnerID: database.config.RunnerID, OrganizationID: database.config.OrganizationID, OwnerID: uuid.NewString(), ThreadID: uuid.NewString(), AgentID: uuid.NewString(), DefinitionID: uuid.NewString(),
						WorkloadID: uuid.NewString(), HumanOwnerID: uuid.NewString(), RunID: live.run, Image: os.Getenv("PREPARED_NODE_IMAGE"), Mode: "start", Turn: 2, Sandbox: sandbox}
					plan, original := seedMigrationWorkspace(t, ctx, live, registry.client, cfg, i%2 == 0)
					job := volumeMigrationConfig{RegistryAddress: registry.address, RegistryToken: database.config.Token, RunnerAddress: nativeAddress, RunnerToken: live.token, Plan: mustMigrationJSON(t, plan), Barrier: checkpoint}
					child := startVolumeMigrator(t, job)
					reached := child.read(t, ctx, "reached.json")
					if reached.Stage != checkpoint {
						t.Fatal("coordinator stopped at the wrong committed boundary")
					}
					persisted, err := registry.client.GetVolumeAnchorMigration(ctx, &runnersv1.GetVolumeAnchorMigrationRequest{OwnerKind: plan.OwnerKind, OwnerId: plan.OwnerId})
					if err != nil {
						t.Fatal(err)
					}
					if persisted.Migration.Complete != (checkpoint == "complete") {
						t.Fatal("owner admission opened at a partial boundary")
					}
					pods, err := live.kube.CoreV1().Pods(live.ns.Name).List(ctx, metav1.ListOptions{})
					if err != nil || len(pods.Items) != 0 {
						t.Fatal("migration dispatched execution")
					}
					child.process.kill(t)
					nativeProcess.kill(t)
					registry.process.kill(t)
					nativeProcess, nativeAddress = startPreparedNative(group, ctx, nativeBinary, live)
					registry = startCheckedRegistry(group, ctx, registryBinary, database.config)
					job.RegistryAddress, job.RunnerAddress, job.Barrier = registry.address, nativeAddress, ""
					finished := startVolumeMigrator(t, job).finish(t, ctx)
					a := finished.Entries[0].Adoption
					if prior := persisted.Migration.Entries[0].Adoption; prior != nil && !proto.Equal(prior, a) {
						t.Fatal("restart replaced persisted native receipt")
					}
					v := database.assertVolume(t, ctx, registry.client, plan.Sources[0].VolumeId)
					if !proto.Equal(v.AnchorAdoption, a) || v.AnchorReservation != nil || !proto.Equal(v.AnchorAdoption.Previous, plan.Sources[0].Previous) {
						t.Fatal("migration rewrote original storage provenance")
					}
					claim, err := live.kube.CoreV1().PersistentVolumeClaims(live.ns.Name).Get(ctx, original.Name, metav1.GetOptions{})
					if err != nil {
						t.Fatal(err)
					}
					spec, _ := json.Marshal(original.Spec)
					hash := sha256.Sum256(spec)
					if claim.UID != original.UID || !reflect.DeepEqual(claim.Spec, original.Spec) || !maps.Equal(claim.Labels, original.Labels) || a.PvcSpecSha256 != hex.EncodeToString(hash[:]) {
						t.Fatal("adoption replaced/resized original native PVC")
					}
					journal, err := live.kube.CoreV1().ConfigMaps(live.ns.Name).Get(ctx, "volume-adoption-"+a.Previous.VolumeKey, metav1.GetOptions{})
					if err != nil || string(journal.UID) != a.InstanceUid || journal.DeletionTimestamp != nil || journal.Immutable == nil || !*journal.Immutable {
						t.Fatal("original adoption journal not retained")
					}
					stored := &runnerv1.VolumeAnchorAdoption{}
					if protojson.Unmarshal([]byte(journal.Data["adoption.json"]), stored) != nil {
						t.Fatal("invalid retained native adoption journal")
					}
					stored.InstanceUid = string(journal.UID)
					if !proto.Equal(stored, a) {
						t.Fatal("registry receipt differs from independently read native journal")
					}
					cfg.RegistryAddress, cfg.RunnerAddress = registry.address, nativeAddress
					for turn := 2; turn <= 3; turn++ {
						cfg.Mode, cfg.Turn, cfg.WorkloadID = "start", turn, uuid.NewString()
						result := startPreparedController(t, cfg).finish(t, ctx)
						if result.Error || result.Binding == nil {
							t.Fatalf("migrated controller followup failed: %s", result.ErrorCode)
						}
						live.track(t, ctx, result.Binding)
						if result.Binding.Volumes[0].InstanceUid != string(original.UID) {
							t.Fatal("followup allocated replacement storage")
						}
						live.probe(t, ctx, cfg, result.Binding, 0)
						cfg.Mode = "stop"
						stopped := startPreparedController(t, cfg).finish(t, ctx)
						if stopped.Error {
							t.Fatal("followup compute removal failed")
						}
						assertPreparedNoRedispatch(t, stopped)
						live.absent(t, ctx, result.Binding)
						if after := database.assertVolume(t, ctx, registry.client, v.Meta.Id); !proto.Equal(v, after) {
							t.Fatal("followup changed durable migration history")
						}
					}
					t.Logf("migration boundary=%s checked-source=%t original-pvc=%s journal=%s retained; turns 1,2,3 verified once", checkpoint, i%2 == 0, original.UID, journal.UID)
				}) {
					return
				}
			}
		})
	}
}
