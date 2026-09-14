package reconciler

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/clientcmd"
)

const retentionOwnerLabel = "agyn.io/volume-retention-test"

// This uses real runner RPCs and Kubernetes PVCs, but a deterministic registry
// fake. It is not a deployed platform/database or node-fencing test.
func TestLiveVolumeRetention(t *testing.T) {
	if os.Getenv("RETENTION_LIVE_TEST") != "trusted-local" {
		t.Skip("requires explicit trusted-local volume retention acceptance")
	}
	kubeconfig, binary := os.Getenv("RETENTION_KUBECONFIG"), os.Getenv("RETENTION_RUNNER_BINARY")
	if !filepath.IsAbs(kubeconfig) || !filepath.IsAbs(binary) {
		t.Fatal("explicit absolute kubeconfig and reviewed native runner fixture binary required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	live := newRetentionLiveNamespace(t, ctx, kubeconfig)
	native := startRetentionNativeRunner(t, ctx, binary, kubeconfig, live)
	f := newVolumeRetentionFixture(t)
	anchor := f.volume("anchor", runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING)
	foreign := f.volume("foreign", runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
	foreign.OrganizationId = uuid.NewString()
	closed := f.volume("closed", runnersv1.VolumeStatus_VOLUME_STATUS_DELETED)
	f.records = []*runnersv1.Volume{anchor, foreign, closed}
	claims := map[string]*corev1.PersistentVolumeClaim{}
	for _, key := range []string{"anchor", "foreign", "closed", "unknown"} {
		claims[key] = live.createClaim(t, ctx, "pvc-"+key, key)
	}
	late := f.volume("late", runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING)
	f.beforeRunnerList = func() {
		if f.registryLists != 1 {
			t.Fatal("late record must be created after the registry snapshot")
		}
		f.records = append(f.records, late)
		claims["late"] = live.createClaim(t, ctx, "pvc-late", "late")
		f.beforeRunnerList = nil
	}
	f.runner.listVolumes = func(ctx context.Context, req *runnerv1.ListVolumesRequest, opts ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
		f.runnerLists++
		if f.beforeRunnerList != nil {
			f.beforeRunnerList()
		}
		return native.ListVolumes(ctx, req, opts...)
	}
	f.runner.removeVolumeBound = func(ctx context.Context, req *runnerv1.RemoveVolumeBoundRequest, opts ...grpc.CallOption) (*runnerv1.RemoveVolumeBoundResponse, error) {
		f.removed = append(f.removed, req.GetExpected().GetInstanceId())
		return native.RemoveVolumeBound(ctx, req, opts...)
	}
	reconcile := func() {
		t.Helper()
		if err := f.reconciler.reconcileVolumes(ctx); err != nil {
			t.Fatal(err)
		}
	}
	retained := func() {
		t.Helper()
		for _, expected := range claims {
			current, err := live.kube.CoreV1().PersistentVolumeClaims(live.namespace).Get(ctx, expected.Name, metav1.GetOptions{})
			if err != nil || current.UID != expected.UID || current.DeletionTimestamp != nil || !reflect.DeepEqual(current.Spec, expected.Spec) || !reflect.DeepEqual(current.Labels, expected.Labels) {
				t.Fatalf("claim %q was removed or changed: %v", expected.Name, err)
			}
		}
	}
	reconcile()
	retained()
	if len(f.removed) != 0 || len(f.updated) != 1 || anchor.GetStatus() != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE || late.GetStatus() != runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING {
		t.Fatalf("first scan failed: removals %v, updates %v", f.removed, f.updated)
	}
	reconcile()
	retained()
	if len(f.removed) != 0 || late.GetStatus() != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE || late.GetInstanceId() != claims["late"].Name {
		t.Fatal("next scan did not recover the newly registered volume without deletion")
	}
	t.Logf("real runner retained all %d PVC UIDs across the cross-organization/closed/unknown/late-record scans", len(claims))

	claims["duplicate"] = live.createClaim(t, ctx, "pvc-duplicate", "anchor")
	checkedTestUpdate(t, anchor, &runnersv1.UpdateVolumeCheckedRequest{
		Id: anchor.Meta.Id, ExpectedRevision: anchor.LifecycleRevision,
		Operation: &runnersv1.UpdateVolumeCheckedRequest_BeginRemoval{BeginRemoval: &runnersv1.BeginVolumeRemoval{}},
	})
	originalIntent := proto.Clone(anchor.RemovalIntent).(*runnersv1.VolumeRemovalIntent)
	f.updated = nil
	for _, key := range []string{"anchor", ""} {
		claim, err := live.kube.CoreV1().PersistentVolumeClaims(live.namespace).Get(ctx, claims["duplicate"].Name, metav1.GetOptions{})
		if err != nil || claim.UID != claims["duplicate"].UID {
			t.Fatal("duplicate fixture claim changed before label update")
		}
		claim.Labels["volume_key"] = key
		claims["duplicate"], err = live.kube.CoreV1().PersistentVolumeClaims(live.namespace).Update(ctx, claim, metav1.UpdateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		reconcile()
		retained()
		if len(f.removed) != 0 || len(f.updated) != 0 || f.ownerMutations != 0 {
			t.Fatal("ambiguous native inventory authorized a mutation")
		}
	}
	t.Log("real duplicate-key and empty-key inventory blocked tracked deletion without record or owner mutation")
	claim, err := live.kube.CoreV1().PersistentVolumeClaims(live.namespace).Get(ctx, claims["duplicate"].Name, metav1.GetOptions{})
	if err != nil || claim.UID != claims["duplicate"].UID {
		t.Fatal("duplicate fixture claim changed before label repair")
	}
	claim.Labels["volume_key"] = "duplicate-now-untracked"
	claims["duplicate"], err = live.kube.CoreV1().PersistentVolumeClaims(live.namespace).Update(ctx, claim, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	missing, err := live.kube.CoreV1().PersistentVolumeClaims(live.namespace).Get(ctx, claims["anchor"].Name, metav1.GetOptions{})
	if err != nil || missing.UID != claims["anchor"].UID {
		t.Fatal("tracked claim changed before missing-label probe")
	}
	delete(missing.Labels, "volume_key")
	claims["anchor"], err = live.kube.CoreV1().PersistentVolumeClaims(live.namespace).Update(ctx, missing, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	reconcile()
	retained()
	if len(f.removed) != 0 || len(f.updated) != 0 || anchor.GetStatus() != runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING {
		t.Fatal("a missing native PVC label must not be reported as confirmed disk absence")
	}
	repaired := claims["anchor"].DeepCopy()
	repaired.Labels["volume_key"] = "anchor"
	claims["anchor"], err = live.kube.CoreV1().PersistentVolumeClaims(live.namespace).Update(ctx, repaired, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("missing label on the tracked native PVC failed inventory without closing its record")
	reconcile()
	if !slices.Equal(f.removed, []string{claims["anchor"].Name}) || len(f.updated) != 1 || f.updated[0].GetBeginRemoval() == nil ||
		anchor.Status != runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING || anchor.RemovalIntent.ConfirmedAt != nil {
		t.Fatalf("only tracked deprovisioning may delete, removals %v, updates %v", f.removed, f.updated)
	}
	if err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
		current, err := live.kube.CoreV1().PersistentVolumeClaims(live.namespace).Get(ctx, claims["anchor"].Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err == nil && current.UID != claims["anchor"].UID {
			return false, fmt.Errorf("tracked claim replaced")
		}
		return false, err
	}); err != nil {
		t.Fatal(err)
	}
	delete(claims, "anchor")
	retained()
	f.reconciler = newTestReconciler(Config{
		Agents: f.reconciler.agents, Runners: f.reconciler.runners, RunnerDialer: f.reconciler.runnerDialer,
	})
	reconcile()
	if anchor.GetStatus() != runnersv1.VolumeStatus_VOLUME_STATUS_DELETED || anchor.GetRemovalIntent().GetConfirmedAt() == nil ||
		len(f.removed) != 2 || len(f.updated) != 3 || f.updated[2].GetConfirmRemoval().GetIntentId() != originalIntent.Id ||
		!proto.Equal(anchor.RemovalIntent.Expected, originalIntent.Expected) {
		t.Fatal("restarted controller must confirm the original intent only after checked native absence")
	}
	retained()
	t.Logf("native checked removal deleted only the tracked empty claim; restarted controller confirmed the original intent; %d other PVCs retained unchanged", len(claims))
	verifyLiveSandboxVolumeCleanup(t, ctx, live, native)
	retained()
}

func verifyLiveSandboxVolumeCleanup(t *testing.T, ctx context.Context, live *retentionLiveNamespace, native runnerv1.RunnerServiceClient) {
	t.Helper()
	sandboxID := uuid.NewString()
	sandbox := &agentsv1.Sandbox{
		Meta: &agentsv1.EntityMeta{Id: sandboxID}, OrganizationId: testOrganizationID, OwnerId: "fixture-sandbox-user",
		Status: agentsv1.SandboxStatus_SANDBOX_STATUS_TERMINATED,
	}
	v := checkedTestSandboxVolume("sandbox-volume", sandboxID, runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING)
	claim := live.createClaimForVolume(t, ctx, "pvc-sandbox-workspace", v)
	deleted := 0
	agents := &testutil.FakeAgentsClient{
		GetSandboxFunc: func(_ context.Context, req *agentsv1.GetSandboxRequest, _ ...grpc.CallOption) (*agentsv1.GetSandboxResponse, error) {
			if req.GetId() != sandboxID {
				t.Fatal("sandbox binding queried the wrong owner")
			}
			return &agentsv1.GetSandboxResponse{Sandbox: sandbox}, nil
		},
		DeleteSandboxFunc: func(_ context.Context, req *agentsv1.DeleteSandboxRequest, _ ...grpc.CallOption) (*agentsv1.DeleteSandboxResponse, error) {
			if req.Id != sandboxID || v.RemovalIntent.GetConfirmedAt() == nil {
				t.Fatal("sandbox finalized without volume confirmation")
			}
			deleted++
			return &agentsv1.DeleteSandboxResponse{}, nil
		},
	}
	registry := &fakeRunnersClient{
		listVolumes: func(context.Context, *runnersv1.ListVolumesRequest, ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{proto.Clone(v).(*runnersv1.Volume)}}, nil
		},
		listWorkloads: func(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{}, nil
		},
		updateVolumeChecked: func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
			return checkedTestUpdate(t, v, req), nil
		},
	}
	dialer := &fakeRunnerDialer{dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
		if id != v.RunnerId {
			t.Fatal("sandbox cleanup changed runner")
		}
		return native, nil
	}}
	first := &Reconciler{agents: agents, runners: registry, runnerDialer: dialer}
	if err := first.reconcileSandbox(ctx, sandbox, time.Now()); err == nil || deleted != 0 || v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING || v.RemovalIntent.GetConfirmedAt() != nil {
		t.Fatalf("native sandbox deletion must remain pending: err=%v deleted=%d", err, deleted)
	}
	intent := proto.Clone(v.RemovalIntent).(*runnersv1.VolumeRemovalIntent)
	if intent.Expected.InstanceUid != string(claim.UID) || intent.Expected.InstanceId != claim.Name {
		t.Fatal("sandbox intent did not pin the observed native UID/name")
	}
	if err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
		current, err := live.kube.CoreV1().PersistentVolumeClaims(live.namespace).Get(ctx, claim.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err == nil && current.UID != claim.UID {
			return false, fmt.Errorf("sandbox claim replaced")
		}
		return false, err
	}); err != nil {
		t.Fatal(err)
	}
	restarted := &Reconciler{agents: agents, runners: registry, runnerDialer: dialer}
	if err := restarted.reconcileSandbox(ctx, sandbox, time.Now()); err != nil || deleted != 1 || v.RemovalIntent.GetConfirmedAt() == nil || v.RemovalIntent.Id != intent.Id || !proto.Equal(v.RemovalIntent.Expected, intent.Expected) {
		t.Fatalf("sandbox finalization did not resume the stored intent: err=%v deleted=%d", err, deleted)
	}
	t.Log("native sandbox workspace bound from inventory with user ownership checked; pending cleanup resumed with original UID before sandbox finalization")
}

type retentionLiveNamespace struct {
	kube          kubernetes.Interface
	namespace     string
	uid           types.UID
	runID         string
	owned         map[schema.GroupVersionResource]map[string]types.UID
	runnerAddress string
}

func retentionResource(group, name string) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: group, Version: "v1", Resource: name}
}

func newRetentionLiveNamespace(t *testing.T, ctx context.Context, kubeconfig string) *retentionLiveNamespace {
	t.Helper()
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Timeout = 10 * time.Second
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := metadata.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f := &retentionLiveNamespace{kube: kube, runID: uuid.NewString(), owned: map[schema.GroupVersionResource]map[string]types.UID{}}
	f.namespace = "orchestrator-volumes-" + f.runID[:12]
	for _, name := range []string{"pods", "services", "secrets", "configmaps", "serviceaccounts", "persistentvolumeclaims", "resourcequotas"} {
		f.owned[retentionResource("", name)] = map[string]types.UID{}
	}
	for _, name := range []string{"roles", "rolebindings"} {
		f.owned[retentionResource(rbacv1.GroupName, name)] = map[string]types.UID{}
	}
	storageClass := "unprovisioned-" + f.runID
	if _, err := kube.StorageV1().StorageClasses().Get(ctx, storageClass, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("test storage class must not exist")
	}
	attempted := false
	t.Cleanup(func() {
		if !attempted {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		ns, err := kube.CoreV1().Namespaces().Get(cleanup, f.namespace, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return
		}
		if err != nil || ns.Labels[retentionOwnerLabel] != f.runID || f.uid != "" && ns.UID != f.uid {
			t.Error("namespace ownership unconfirmed; cleanup refused")
			return
		}
		for gvr, names := range f.owned {
			items, err := meta.Resource(gvr).Namespace(f.namespace).List(cleanup, metav1.ListOptions{})
			if err != nil {
				t.Error(err)
				return
			}
			for _, item := range items.Items {
				if gvr.Resource == "serviceaccounts" && item.Name == "default" {
					continue
				}
				if gvr.Resource == "configmaps" {
					cm, err := kube.CoreV1().ConfigMaps(f.namespace).Get(cleanup, item.Name, metav1.GetOptions{})
					if err == nil && cm.UID == item.UID && retentionNamespaceCA(cm) {
						if cm.Labels["trust.cert-manager.io/bundle"] == cm.Name {
							bundle, err := meta.Resource(schema.GroupVersionResource{Group: "trust.cert-manager.io", Version: "v1alpha1", Resource: "bundles"}).Get(cleanup, cm.Name, metav1.GetOptions{})
							if err != nil || bundle.UID != cm.OwnerReferences[0].UID {
								t.Error("trust bundle controller identity unconfirmed; cleanup refused")
								return
							}
						}
						continue
					}
					t.Error("unknown configmap; cleanup refused")
					return
				}
				uid, planned := names[item.Name]
				if !planned || uid != "" && uid != item.UID || item.Labels[retentionOwnerLabel] != f.runID {
					t.Errorf("unknown/replaced %s %s; cleanup refused", gvr.Resource, item.Name)
					return
				}
			}
		}
		claims, err := kube.CoreV1().PersistentVolumeClaims(f.namespace).List(cleanup, metav1.ListOptions{})
		if err != nil {
			t.Error(err)
			return
		}
		for _, claim := range claims.Items {
			if claim.Spec.VolumeName != "" || claim.Status.Phase == corev1.ClaimBound || claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName != storageClass {
				t.Error("claim acquired backing storage; cleanup refused")
				return
			}
		}
		if err := kube.CoreV1().Namespaces().Delete(cleanup, f.namespace, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &ns.UID, ResourceVersion: &ns.ResourceVersion}}); err != nil {
			t.Error(err)
			return
		}
		if err := wait.PollUntilContextTimeout(cleanup, time.Second, 50*time.Second, true, func(ctx context.Context) (bool, error) {
			current, err := kube.CoreV1().Namespaces().Get(ctx, f.namespace, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			if err == nil && current.UID != f.uid {
				return false, fmt.Errorf("namespace replaced during cleanup")
			}
			return false, err
		}); err != nil {
			t.Error(err)
		} else {
			t.Logf("cleanup confirmed namespace=%s uid=%s absent", f.namespace, ns.UID)
		}
	})
	attempted = true
	labels := map[string]string{retentionOwnerLabel: f.runID}
	ns, err := kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: f.namespace, Labels: labels}}, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			attempted = false
		}
		t.Fatal(err)
	}
	f.uid = ns.UID
	t.Logf("volume retention run=%s namespace=%s uid=%s", f.runID, f.namespace, f.uid)
	f.owned[retentionResource("", "serviceaccounts")]["volume-inspector"] = ""
	account, err := kube.CoreV1().ServiceAccounts(f.namespace).Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "volume-inspector", Labels: labels}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	f.owned[retentionResource("", "serviceaccounts")][account.Name] = account.UID
	grantRetentionNamespaceRead(t, ctx, f)
	f.owned[retentionResource(rbacv1.GroupName, "roles")]["volume-inspector"] = ""
	role, err := kube.RbacV1().Roles(f.namespace).Create(ctx, &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "volume-inspector", Labels: labels}, Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"persistentvolumeclaims"}, Verbs: []string{"get", "list", "delete"}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	f.owned[retentionResource(rbacv1.GroupName, "roles")][role.Name] = role.UID
	f.owned[retentionResource(rbacv1.GroupName, "rolebindings")]["volume-inspector"] = ""
	binding, err := kube.RbacV1().RoleBindings(f.namespace).Create(ctx, &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "volume-inspector", Labels: labels}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: role.Name}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Namespace: f.namespace, Name: account.Name}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	f.owned[retentionResource(rbacv1.GroupName, "rolebindings")][binding.Name] = binding.UID
	f.owned[retentionResource("", "resourcequotas")]["empty-claims-only"] = ""
	quota, err := kube.CoreV1().ResourceQuotas(f.namespace).Create(ctx, &corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: "empty-claims-only", Labels: labels}, Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{"count/pods": resource.MustParse("0"), "persistentvolumeclaims": resource.MustParse("6"), "requests.storage": resource.MustParse("6Mi")}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	f.owned[retentionResource("", "resourcequotas")][quota.Name] = quota.UID
	if err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 15*time.Second, true, func(ctx context.Context) (bool, error) {
		current, err := kube.CoreV1().ResourceQuotas(f.namespace).Get(ctx, quota.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		count, ok := current.Status.Hard["count/pods"]
		return ok && count.IsZero(), nil
	}); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *retentionLiveNamespace) createClaim(t *testing.T, ctx context.Context, name, key string) *corev1.PersistentVolumeClaim {
	t.Helper()
	return f.createClaimForVolume(t, ctx, name, checkedTestVolume(key, runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING))
}

func (f *retentionLiveNamespace) createClaimForVolume(t *testing.T, ctx context.Context, name string, volume *runnersv1.Volume) *corev1.PersistentVolumeClaim {
	t.Helper()
	storageClass := "unprovisioned-" + f.runID
	f.owned[retentionResource("", "persistentvolumeclaims")][name] = ""
	labels := checkedTestInstance(volume, name, "unused").IdentityLabels
	labels[retentionOwnerLabel] = f.runID
	claim, err := f.kube.CoreV1().PersistentVolumeClaims(f.namespace).Create(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}, Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: &storageClass, AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Mi")}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	f.owned[retentionResource("", "persistentvolumeclaims")][name] = claim.UID
	return claim
}

func startRetentionNativeRunner(t *testing.T, ctx context.Context, binary, kubeconfig string, live *retentionLiveNamespace) runnerv1.RunnerServiceClient {
	t.Helper()
	cmd := exec.Command(binary)
	cmd.Env = []string{"RETENTION_LIVE_TEST=trusted-local", "RETENTION_KUBECONFIG=" + kubeconfig, "RETENTION_NAMESPACE=" + live.namespace, "RETENTION_NAMESPACE_UID=" + string(live.uid), "RETENTION_RUN_ID=" + live.runID}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("native fixture exit: %v", err)
			}
		case <-time.After(15 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Error("native fixture required forced termination")
		}
	})
	line := make(chan []byte, 1)
	go func() {
		reader := bufio.NewReaderSize(stdout, 4096)
		data, _, err := reader.ReadLine()
		if err == nil {
			line <- data
		} else {
			line <- nil
		}
	}()
	var ready struct{ Address, Namespace, UID string }
	select {
	case data := <-line:
		if err := json.Unmarshal(data, &ready); err != nil {
			t.Fatal("native fixture did not produce a readiness receipt")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(20 * time.Second):
		t.Fatal("native fixture readiness timed out")
	}
	host, _, err := net.SplitHostPort(ready.Address)
	if err != nil || host != "127.0.0.1" || ready.Namespace != live.namespace || ready.UID != string(live.uid) {
		t.Fatal("native fixture receipt does not match this disposable namespace")
	}
	live.runnerAddress = ready.Address
	conn, err := grpc.NewClient(ready.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return runnerv1.NewRunnerServiceClient(conn)
}

// Match only controller-injected, certificate-only configmaps during cleanup.
func retentionNamespaceCA(cm *corev1.ConfigMap) bool {
	if len(cm.Data) != 1 || len(cm.BinaryData) != 0 {
		return false
	}
	key := "ca.crt"
	if cm.Name != "" && cm.Labels["trust.cert-manager.io/bundle"] == cm.Name {
		if len(cm.OwnerReferences) != 1 {
			return false
		}
		owner := cm.OwnerReferences[0]
		if owner.APIVersion != "trust.cert-manager.io/v1alpha1" || owner.Kind != "Bundle" || owner.Name != cm.Name || owner.UID == "" || owner.Controller == nil || !*owner.Controller {
			return false
		}
		for name, value := range cm.Data {
			key = name
			if cm.Annotations["trust.cert-manager.io/hash"] != fmt.Sprintf("%x", sha256.Sum256([]byte(value))) {
				return false
			}
		}
	} else if cm.Name == "istio-ca-root-cert" && cm.Labels["istio.io/config"] == "true" {
		key = "root-cert.pem"
	} else if cm.Name != "kube-root-ca.crt" {
		return false
	}
	remaining := bytes.TrimSpace([]byte(cm.Data[key]))
	if len(remaining) == 0 {
		return false
	}
	for len(remaining) > 0 {
		if !strings.HasPrefix(string(remaining), "-----BEGIN CERTIFICATE-----") {
			return false
		}
		block, rest := pem.Decode(remaining)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return false
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !cert.IsCA {
			return false
		}
		remaining = bytes.TrimSpace(rest)
	}
	return true
}

func TestRetentionNamespaceCACleanup(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	certificate := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	for _, name := range []string{"kube-root-ca.crt", "istio-ca-root-cert", "test-trust-bundle"} {
		key := "ca.crt"
		if name == "istio-ca-root-cert" {
			key = "root-cert.pem"
		}
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"istio.io/config": "true"}}, Data: map[string]string{key: certificate}}
		invalidCases := []string{"name", "extra-data", "binary-data", "non-certificate", "trailing-data", "missing-label"}
		if name == "test-trust-bundle" {
			controller := true
			cm.Labels = map[string]string{"trust.cert-manager.io/bundle": name}
			cm.Annotations = map[string]string{"trust.cert-manager.io/hash": fmt.Sprintf("%x", sha256.Sum256([]byte(certificate)))}
			cm.OwnerReferences = []metav1.OwnerReference{{APIVersion: "trust.cert-manager.io/v1alpha1", Kind: "Bundle", Name: name, UID: "bundle-uid", Controller: &controller}}
			invalidCases = append(invalidCases, "missing-owner", "wrong-owner", "missing-controller", "wrong-hash")
		}
		if !retentionNamespaceCA(cm) {
			t.Fatal("valid controller CA rejected")
		}
		for _, invalid := range invalidCases {
			if invalid == "missing-label" && name == "kube-root-ca.crt" {
				continue
			}
			t.Run(name+"/"+invalid, func(t *testing.T) {
				bad := cm.DeepCopy()
				switch invalid {
				case "name":
					bad.Name = "user-config"
				case "extra-data":
					bad.Data["user-data"] = "keep"
				case "binary-data":
					bad.BinaryData = map[string][]byte{"data": []byte("keep")}
				case "non-certificate":
					bad.Data[key] = "keep"
				case "trailing-data":
					bad.Data[key] += "keep"
				case "missing-label":
					bad.Labels = nil
				case "missing-owner":
					bad.OwnerReferences = nil
				case "wrong-owner":
					bad.OwnerReferences[0].Name = "other-bundle"
				case "missing-controller":
					bad.OwnerReferences[0].Controller = nil
				case "wrong-hash":
					bad.Annotations = nil
				}
				if retentionNamespaceCA(bad) {
					t.Fatal("unexpected data accepted as a controller CA")
				}
			})
		}
	}
}
