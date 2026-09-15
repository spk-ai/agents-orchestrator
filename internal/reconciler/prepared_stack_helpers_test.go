package reconciler

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/yaml"
)

type preparedStackNamespace struct {
	kube                   kubernetes.Interface
	ns                     *corev1.Namespace
	run, token, kubeconfig string
	bindings               map[string]*runnerv1.WorkloadBinding
	claims                 map[string]types.UID
	runner                 runnerv1.RunnerServiceClient
	role                   *rbacv1.ClusterRole
	roleBinding            *rbacv1.ClusterRoleBinding
	cleaned                bool
}

func newPreparedStackNamespace(t *testing.T, ctx context.Context, kubeconfig, chart string) *preparedStackNamespace {
	t.Helper()
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal("fixture Kubernetes configuration unavailable")
	}
	cfg.Timeout = 10 * time.Second
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal("fixture Kubernetes client unavailable")
	}
	run := uuid.NewString()
	f := &preparedStackNamespace{kube: kube, run: run, token: checkedStackSecret(t), kubeconfig: kubeconfig, bindings: map[string]*runnerv1.WorkloadBinding{}, claims: map[string]types.UID{}}
	labels := map[string]string{preparedStackLabel: run}
	ns, err := kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "orchestrator-prepared-" + run[:12], Labels: labels}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	f.ns = ns
	t.Cleanup(func() { f.cleanupNamespace(t) })
	t.Logf("prepared stack namespace=%s uid=%s run=%s", ns.Name, ns.UID, run)
	data, err := os.ReadFile(chart)
	if err != nil {
		t.Fatal("reviewed native chart values unavailable")
	}
	var values struct {
		RBAC struct {
			Rules []rbacv1.PolicyRule `json:"rules"`
		} `json:"rbac"`
	}
	if yaml.Unmarshal(data, &values) != nil || len(values.RBAC.Rules) == 0 {
		t.Fatal("reviewed chart RBAC required")
	}
	if _, err = kube.CoreV1().ServiceAccounts(ns.Name).Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "runner", Labels: labels}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = kube.RbacV1().Roles(ns.Name).Create(ctx, &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "runner", Labels: labels}, Rules: values.RBAC.Rules}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	subject := rbacv1.Subject{Kind: "ServiceAccount", Name: "runner", Namespace: ns.Name}
	if _, err = kube.RbacV1().RoleBindings(ns.Name).Create(ctx, &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "runner", Labels: labels}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "runner"}, Subjects: []rbacv1.Subject{subject}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.role, err = kube.RbacV1().ClusterRoles().Create(ctx, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: ns.Name + "-backend", Labels: labels}, Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"namespaces"}, ResourceNames: []string{ns.Name}, Verbs: []string{"get"}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	f.roleBinding, err = kube.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: f.role.Name, Labels: labels}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: f.role.Name}, Subjects: []rbacv1.Subject{subject}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = kube.NetworkingV1().NetworkPolicies(ns.Name).Create(ctx, &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: "deny-network", Labels: labels}, Spec: networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{}, PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress}}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = kube.CoreV1().ResourceQuotas(ns.Name).Create(ctx, &corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: "fixture", Labels: labels}, Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
		corev1.ResourcePods: resource.MustParse("4"), corev1.ResourcePersistentVolumeClaims: resource.MustParse("20"), corev1.ResourceRequestsStorage: resource.MustParse("20Mi"),
		corev1.ResourceRequestsCPU: resource.MustParse("500m"), corev1.ResourceRequestsMemory: resource.MustParse("512Mi"), corev1.ResourceLimitsCPU: resource.MustParse("2"), corev1.ResourceLimitsMemory: resource.MustParse("1Gi"),
	}}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *preparedStackNamespace) cleanupNamespace(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ns, err := f.kube.CoreV1().Namespaces().Get(ctx, f.ns.Name, metav1.GetOptions{})
	if err != nil || ns.UID != f.ns.UID || ns.Labels[preparedStackLabel] != f.run {
		t.Error("namespace identity changed; cleanup refused")
		return
	}
	pods, err := f.kube.CoreV1().Pods(ns.Name).List(ctx, metav1.ListOptions{})
	if err != nil || len(pods.Items) != 0 {
		t.Error("prepared Pods remain; namespace cleanup refused")
		return
	}
	claims, err := f.kube.CoreV1().PersistentVolumeClaims(ns.Name).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Error(err)
		return
	}
	for _, v := range claims.Items {
		if f.claims[v.Name] != v.UID || v.Labels[preparedStackLabel] != f.run {
			t.Error("unknown/replaced workspace; namespace cleanup refused")
			return
		}
		for _, hold := range v.Finalizers {
			if strings.HasPrefix(hold, "agyn.io/workload-") {
				t.Error("prepared workspace hold remains; cleanup refused")
				return
			}
		}
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
		secrets, err := f.kube.CoreV1().Secrets(ns.Name).List(ctx, metav1.ListOptions{})
		return err == nil && len(secrets.Items) == 0, err
	}); err != nil {
		t.Error("temporary Secret GC not confirmed; cleanup refused")
		return
	}
	services, err := f.kube.CoreV1().Services(ns.Name).List(ctx, metav1.ListOptions{})
	if err != nil || len(services.Items) != 0 {
		t.Error("unexpected fixture Service; cleanup refused")
		return
	}
	if err := f.kube.CoreV1().Namespaces().Delete(ctx, ns.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &ns.UID, ResourceVersion: &ns.ResourceVersion}}); err != nil {
		t.Error(err)
		return
	}
	if err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 50*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := f.kube.CoreV1().Namespaces().Get(ctx, ns.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}); err != nil {
		t.Error("namespace absence unconfirmed")
		return
	}
	if f.roleBinding != nil {
		b, err := f.kube.RbacV1().ClusterRoleBindings().Get(ctx, f.roleBinding.Name, metav1.GetOptions{})
		if err != nil || b.UID != f.roleBinding.UID || b.Labels[preparedStackLabel] != f.run {
			t.Error("cluster binding identity changed; cleanup refused")
			return
		}
		if err := f.kube.RbacV1().ClusterRoleBindings().Delete(ctx, b.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &b.UID, ResourceVersion: &b.ResourceVersion}}); err != nil {
			t.Error(err)
			return
		}
		if _, err := f.kube.RbacV1().ClusterRoleBindings().Get(ctx, b.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Error("cluster binding absence unconfirmed")
			return
		}
	}
	if f.role != nil {
		r, err := f.kube.RbacV1().ClusterRoles().Get(ctx, f.role.Name, metav1.GetOptions{})
		if err != nil || r.UID != f.role.UID || r.Labels[preparedStackLabel] != f.run {
			t.Error("cluster role identity changed; cleanup refused")
			return
		}
		if err := f.kube.RbacV1().ClusterRoles().Delete(ctx, r.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &r.UID, ResourceVersion: &r.ResourceVersion}}); err != nil {
			t.Error(err)
			return
		}
		if _, err := f.kube.RbacV1().ClusterRoles().Get(ctx, r.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Error("cluster role absence unconfirmed")
			return
		}
	}
	t.Logf("confirmed namespace and GET-only cluster RBAC absent namespace=%s uid=%s; Secret GC observed; no hold stripping", ns.Name, ns.UID)
}

func (f *preparedStackNamespace) track(t *testing.T, ctx context.Context, b *runnerv1.WorkloadBinding) {
	t.Helper()
	if b == nil || !preparedUUID(b.WorkloadId) || !preparedUUID(b.InstanceUid) || b.BackendId != "kubernetes-namespace/v1/"+f.ns.Name+"/"+string(f.ns.UID) || len(b.Volumes) != 1 {
		t.Fatal("invalid fixture native receipt")
	}
	pod, err := f.kube.CoreV1().Pods(f.ns.Name).Get(ctx, "workload-"+b.WorkloadId, metav1.GetOptions{})
	if err != nil || string(pod.UID) != b.InstanceUid || pod.Labels[preparedStackLabel] != f.run {
		t.Fatal("independent prepared Pod identity differs")
	}
	workloadOwner := f.anchor(t, ctx, b.Anchor)
	if len(pod.OwnerReferences) != 1 || pod.OwnerReferences[0].Kind != "ConfigMap" || pod.OwnerReferences[0].Name != workloadOwner.Name || pod.OwnerReferences[0].UID != workloadOwner.UID {
		t.Fatal("Pod did not retain its persisted workload owner")
	}
	if old := f.bindings[b.WorkloadId]; old != nil && !samePreparedBinding(old, b) {
		t.Fatal("fixture receipt changed")
	}
	f.bindings[b.WorkloadId] = proto.Clone(b).(*runnerv1.WorkloadBinding)
	for _, v := range b.Volumes {
		claim, err := f.kube.CoreV1().PersistentVolumeClaims(f.ns.Name).Get(ctx, v.InstanceId, metav1.GetOptions{})
		if err != nil || string(claim.UID) != v.InstanceUid || claim.Labels[preparedStackLabel] != f.run {
			t.Fatal("independent prepared PVC identity differs")
		}
		volumeOwner := f.anchor(t, ctx, v.Anchor)
		if volumeOwner.UID == workloadOwner.UID || len(claim.OwnerReferences) != 1 || claim.OwnerReferences[0].Kind != "ConfigMap" || claim.OwnerReferences[0].Name != volumeOwner.Name || claim.OwnerReferences[0].UID != volumeOwner.UID {
			t.Fatal("PVC does not have independent durable ownership")
		}
		if old := f.claims[claim.Name]; old != "" && old != claim.UID {
			t.Fatal("fixture workspace replaced")
		}
		f.claims[claim.Name] = claim.UID
	}
	secrets, err := f.kube.CoreV1().Secrets(f.ns.Name).List(ctx, metav1.ListOptions{LabelSelector: "agyn.io/workload-id=" + b.WorkloadId})
	if err != nil || len(secrets.Items) != 1 {
		t.Fatal("one temporary inline-file Secret required")
	}
	for _, s := range secrets.Items {
		if len(s.OwnerReferences) != 1 || s.OwnerReferences[0].UID != pod.UID || s.OwnerReferences[0].Name != pod.Name {
			t.Fatal("temporary Secret lacks exact Pod ownership")
		}
	}
}

func (f *preparedStackNamespace) anchor(t *testing.T, ctx context.Context, a *runnerv1.ResourceAnchor) *corev1.ConfigMap {
	t.Helper()
	if a == nil || !preparedUUID(a.ResourceId) || !preparedUUID(a.InstanceUid) || a.BackendId != "kubernetes-namespace/v1/"+f.ns.Name+"/"+string(f.ns.UID) {
		t.Fatal("complete fixture anchor identity required")
	}
	prefix := "workload-anchor-"
	if a.Kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME {
		prefix = "volume-anchor-"
	} else if a.Kind != runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD {
		t.Fatal("unsupported fixture anchor kind")
	}
	cm, err := f.kube.CoreV1().ConfigMaps(f.ns.Name).Get(ctx, prefix+a.ResourceId, metav1.GetOptions{})
	if err != nil || string(cm.UID) != a.InstanceUid || cm.DeletionTimestamp != nil || cm.Immutable == nil || !*cm.Immutable ||
		len(cm.OwnerReferences) != 0 || !maps.Equal(cm.Labels, a.IdentityLabels) || cm.Annotations["agyn.io/resource-anchor-version"] != "v1" {
		t.Fatal("independent native resource owner differs from persisted anchor")
	}
	expected := proto.Clone(a).(*runnerv1.ResourceAnchor)
	expected.InstanceUid = ""
	intent := &runnerv1.ResourceAnchor{}
	if len(cm.Data) != 1 || protojson.Unmarshal([]byte(cm.Data["identity.json"]), intent) != nil || !proto.Equal(expected, intent) {
		t.Fatal("native owner intent differs from registry")
	}
	return cm
}

func (f *preparedStackNamespace) anchorAbsent(t *testing.T, ctx context.Context, a *runnerv1.ResourceAnchor) {
	t.Helper()
	if a == nil || a.Kind != runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD || !preparedUUID(a.ResourceId) ||
		a.BackendId != "kubernetes-namespace/v1/"+f.ns.Name+"/"+string(f.ns.UID) {
		t.Fatal("exact fixture workload anchor required")
	}
	if _, err := f.kube.CoreV1().ConfigMaps(f.ns.Name).Get(ctx, "workload-anchor-"+a.ResourceId, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("independent workload anchor absence unconfirmed")
	}
}

// Captured receipts are test-only cleanup authority. In the unknown-prepare
// crash case the production registry deliberately never receives this receipt.
func (f *preparedStackNamespace) cleanupExecution(t *testing.T) {
	t.Helper()
	if f.cleaned {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	pods, err := f.kube.CoreV1().Pods(f.ns.Name).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Error(err)
		return
	}
	for _, pod := range pods.Items {
		b := f.bindings[pod.Labels["agyn.io/workload-id"]]
		if b == nil || string(pod.UID) != b.InstanceUid || pod.Labels[preparedStackLabel] != f.run {
			t.Error("untracked Pod; cleanup refused")
			return
		}
	}
	for _, b := range f.bindings {
		if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			response, err := f.runner.RemovePreparedWorkload(ctx, &runnerv1.RemovePreparedWorkloadRequest{Expected: b})
			if status.Code(err) == codes.Aborted {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if !samePreparedBinding(response.Binding, b) {
				return false, fmt.Errorf("cleanup receipt changed")
			}
			return response.State == runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_ABSENT, nil
		}); err != nil {
			t.Error(err)
			return
		}
	}
	f.cleaned = true
}

func startPreparedNative(t *testing.T, ctx context.Context, binary string, f *preparedStackNamespace) (*checkedStackProcess, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "native.json")
	checkedStackJSON(t, path, map[string]string{"Kubeconfig": f.kubeconfig, "Namespace": f.ns.Name, "NamespaceUID": string(f.ns.UID), "RunID": f.run, "Token": f.token})
	cmd := exec.Command(binary)
	cmd.Env = []string{"PREPARED_STACK_TEST=trusted-local", "PREPARED_RUNNER_CONFIG_FILE=" + path}
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	p := startCheckedStackProcess(t, cmd)
	line := make(chan []byte, 1)
	go func() {
		data, prefix, err := bufio.NewReaderSize(stdout, 4096).ReadLine()
		if err != nil || prefix {
			line <- nil
		} else {
			line <- data
		}
	}()
	var ready struct{ Address, Namespace, UID, RunID string }
	select {
	case data := <-line:
		if json.Unmarshal(data, &ready) != nil {
			t.Fatal("native fixture readiness missing")
		}
	case <-p.done:
		t.Fatal("native fixture exited before readiness")
	case <-ctx.Done():
		t.Fatal("native fixture readiness deadline expired")
	}
	host, _, err := net.SplitHostPort(ready.Address)
	if err != nil || host != "127.0.0.1" || ready.Namespace != f.ns.Name || ready.UID != string(f.ns.UID) || ready.RunID != f.run {
		t.Fatal("native fixture readiness identity mismatch")
	}
	conn, err := grpc.NewClient(ready.Address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoke(metadata.AppendToOutgoingContext(ctx, "x-prepared-runner-fixture-token", f.token), method, req, reply, cc, opts...)
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	f.runner = runnerv1.NewRunnerServiceClient(conn)
	// Register after the process/connection cleanups, including on replacement.
	t.Cleanup(func() { f.cleanupExecution(t) })
	t.Logf("real prepared runner ready pid=%d namespace=%s", p.cmd.Process.Pid, f.ns.Name)
	return p, ready.Address
}

func (d *checkedStackDatabase) assertPreparedWorkload(t *testing.T, ctx context.Context, client runnersv1.RunnersServiceClient, id string) *runnersv1.Workload {
	t.Helper()
	if _, err := d.inspect(ctx); err != nil {
		t.Fatal("owned database required")
	}
	if !preparedUUID(id) || !regexp.MustCompile(`^checked_[a-f0-9]{32}$`).MatchString(d.config.Schema) {
		t.Fatal("canonical database probe identity required")
	}
	query := `SELECT json_build_object('status',w.status,'phase',w.preparation_phase,'revision',w.preparation_revision,'backend',w.prepared_backend_id,
		'volumes',w.prepared_volume_ids,'binding',w.prepared_binding,'observation',w.prepared_removal_observation,'confirmed',w.removal_confirmed_at,
		'anchors',w.resource_anchors,'anchoredOwner',g.resource_anchors_required,
		'owner',w.owner_id,'runner',w.runner_id,'organization',w.organization_id,'thread',w.thread_id,'agent',w.agent_id,
		'pinBackend',g.prepared_backend_id,'pinRunner',g.prepared_runner_id,'pinOrganization',g.prepared_organization_id,'pinThread',g.prepared_thread_id,'pinAgent',g.prepared_agent_id)::text
		FROM "` + d.config.Schema + `".workloads w JOIN "` + d.config.Schema + `".runtime_volume_admission_guards g USING(owner_kind,owner_id) WHERE w.id='` + id + `'`
	data, err := checkedStackDocker(ctx, "exec", d.id, "psql", "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "runners_controller_acceptance", "-c", query)
	if err != nil {
		t.Fatal("independent prepared workload read failed")
	}
	var stored struct {
		Status, Phase, Backend, Owner, Runner, Organization, Thread, Agent string
		PinBackend, PinRunner, PinOrganization, PinThread, PinAgent        string
		Revision                                                           uint64
		Volumes                                                            []string
		Binding, Observation, Anchors                                      json.RawMessage
		AnchoredOwner                                                      bool
		Confirmed                                                          *time.Time
	}
	if json.Unmarshal(data, &stored) != nil {
		t.Fatal("independent prepared workload read invalid")
	}
	response, err := client.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: id})
	if err != nil || validatePreparedWorkload(response.GetWorkload()) != nil {
		t.Fatalf("prepared workload read failed: %v", err)
	}
	w := response.Workload
	if stored.Status != strings.ToLower(strings.TrimPrefix(w.Status.String(), "WORKLOAD_STATUS_")) || stored.Phase != strings.ToLower(strings.TrimPrefix(w.Preparation.Phase.String(), "PREPARED_WORKLOAD_PHASE_")) ||
		stored.Revision != w.Preparation.Revision || stored.Backend != w.Preparation.BackendId || !slices.Equal(stored.Volumes, w.Preparation.VolumeIds) ||
		stored.Owner != w.OwnerId || stored.Runner != w.RunnerId || stored.Organization != w.OrganizationId || stored.Thread != w.ThreadId || stored.Agent != w.AgentId ||
		stored.PinBackend != stored.Backend || stored.PinRunner != stored.Runner || stored.PinOrganization != stored.Organization || stored.PinThread != stored.Thread || stored.PinAgent != stored.Agent ||
		!stored.AnchoredOwner || w.Preparation.Resources == nil {
		t.Fatal("prepared registry response/owner pin differs from independent SQL read")
	}
	if (stored.Confirmed == nil) != (w.RemovalConfirmedAt == nil) || stored.Confirmed != nil && !stored.Confirmed.Equal(w.RemovalConfirmedAt.AsTime()) {
		t.Fatal("removal confirmation differs from independent SQL read")
	}
	for _, v := range []struct {
		raw     json.RawMessage
		message proto.Message
	}{{stored.Binding, w.Preparation.Binding}, {stored.Observation, w.Preparation.RemovalObservation}, {stored.Anchors, w.Preparation.Resources}} {
		if string(v.raw) == "null" {
			if v.message.ProtoReflect().IsValid() {
				t.Fatal("registry invented unpersisted native receipt")
			}
			continue
		}
		expected := v.message.ProtoReflect().New().Interface()
		if protojson.Unmarshal(v.raw, expected) != nil || !proto.Equal(expected, v.message) {
			t.Fatal("native receipt differs from independent SQL read")
		}
	}
	for _, id := range w.Preparation.VolumeIds {
		v := d.assertVolume(t, ctx, client, id)
		query := `SELECT json_build_object('anchor',resource_anchor,'reservation',anchor_reservation)::text FROM "` + d.config.Schema + `".volumes WHERE id='` + id + `'`
		data, err := checkedStackDocker(ctx, "exec", d.id, "psql", "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "runners_controller_acceptance", "-c", query)
		var ownership struct{ Anchor, Reservation json.RawMessage }
		if err != nil || json.Unmarshal(data, &ownership) != nil {
			t.Fatal("independent volume ownership read failed")
		}
		for _, field := range []struct {
			raw     json.RawMessage
			message proto.Message
		}{{ownership.Anchor, v.ResourceAnchor}, {ownership.Reservation, v.AnchorReservation}} {
			if string(field.raw) == "null" {
				if field.message.ProtoReflect().IsValid() {
					t.Fatal("registry invented a volume anchor or reservation receipt")
				}
				continue
			}
			expected := field.message.ProtoReflect().New().Interface()
			if protojson.Unmarshal(field.raw, expected) != nil || !proto.Equal(expected, field.message) {
				t.Fatal("volume ownership differs from independent SQL read")
			}
		}
		if w.Preparation.Resources.Workload != nil && !proto.Equal(workloadVolumeAnchor(w, id), v.ResourceAnchor) {
			t.Fatal("workload and persistent volume have different native owners")
		}
	}
	return w
}
