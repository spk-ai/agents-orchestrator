package reconciler

import (
	"context"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func grantRetentionNamespaceRead(t *testing.T, ctx context.Context, f *retentionLiveNamespace) {
	t.Helper()
	name := f.namespace + "-backend"
	labels := map[string]string{retentionOwnerLabel: f.runID}
	var roleUID, bindingUID types.UID
	roleAttempted, bindingAttempted := false, false
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if bindingAttempted {
			binding, err := f.kube.RbacV1().ClusterRoleBindings().Get(cleanup, name, metav1.GetOptions{})
			if !apierrors.IsNotFound(err) {
				if err != nil || binding.Labels[retentionOwnerLabel] != f.runID || bindingUID != "" && binding.UID != bindingUID {
					t.Error("backend role binding ownership unknown; cleanup refused")
					return
				}
				if err := f.kube.RbacV1().ClusterRoleBindings().Delete(cleanup, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &binding.UID, ResourceVersion: &binding.ResourceVersion}}); err != nil {
					t.Error(err)
					return
				}
				if _, err := f.kube.RbacV1().ClusterRoleBindings().Get(cleanup, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
					t.Error("backend role binding absence unconfirmed")
					return
				}
			}
		}
		if roleAttempted {
			role, err := f.kube.RbacV1().ClusterRoles().Get(cleanup, name, metav1.GetOptions{})
			if !apierrors.IsNotFound(err) {
				if err != nil || role.Labels[retentionOwnerLabel] != f.runID || roleUID != "" && role.UID != roleUID {
					t.Error("backend role ownership unknown; cleanup refused")
					return
				}
				if err := f.kube.RbacV1().ClusterRoles().Delete(cleanup, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &role.UID, ResourceVersion: &role.ResourceVersion}}); err != nil {
					t.Error(err)
					return
				}
				if _, err := f.kube.RbacV1().ClusterRoles().Get(cleanup, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
					t.Error("backend role absence unconfirmed")
					return
				}
			}
		}
		t.Logf("backend namespace-read RBAC cleanup confirmed name=%s", name)
	})
	roleAttempted = true
	role, err := f.kube.RbacV1().ClusterRoles().Create(ctx, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"namespaces"}, ResourceNames: []string{f.namespace}, Verbs: []string{"get"}}}}, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			roleAttempted = false
		}
		t.Fatal(err)
	}
	roleUID = role.UID
	bindingAttempted = true
	binding, err := f.kube.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name},
		Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Namespace: f.namespace, Name: "volume-inspector"}}}, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			bindingAttempted = false
		}
		t.Fatal(err)
	}
	bindingUID = binding.UID
}
