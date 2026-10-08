package leader

import (
	"context"
	"testing"
	"time"

	"github.com/agynio/agents-orchestrator/internal/config"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

func testLock(client *fake.Clientset) *resourcelock.LeaseLock {
	return &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: "agents-orchestrator", Namespace: "orchestrator-test"},
		Client:     client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: "orchestrator-0"},
	}
}

// The elector gets exactly the configured timings, and client-go accepts both
// the historical defaults and lengthened ones.
func TestElectionConfigUsesConfiguredTimings(t *testing.T) {
	for _, timings := range []config.LeaderElectionTimings{
		{LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 2 * time.Second},
		{LeaseDuration: 120 * time.Second, RenewDeadline: 90 * time.Second, RetryPeriod: 10 * time.Second},
	} {
		election := electionConfig(testLock(fake.NewClientset()), timings, func(context.Context) {})
		if election.LeaseDuration != timings.LeaseDuration || election.RenewDeadline != timings.RenewDeadline || election.RetryPeriod != timings.RetryPeriod {
			t.Fatalf("timings not applied: %+v", election)
		}
		if !election.ReleaseOnCancel || election.Callbacks.OnStoppedLeading == nil {
			t.Fatal("expected the Lease to be released on shutdown and a lost-leadership log")
		}
		if _, err := leaderelection.NewLeaderElector(election); err != nil {
			t.Fatalf("client-go refused %+v: %v", timings, err)
		}
	}
}

// The configured lease duration is what the Lease advertises to standbys, and
// a cancelled leader releases it so a replacement need not wait it out.
func TestLeaseAdvertisesConfiguredDurationAndIsReleased(t *testing.T) {
	client := fake.NewClientset()
	leader := &Leader{
		client:    client,
		namespace: "orchestrator-test",
		name:      "agents-orchestrator",
		identity:  "orchestrator-0",
		timings:   config.LeaderElectionTimings{LeaseDuration: 120 * time.Second, RenewDeadline: 90 * time.Second, RetryPeriod: 10 * time.Second},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	leases := make(chan *coordinationv1.Lease, 1)
	leader.onStarted = func(leadCtx context.Context) {
		lease, err := client.CoordinationV1().Leases("orchestrator-test").Get(leadCtx, "agents-orchestrator", metav1.GetOptions{})
		if err != nil {
			t.Errorf("get lease: %v", err)
		}
		leases <- lease
		cancel()
	}
	if err := leader.Run(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case lease := <-leases:
		if lease == nil || lease.Spec.LeaseDurationSeconds == nil || *lease.Spec.LeaseDurationSeconds != 120 {
			t.Fatalf("lease does not advertise 120s: %+v", lease)
		}
		if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "orchestrator-0" {
			t.Fatalf("unexpected holder: %+v", lease.Spec.HolderIdentity)
		}
	default:
		t.Fatal("never started leading")
	}
	released, err := client.CoordinationV1().Leases("orchestrator-test").Get(context.Background(), "agents-orchestrator", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if released.Spec.HolderIdentity != nil && *released.Spec.HolderIdentity != "" {
		t.Fatalf("lease not released on cancel: holder %q", *released.Spec.HolderIdentity)
	}
}
