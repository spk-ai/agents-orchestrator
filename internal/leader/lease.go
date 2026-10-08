package leader

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/k8sclient"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type Leader struct {
	client    kubernetes.Interface
	namespace string
	name      string
	identity  string
	timings   config.LeaderElectionTimings
	onStarted func(context.Context)
}

func New(cfg *config.Config, onStarted func(context.Context)) (*Leader, error) {
	if onStarted == nil {
		return nil, fmt.Errorf("onStarted must be provided")
	}
	identity := os.Getenv("HOSTNAME")
	if identity == "" {
		return nil, fmt.Errorf("HOSTNAME must be set")
	}
	namespace, err := k8sclient.ResolveNamespace(cfg.LeaseNamespace, "lease")
	if err != nil {
		return nil, err
	}
	name := cfg.LeaseName
	if name == "" {
		name = "agents-orchestrator"
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubernetes config: %w", err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}

	return &Leader{
		client:    client,
		namespace: namespace,
		name:      name,
		identity:  identity,
		timings:   cfg.LeaderElection,
		onStarted: onStarted,
	}, nil
}

// Run campaigns for the Lease and runs onStarted while this process holds it.
// It returns when ctx ends (releasing the Lease) or when a renewal has kept
// failing for the renew deadline. client-go's elector is single-use and the
// leader workload's context is already cancelled by then, so the caller exits
// and the restarted process campaigns again; a standby takes over after the
// lease duration.
func (l *Leader) Run(ctx context.Context) error {
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      l.name,
			Namespace: l.namespace,
		},
		Client: l.client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: l.identity,
		},
	}

	elector, err := leaderelection.NewLeaderElector(electionConfig(lock, l.timings, l.onStarted))
	if err != nil {
		return fmt.Errorf("create leader elector: %w", err)
	}

	log.Printf("leader: campaigning for lease %s/%s (lease %s, renew deadline %s, retry %s)",
		l.namespace, l.name, l.timings.LeaseDuration, l.timings.RenewDeadline, l.timings.RetryPeriod)
	elector.Run(ctx)
	return nil
}

func electionConfig(lock resourcelock.Interface, timings config.LeaderElectionTimings, onStarted func(context.Context)) leaderelection.LeaderElectionConfig {
	return leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: timings.LeaseDuration,
		RenewDeadline: timings.RenewDeadline,
		RetryPeriod:   timings.RetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: onStarted,
			OnStoppedLeading: func() {
				log.Printf("leader: lost leadership")
			},
		},
		ReleaseOnCancel: true,
	}
}
