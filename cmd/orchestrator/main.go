package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	groupsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/groups/v1"
	imageproxyv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/image_proxy/v1"
	imagesv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/images/v1"
	llmv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/llm/v1"
	meteringv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/metering/v1"
	notificationsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/notifications/v1"
	organizationsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/organizations/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/k8sclient"
	"github.com/agynio/agents-orchestrator/internal/leader"
	"github.com/agynio/agents-orchestrator/internal/reconciler"
	"github.com/agynio/agents-orchestrator/internal/runnerdial"
	"github.com/agynio/agents-orchestrator/internal/subscriber"
	"github.com/agynio/agents-orchestrator/internal/zitimanager"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("orchestrator: %v", err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}

	closeConn := func(conn *grpc.ClientConn) {
		if conn == nil {
			return
		}
		_ = conn.Close()
	}

	threadsConn, err := grpc.DialContext(ctx, cfg.ThreadsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial threads: %w", err)
	}
	defer closeConn(threadsConn)

	notificationsConn, err := grpc.DialContext(ctx, cfg.NotificationsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial notifications: %w", err)
	}
	defer closeConn(notificationsConn)

	agentsConn, err := grpc.DialContext(
		ctx,
		cfg.AgentsAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("dial agents: %w", err)
	}
	defer closeConn(agentsConn)

	secretsConn, err := grpc.DialContext(ctx, cfg.SecretsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial secrets: %w", err)
	}
	defer closeConn(secretsConn)

	llmConn, err := grpc.DialContext(ctx, cfg.LLMAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial llm: %w", err)
	}
	defer closeConn(llmConn)

	runnersConn, err := grpc.NewClient(cfg.RunnersAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial runners: %w", err)
	}
	defer closeConn(runnersConn)

	meteringConn, err := grpc.NewClient(cfg.MeteringServiceAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial metering: %w", err)
	}
	defer closeConn(meteringConn)

	var (
		runnerDialer   runnerdial.RunnerDialer
		zitiMgmtConn   *grpc.ClientConn
		zitiMgmtClient zitimgmtv1.ZitiManagementServiceClient
	)
	// TODO: The E2E cluster does not yet deploy ziti-management or identities,
	// so we support a direct runner dial path for now. Remove this fallback
	// once ziti-management is part of the platform stack.
	if cfg.ZitiEnabled {
		zitiMgmtConn, err = grpc.NewClient(cfg.ZitiManagementAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial ziti management: %w", err)
		}
		zitiMgmtClient = zitimgmtv1.NewZitiManagementServiceClient(zitiMgmtConn)
		manager, err := zitimanager.New(ctx, zitiMgmtClient, cfg.ZitiEnrollmentTimeout, cfg.ZitiLeaseRenewalInterval)
		if err != nil {
			return err
		}
		go func() {
			if err := <-manager.IdentityLost(); err != nil {
				log.Fatalf("terminating: %v", err)
			}
		}()
		go manager.RunLeaseRenewal(ctx)
		runnerDialer = runnerdial.NewDialer(manager)
	} else {
		runnerConn, err := grpc.NewClient(cfg.RunnerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial runner: %w", err)
		}
		defer closeConn(runnerConn)
		runnerClient := runnerv1.NewRunnerServiceClient(runnerConn)
		runnerDialer = runnerdial.NewFallbackDialer(runnerClient)
	}
	defer runnerDialer.Close()
	defer closeConn(zitiMgmtConn)

	threadsClient := threadsv1.NewThreadsServiceClient(threadsConn)
	notificationsClient := notificationsv1.NewNotificationsServiceClient(notificationsConn)
	agentsClient := agentsv1.NewAgentsServiceClient(agentsConn)
	secretsClient := secretsv1.NewSecretsServiceClient(secretsConn)
	runnersClient := runnersv1.NewRunnersServiceClient(runnersConn)
	meteringClient := meteringv1.NewMeteringServiceClient(meteringConn)
	var groupsClient groupsv1.GroupsServiceClient
	var groupsConn *grpc.ClientConn
	if cfg.GroupSyncEnabled {
		groupsConn, err = grpc.NewClient(cfg.GroupsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial groups: %w", err)
		}
		defer closeConn(groupsConn)
		groupsClient = groupsv1.NewGroupsServiceClient(groupsConn)
	}
	subscriber := subscriber.New(notificationsClient, cfg.PlatformIdentityID)
	egressCANamespace, err := k8sclient.ResolveNamespace(cfg.EgressCANamespace, "egress CA")
	if err != nil {
		return err
	}
	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("load kubernetes config: %w", err)
	}
	kubeClient, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return fmt.Errorf("create kubernetes client: %w", err)
	}
	egressCACert, err := assembler.LoadEgressCACertificate(ctx, assembler.NewKubernetesSecretGetter(kubeClient.CoreV1()), egressCANamespace)
	if err != nil {
		return err
	}
	assembledWorkloads := assembler.NewWithRunnersAndEgressCA(agentsClient, runnersClient, secretsClient, &cfg, egressCACert).
		WithLLM(llmv1.NewLLMServiceClient(llmConn))

	// The catalog path is optional: without it, references stay as the agents
	// service stored them and no credential is minted, which is the
	// pre-catalog behaviour.
	var imageProxyClient reconciler.ImageProxyClient
	if cfg.ImagesAddress != "" && cfg.OrganizationsAddress != "" && cfg.ImageProxyAddress != "" {
		imagesConn, err := grpc.DialContext(ctx, cfg.ImagesAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial images: %w", err)
		}
		defer closeConn(imagesConn)
		organizationsConn, err := grpc.DialContext(ctx, cfg.OrganizationsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial organizations: %w", err)
		}
		defer closeConn(organizationsConn)
		proxyConn, err := grpc.DialContext(ctx, cfg.ImageProxyAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial image proxy: %w", err)
		}
		defer closeConn(proxyConn)

		proxy := imageproxyv1.NewImageProxyServiceClient(proxyConn)
		imageProxyClient = proxy
		assembledWorkloads.WithCatalog(
			imagesv1.NewImagesServiceClient(imagesConn),
			organizationsv1.NewOrganizationsServiceClient(organizationsConn),
			proxy,
		)
	}
	assembler := assembledWorkloads
	reconciler := reconciler.New(reconciler.Config{
		Threads:                   threadsClient,
		Agents:                    agentsClient,
		RunnerDialer:              runnerDialer,
		ZitiMgmt:                  zitiMgmtClient,
		Groups:                    groupsClient,
		Runners:                   runnersClient,
		Metering:                  meteringClient,
		Assembler:                 assembler,
		Wake:                      subscriber.Wake(),
		SandboxWake:               subscriber.SandboxWake(),
		Poll:                      cfg.PollInterval,
		WorkloadReconcileInterval: cfg.WorkloadReconcileInterval,
		Idle:                      cfg.IdleTimeout,
		StopSec:                   cfg.StopTimeoutSec,
		StopInactiveInstances:     cfg.StopInactiveInstances,
		MeteringSampleInterval:    cfg.MeteringSampleInterval,
		PlatformIdentityID:        cfg.PlatformIdentityID,
	})
	if imageProxyClient != nil {
		reconciler.WithImageProxy(imageProxyClient, cfg.ImageProxyHost)
	}

	start := func(leadCtx context.Context) {
		group, groupCtx := errgroup.WithContext(leadCtx)
		if cfg.GroupSyncEnabled && cfg.NATSURL != "" {
			reconciler.StartGroupMembershipConsumerLoop(groupCtx, cfg.NATSURL)
		}
		group.Go(func() error {
			return subscriber.Run(groupCtx)
		})
		group.Go(func() error {
			return reconciler.Run(groupCtx)
		})
		if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("orchestrator: leader workload stopped: %v", err)
		}
	}

	leader, err := leader.New(&cfg, start)
	if err != nil {
		return err
	}

	log.Printf("orchestrator: ready")
	if err := leader.Run(ctx); err != nil {
		return err
	}
	return nil
}
