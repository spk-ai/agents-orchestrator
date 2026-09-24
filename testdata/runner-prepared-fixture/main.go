// Build inside the reviewed k8s-runner checkout. Only the namespace and RPC
// boundary are fixtures; preparation, inspection, activation and removal are real.
package main

import (
	"context"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/agynio/k8s-runner/internal/config"
	"github.com/agynio/k8s-runner/internal/server"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const ownerLabel = "agyn.io/prepared-controller-test"

type fixtureConfig struct {
	Kubeconfig, Namespace, NamespaceUID, RunID, Token string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	path := os.Getenv("PREPARED_RUNNER_CONFIG_FILE")
	if os.Getenv("PREPARED_STACK_TEST") != "trusted-local" || !filepath.IsAbs(path) {
		return fmt.Errorf("explicit trusted-local fixture configuration required")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return fmt.Errorf("private regular fixture configuration required")
	}
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("fixture configuration unavailable")
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	var settings fixtureConfig
	if decoder.Decode(&settings) != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		!filepath.IsAbs(settings.Kubeconfig) || !strings.HasPrefix(settings.Namespace, "orchestrator-prepared-") {
		return fmt.Errorf("invalid fixture configuration")
	}
	for _, value := range []string{settings.RunID, settings.NamespaceUID} {
		if id, err := uuid.Parse(value); err != nil || id == uuid.Nil || id.String() != value {
			return fmt.Errorf("canonical fixture identity required")
		}
	}
	if token, err := hex.DecodeString(settings.Token); err != nil || len(token) != 32 {
		return fmt.Errorf("fixture credential required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg, err := clientcmd.BuildConfigFromFlags("", settings.Kubeconfig)
	if err != nil {
		return fmt.Errorf("fixture Kubernetes configuration unavailable")
	}
	cfg.Timeout = 10 * time.Second
	admin, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("fixture Kubernetes client unavailable")
	}
	ns, err := admin.CoreV1().Namespaces().Get(ctx, settings.Namespace, metav1.GetOptions{})
	if err != nil || string(ns.UID) != settings.NamespaceUID || ns.Labels[ownerLabel] != settings.RunID {
		return fmt.Errorf("fixture namespace ownership unconfirmed")
	}
	cfg.Impersonate = rest.ImpersonationConfig{
		UserName: "system:serviceaccount:" + ns.Name + ":runner",
		Groups:   []string{"system:serviceaccounts", "system:serviceaccounts:" + ns.Name, "system:authenticated"},
	}
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("scoped fixture Kubernetes client unavailable")
	}
	if _, err := kube.CoreV1().Namespaces().List(ctx, metav1.ListOptions{}); !apierrors.IsForbidden(err) {
		return fmt.Errorf("namespace listing must be denied")
	}
	if _, err := kube.CoreV1().Namespaces().Get(ctx, "default", metav1.GetOptions{}); !apierrors.IsForbidden(err) {
		return fmt.Errorf("other namespace identity access must be denied")
	}
	if _, err := kube.CoreV1().PersistentVolumeClaims("default").List(ctx, metav1.ListOptions{}); !apierrors.IsForbidden(err) {
		return fmt.Errorf("cross-namespace PVC access must be denied")
	}
	if _, err := kube.CoreV1().Secrets(ns.Name).List(ctx, metav1.ListOptions{}); !apierrors.IsForbidden(err) {
		return fmt.Errorf("Secret listing must be denied")
	}
	observed, err := kube.CoreV1().Namespaces().Get(ctx, ns.Name, metav1.GetOptions{})
	if err != nil || observed.UID != ns.UID {
		return fmt.Errorf("scoped backend identity read required")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("fixture loopback listener unavailable")
	}
	defer listener.Close()
	rpc := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		values := md.Get("x-prepared-runner-fixture-token")
		if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte(settings.Token)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "fixture credential required")
		}
		switch info.FullMethod {
		case runnerv1.RunnerService_PrepareAnchoredWorkload_FullMethodName:
			if req.(*runnerv1.PrepareAnchoredWorkloadRequest).GetPreparation().GetWorkload().GetLabels()[ownerLabel] != settings.RunID {
				return nil, status.Error(codes.PermissionDenied, "fixture workload ownership required")
			}
		case runnerv1.RunnerService_ListVolumes_FullMethodName,
			runnerv1.RunnerService_ReserveVolumeAnchorAdoption_FullMethodName,
			runnerv1.RunnerService_ApplyVolumeAnchorAdoption_FullMethodName,
			runnerv1.RunnerService_FinalizeVolumeAnchorAdoption_FullMethodName,
			runnerv1.RunnerService_ObserveVolumeAnchorAdoption_FullMethodName,
			runnerv1.RunnerService_ReserveResourceAnchor_FullMethodName,
			runnerv1.RunnerService_RemoveWorkloadAnchor_FullMethodName,
			runnerv1.RunnerService_RemoveVolumeAnchored_FullMethodName,
			runnerv1.RunnerService_ObserveWorkloadPreparation_FullMethodName,
			runnerv1.RunnerService_RevokeWorkloadPreparation_FullMethodName,
			runnerv1.RunnerService_ObservePreparationRevocation_FullMethodName,
			runnerv1.RunnerService_InspectPreparedWorkload_FullMethodName,
			runnerv1.RunnerService_ActivateWorkload_FullMethodName,
			runnerv1.RunnerService_RemovePreparedWorkload_FullMethodName:
		default:
			return nil, status.Error(codes.PermissionDenied, "fixture RPC not allowed")
		}
		return handler(ctx, req)
	}), grpc.StreamInterceptor(func(any, grpc.ServerStream, *grpc.StreamServerInfo, grpc.StreamHandler) error {
		return status.Error(codes.PermissionDenied, "fixture streams not allowed")
	}))
	runnerv1.RegisterRunnerServiceServer(rpc, server.New(server.Options{
		Clientset: kube, Namespace: ns.Name, StorageSize: "1Mi", Logger: zap.NewNop(),
		SupportingContainerResources: &config.ComputeResources{RequestsCPU: "50m", RequestsMemory: "64Mi", LimitsCPU: "250m", LimitsMemory: "128Mi"},
	}))
	if err := json.NewEncoder(os.Stdout).Encode(map[string]string{"address": listener.Addr().String(), "namespace": ns.Name, "uid": string(ns.UID), "runId": settings.RunID}); err != nil {
		return fmt.Errorf("fixture readiness write failed")
	}
	done := make(chan error, 1)
	go func() { done <- rpc.Serve(listener) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		rpc.Stop()
		<-done
		return nil
	}
}
