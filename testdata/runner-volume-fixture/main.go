// This model-free fixture must be built inside a reviewed k8s-runner checkout.
// It exposes that checkout's real ListVolumes and RemoveVolumeChecked over loopback.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/agynio/k8s-runner/internal/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	kubeconfig, namespace := os.Getenv("RETENTION_KUBECONFIG"), os.Getenv("RETENTION_NAMESPACE")
	uid, runID := os.Getenv("RETENTION_NAMESPACE_UID"), os.Getenv("RETENTION_RUN_ID")
	if os.Getenv("RETENTION_LIVE_TEST") != "trusted-local" || !filepath.IsAbs(kubeconfig) || uid == "" || runID == "" || !strings.HasPrefix(namespace, "orchestrator-volumes-") {
		return fmt.Errorf("explicit disposable-namespace fixture configuration required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return err
	}
	cfg.Timeout = 10 * time.Second
	admin, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	ns, err := admin.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err != nil || string(ns.UID) != uid || ns.Labels["agyn.io/volume-retention-test"] != runID {
		return fmt.Errorf("namespace ownership not confirmed")
	}
	cfg.Impersonate = rest.ImpersonationConfig{
		UserName: "system:serviceaccount:" + namespace + ":volume-inspector",
		Groups:   []string{"system:serviceaccounts", "system:serviceaccounts:" + namespace, "system:authenticated"},
	}
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	if _, err := kube.CoreV1().PersistentVolumeClaims("default").List(ctx, metav1.ListOptions{}); !apierrors.IsForbidden(err) {
		return fmt.Errorf("cross-namespace PVC access must be denied")
	}
	if _, err := kube.CoreV1().Secrets(namespace).List(ctx, metav1.ListOptions{}); !apierrors.IsForbidden(err) {
		return fmt.Errorf("secret access must be denied")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		switch info.FullMethod {
		case runnerv1.RunnerService_ListVolumes_FullMethodName:
		case runnerv1.RunnerService_RemoveVolumeChecked_FullMethodName:
			name := req.(*runnerv1.RemoveVolumeCheckedRequest).GetExpected().GetInstanceId()
			claim, err := kube.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				break
			}
			if err != nil || claim.Labels["agyn.io/volume-retention-test"] != runID || claim.Spec.VolumeName != "" || claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName != "unprovisioned-"+runID {
				return nil, status.Error(codes.FailedPrecondition, "only this fixture's empty claims may be removed")
			}
		default:
			return nil, status.Error(codes.PermissionDenied, "fixture exposes volume inspection/removal only")
		}
		return handler(ctx, req)
	}))
	runnerv1.RegisterRunnerServiceServer(grpcServer, server.New(server.Options{Clientset: kube, Namespace: namespace, Logger: zap.NewNop()}))
	if err := json.NewEncoder(os.Stdout).Encode(map[string]string{"address": listener.Addr().String(), "namespace": namespace, "uid": uid}); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- grpcServer.Serve(listener) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		grpcServer.Stop()
		<-done
		return nil
	}
}
