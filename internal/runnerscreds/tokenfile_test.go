package runnerscreds

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// writeProjectedToken mimics kubelet's atomic writer: the token path is a
// symlink into a timestamped directory that is swapped on rotation.
func writeProjectedToken(t *testing.T, dir, token string) string {
	t.Helper()
	data := filepath.Join(dir, "..data-"+time.Now().Format("150405.000000000"))
	if err := os.MkdirAll(data, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, "token"), []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "token")
	tmp := link + ".tmp"
	_ = os.Remove(tmp)
	if err := os.Symlink(filepath.Join(data, "token"), tmp); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, link); err != nil {
		t.Fatal(err)
	}
	return link
}

func TestTokenFileHeaderAndRotation(t *testing.T) {
	dir := t.TempDir()
	path := writeProjectedToken(t, dir, "first.token.v1\n")
	creds, err := NewTokenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if creds.RequireTransportSecurity() {
		t.Fatal("caller token must work over plaintext h2c")
	}
	md, err := creds.GetRequestMetadata(context.Background())
	if err != nil || md[MetadataKey] != "first.token.v1" || len(md) != 1 {
		t.Fatalf("metadata %v %v", md, err)
	}
	writeProjectedToken(t, dir, "second.token.v2")
	md, err = creds.GetRequestMetadata(context.Background())
	if err != nil || md[MetadataKey] != "second.token.v2" {
		t.Fatalf("rotated token not picked up: %v %v", md, err)
	}
}

func TestTokenFileRereadsAfterMaxAge(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("a.b.c"), 0o600); err != nil {
		t.Fatal(err)
	}
	creds, err := NewTokenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	creds.now = func() time.Time { return now }
	stat, _ := os.Stat(path)
	if err := os.WriteFile(path, []byte("d.e.f"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, stat.ModTime(), stat.ModTime()); err != nil {
		t.Fatal(err)
	}
	creds.loadedAt = now
	if md, _ := creds.GetRequestMetadata(context.Background()); md[MetadataKey] != "a.b.c" {
		t.Fatalf("re-read before max age: %v", md)
	}
	now = now.Add(maxAge)
	if md, _ := creds.GetRequestMetadata(context.Background()); md[MetadataKey] != "d.e.f" {
		t.Fatalf("no re-read after max age: %v", md)
	}
}

func TestDialOptionsRejectUnusableFiles(t *testing.T) {
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty")
	_ = os.WriteFile(empty, []byte(" \n"), 0o600)
	big := filepath.Join(dir, "big")
	_ = os.WriteFile(big, []byte(strings.Repeat("a", maxTokenBytes+1)), 0o600)
	for _, path := range []string{filepath.Join(dir, "missing"), empty, big} {
		if _, err := DialOptions(path); err == nil {
			t.Errorf("accepted %q", path)
		}
	}
	options, err := DialOptions("")
	if err != nil || len(options) != 1 {
		t.Fatalf("no token file: %d options, %v", len(options), err)
	}
}

type captureRunners struct {
	runnersv1.UnimplementedRunnersServiceServer
	mu    sync.Mutex
	calls []metadata.MD
}

func (s *captureRunners) capture(ctx context.Context) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, md)
}

func (s *captureRunners) ListWorkloads(ctx context.Context, _ *runnersv1.ListWorkloadsRequest) (*runnersv1.ListWorkloadsResponse, error) {
	s.capture(ctx)
	return &runnersv1.ListWorkloadsResponse{}, nil
}

func (s *captureRunners) StreamWorkloadLogs(_ *runnerv1.StreamWorkloadLogsRequest, stream grpc.ServerStreamingServer[runnerv1.StreamWorkloadLogsResponse]) error {
	s.capture(stream.Context())
	return nil
}

func serve(t *testing.T) (*captureRunners, func(context.Context, string) (net.Conn, error)) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	capture := &captureRunners{}
	runnersv1.RegisterRunnersServiceServer(server, capture)
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()
	return capture, func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }
}

func TestRunnersConnectionCarriesToken(t *testing.T) {
	capture, dialer := serve(t)
	options, err := DialOptions(writeProjectedToken(t, t.TempDir(), "runners.caller.token"))
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient("passthrough:///bufnet", append(options, grpc.WithContextDialer(dialer))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := runnersv1.NewRunnersServiceClient(conn)
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-identity-id", "identity-1")
	if _, err := client.ListWorkloads(ctx, &runnersv1.ListWorkloadsRequest{}); err != nil {
		t.Fatal(err)
	}
	stream, err := client.StreamWorkloadLogs(ctx, &runnerv1.StreamWorkloadLogsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("expected end of stream")
	}
	if len(capture.calls) != 2 {
		t.Fatalf("calls %d", len(capture.calls))
	}
	for _, md := range capture.calls {
		if got := md.Get(MetadataKey); len(got) != 1 || got[0] != "runners.caller.token" {
			t.Fatalf("caller token %v", got)
		}
		if got := md.Get("x-identity-id"); len(got) != 1 || got[0] != "identity-1" {
			t.Fatalf("identity %v", got)
		}
	}
}

// Connections built without the token option (every non-Runners client)
// never carry it.
func TestOtherConnectionsCarryNoToken(t *testing.T) {
	capture, dialer := serve(t)
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dialer))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := runnersv1.NewRunnersServiceClient(conn).ListWorkloads(context.Background(), &runnersv1.ListWorkloadsRequest{}); err != nil {
		t.Fatal(err)
	}
	if got := capture.calls[0].Get(MetadataKey); len(got) != 0 {
		t.Fatalf("plain connection sent %v", got)
	}
}

func TestMissingTokenFailsClosed(t *testing.T) {
	capture, dialer := serve(t)
	path := writeProjectedToken(t, t.TempDir(), "runners.caller.token")
	options, err := DialOptions(path)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient("passthrough:///bufnet", append(options, grpc.WithContextDialer(dialer))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	_, err = runnersv1.NewRunnersServiceClient(conn).ListWorkloads(context.Background(), &runnersv1.ListWorkloadsRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing token: %v, want Unauthenticated", err)
	}
	if len(capture.calls) != 0 {
		t.Fatal("server received a call without a caller token")
	}
}
