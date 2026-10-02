// Package runnerscreds attaches this process's projected ServiceAccount token
// to Runners calls. Runners authorizes control-plane callers with a
// Kubernetes TokenReview of that token and the deployment's grants.
//
// @see runners::internal/rpcauth/interceptor
package runnerscreds

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// MetadataKey carries the caller token. It is dedicated to Runners caller
// authorization and is single-valued.
const MetadataKey = "x-agyn-caller-token"

const (
	maxTokenBytes = 16 * 1024
	// maxAge re-reads the file even when its metadata looks unchanged.
	// Kubelet rotates projected tokens at 80% of their lifetime (at least 8 of
	// 10 minutes), so a cached copy is always still valid.
	maxAge = 30 * time.Second
)

// DialOptions returns the options for a Runners connection: plaintext h2c
// like every platform gRPC client, plus the caller token when tokenFile is
// set. A set but unreadable file is an error, so a misconfigured process
// fails at startup instead of calling Runners anonymously.
func DialOptions(tokenFile string) ([]grpc.DialOption, error) {
	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if strings.TrimSpace(tokenFile) == "" {
		return options, nil
	}
	creds, err := NewTokenFile(tokenFile)
	if err != nil {
		return nil, err
	}
	return append(options, grpc.WithPerRPCCredentials(creds)), nil
}

// TokenFile re-reads the kubelet-rotated token when the file changes
// (projected volumes swap a symlink, so the file identity changes) or after
// maxAge. An unreadable, empty or oversized file fails the RPC as
// Unauthenticated instead of sending it without a token.
type TokenFile struct {
	path string
	now  func() time.Time

	mu       sync.Mutex
	token    string
	info     os.FileInfo
	loadedAt time.Time
}

var _ credentials.PerRPCCredentials = (*TokenFile)(nil)

// NewTokenFile reads path once to prove it is usable.
func NewTokenFile(path string) (*TokenFile, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("runners caller token file path is required")
	}
	t := &TokenFile{path: path, now: time.Now}
	if _, err := t.current(); err != nil {
		return nil, err
	}
	return t, nil
}

// GetRequestMetadata implements credentials.PerRPCCredentials.
func (t *TokenFile) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	token, err := t.current()
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "runners caller token: %v", err)
	}
	return map[string]string{MetadataKey: token}, nil
}

// RequireTransportSecurity is false: platform gRPC is plaintext h2c. The
// token is audience-bound to Runners and bound to this pod.
func (t *TokenFile) RequireTransportSecurity() bool {
	return false
}

func (t *TokenFile) current() (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	info, err := os.Stat(t.path)
	if err != nil {
		return "", fmt.Errorf("stat %s: %w", t.path, err)
	}
	if t.token != "" && t.info != nil && os.SameFile(t.info, info) &&
		t.info.ModTime().Equal(info.ModTime()) && t.info.Size() == info.Size() &&
		t.now().Sub(t.loadedAt) < maxAge {
		return t.token, nil
	}
	token, err := read(t.path)
	if err != nil {
		return "", err
	}
	t.token, t.info, t.loadedAt = token, info, t.now()
	return token, nil
}

func read(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxTokenBytes+1))
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	if len(data) > maxTokenBytes {
		return "", fmt.Errorf("%s exceeds %d bytes", path, maxTokenBytes)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("%s is empty", path)
	}
	return token, nil
}
