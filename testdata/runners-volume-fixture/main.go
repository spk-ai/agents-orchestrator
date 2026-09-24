// Build inside a reviewed Runners checkout. This serves its real registry and
// migrations on loopback; Agents metadata and authorization tuples are stubs.
package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"syscall"
	"time"

	agentsv1 "github.com/agynio/runners/.gen/go/agynio/api/agents/v1"
	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/runners/internal/db"
	"github.com/agynio/runners/internal/server"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type fixtureConfig struct {
	DSN               string `json:"dsn"`
	Schema            string `json:"schema"`
	RunID             string `json:"runId"`
	Token             string `json:"token"`
	RunnerID          string `json:"runnerId"`
	OrganizationID    string `json:"organizationId"`
	PreparedWorkloads bool   `json:"preparedWorkloads,omitempty"`
	VolumeMigration   bool   `json:"volumeMigration,omitempty"`
}

type tupleWriter struct {
	authorizationv1.AuthorizationServiceClient
}

func (tupleWriter) Write(context.Context, *authorizationv1.WriteRequest, ...grpc.CallOption) (*authorizationv1.WriteResponse, error) {
	return &authorizationv1.WriteResponse{}, nil
}

// Registry read APIs enrich persisted rows with names and attachments. These
// names do not stand in for the separate controller's owner/TTL checks.
type agentsMetadata struct{ organizationID string }

func (agentsMetadata) GetAgent(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
	return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: req.GetId()}, Name: "fixture-agent"}}, nil
}

func (a agentsMetadata) GetSandbox(_ context.Context, req *agentsv1.GetSandboxRequest, _ ...grpc.CallOption) (*agentsv1.GetSandboxResponse, error) {
	return &agentsv1.GetSandboxResponse{Sandbox: &agentsv1.Sandbox{
		Meta: &agentsv1.EntityMeta{Id: req.GetId()}, Name: "fixture-sandbox", OrganizationId: a.organizationID, OwnerId: "fixture-sandbox-user",
	}}, nil
}

func (agentsMetadata) GetVolume(_ context.Context, req *agentsv1.GetVolumeRequest, _ ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error) {
	return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{Meta: &agentsv1.EntityMeta{Id: req.GetId()}, Name: "fixture-volume", Persistent: true, Size: "1Mi"}}, nil
}

func (agentsMetadata) ListVolumes(context.Context, *agentsv1.ListVolumesRequest, ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "fixture metadata listing not implemented")
}

func (agentsMetadata) ListVolumeAttachments(context.Context, *agentsv1.ListVolumeAttachmentsRequest, ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "fixture attachment listing not implemented")
}

func (agentsMetadata) GetMcp(context.Context, *agentsv1.GetMcpRequest, ...grpc.CallOption) (*agentsv1.GetMcpResponse, error) {
	return nil, status.Error(codes.Unimplemented, "fixture MCP metadata not implemented")
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	path := os.Getenv("CHECKED_REGISTRY_CONFIG_FILE")
	if os.Getenv("CHECKED_VOLUME_STACK_TEST") != "trusted-local" || !filepath.IsAbs(path) {
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
	var cfg fixtureConfig
	if err := decoder.Decode(&cfg); err != nil {
		return fmt.Errorf("invalid fixture configuration")
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("only one fixture configuration value allowed")
	}
	for _, value := range []string{cfg.RunID, cfg.RunnerID, cfg.OrganizationID} {
		if parsed, err := uuid.Parse(value); err != nil || parsed.String() != value {
			return fmt.Errorf("canonical fixture identities required")
		}
	}
	runID := uuid.MustParse(cfg.RunID)
	if !regexp.MustCompile(`^checked_[a-f0-9]{32}$`).MatchString(cfg.Schema) ||
		cfg.Schema != "checked_"+hex.EncodeToString(runID[:]) {
		return fmt.Errorf("run-owned fixture schema required")
	}
	if token, err := hex.DecodeString(cfg.Token); err != nil || len(token) != 32 {
		return fmt.Errorf("fixture RPC token required")
	}
	poolConfig, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return fmt.Errorf("invalid database configuration")
	}
	if poolConfig.ConnConfig.Host != "127.0.0.1" || poolConfig.ConnConfig.Database != "runners_controller_acceptance" {
		return fmt.Errorf("disposable loopback controller-acceptance database required")
	}
	for _, fallback := range poolConfig.ConnConfig.Fallbacks {
		if fallback.Host != "127.0.0.1" {
			return fmt.Errorf("non-loopback database fallback refused")
		}
	}
	schema := pgx.Identifier{cfg.Schema}.Sanitize()
	poolConfig.MaxConns = 12
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	poolConfig.ConnConfig.RuntimeParams["application_name"] = cfg.Schema
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	setup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(setup, poolConfig)
	if err != nil {
		return fmt.Errorf("fixture database unavailable")
	}
	defer pool.Close()
	if err := initialize(setup, pool, cfg, schema); err != nil {
		return err
	}
	// Keep the volume-only fixture buildable against its older reviewed APIs.
	// Prepared mode requires the distinct anchored server handlers.
	preparedRPCs := map[string]bool{}
	migrationRPCs := map[string]bool{}
	for _, method := range runnersv1.RunnersService_ServiceDesc.Methods {
		if method.MethodName == "CreateAnchoredWorkload" || method.MethodName == "BindWorkloadResourceAnchors" || method.MethodName == "UpdateAnchoredWorkload" {
			preparedRPCs["/"+runnersv1.RunnersService_ServiceDesc.ServiceName+"/"+method.MethodName] = true
		}
		switch method.MethodName {
		case "BeginVolumeAnchorMigration", "GetVolumeAnchorMigration", "AdvanceVolumeAnchorMigration", "CreateVolume", "UpdateVolume":
			migrationRPCs["/"+runnersv1.RunnersService_ServiceDesc.ServiceName+"/"+method.MethodName] = true
		}
	}
	if cfg.VolumeMigration && len(migrationRPCs) != 5 {
		return fmt.Errorf("reviewed volume migration RPCs required")
	}
	if cfg.PreparedWorkloads && len(preparedRPCs) != 3 {
		return fmt.Errorf("reviewed prepared workload RPCs required")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("loopback listener unavailable")
	}
	defer listener.Close()
	rpc := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		values := md.Get("x-checked-volume-fixture-token")
		if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte(cfg.Token)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "fixture credential required")
		}
		if preparedRPCs[info.FullMethod] {
			if cfg.PreparedWorkloads {
				return handler(ctx, req)
			}
			return nil, status.Error(codes.PermissionDenied, "prepared fixture mode required")
		}
		if migrationRPCs[info.FullMethod] {
			if cfg.VolumeMigration {
				return handler(ctx, req)
			}
			return nil, status.Error(codes.PermissionDenied, "migration fixture mode required")
		}
		switch info.FullMethod {
		case runnersv1.RunnersService_CreateWorkload_FullMethodName:
			if cfg.PreparedWorkloads {
				return nil, status.Error(codes.PermissionDenied, "legacy start not allowed in prepared fixture")
			}
			return handler(ctx, req)
		case runnersv1.RunnersService_CreateVolumeChecked_FullMethodName,
			runnersv1.RunnersService_UpdateVolumeChecked_FullMethodName,
			runnersv1.RunnersService_GetVolume_FullMethodName,
			runnersv1.RunnersService_ListVolumes_FullMethodName,
			runnersv1.RunnersService_ListRunners_FullMethodName,
			runnersv1.RunnersService_UpdateWorkload_FullMethodName,
			runnersv1.RunnersService_GetWorkload_FullMethodName,
			runnersv1.RunnersService_ListWorkloads_FullMethodName,
			runnersv1.RunnersService_ListWorkloadsByAgentInstance_FullMethodName:
			return handler(ctx, req)
		default:
			return nil, status.Error(codes.PermissionDenied, "fixture RPC not allowed")
		}
	}))
	runnersv1.RegisterRunnersServiceServer(rpc, server.New(server.Options{
		Pool: pool, AuthorizationClient: tupleWriter{}, AgentsClient: agentsMetadata{organizationID: cfg.OrganizationID},
	}))
	if err := json.NewEncoder(os.Stdout).Encode(map[string]string{
		"address": listener.Addr().String(), "schema": cfg.Schema, "runId": cfg.RunID, "runnerId": cfg.RunnerID,
	}); err != nil {
		return err
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

func initialize(ctx context.Context, pool *pgxpool.Pool, cfg fixtureConfig, schema string) error {
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", cfg.Schema); err != nil {
			return err
		}
		var exists bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)", cfg.Schema).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			if _, err := tx.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, "CREATE TABLE "+schema+".fixture_ownership (run_id UUID PRIMARY KEY)"); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, "INSERT INTO "+schema+".fixture_ownership (run_id) VALUES ($1)", cfg.RunID)
			return err
		}
		var owned bool
		if err := tx.QueryRow(ctx, "SELECT count(*) = 1 AND bool_and(run_id = $1::uuid) FROM "+schema+".fixture_ownership", cfg.RunID).Scan(&owned); err != nil || !owned {
			return fmt.Errorf("existing schema does not belong to this fixture")
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("fixture schema initialization failed")
	}
	if err := db.ApplyMigrations(ctx, pool); err != nil {
		return fmt.Errorf("fixture registry migration failed")
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations WHERE version IN
		('0017_workload_removal_confirmation.sql', '0018_checked_volume_lifecycle.sql', '0019_volume_workload_admission.sql')`).Scan(&count); err != nil || count != 3 {
		return fmt.Errorf("reviewed workload, volume and admission migrations required")
	}
	if cfg.PreparedWorkloads {
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations WHERE version IN
			('0020_legacy_volume_adoption.sql', '0021_volume_backend_identity.sql', '0022_prepared_workloads.sql', '0023_resource_anchors.sql', '0024_resource_anchor_thread_identity.sql', '0025_anchored_volume_removal.sql')`).Scan(&count); err != nil || count != 6 {
			return fmt.Errorf("reviewed prepared workload migrations required")
		}
	}
	identity := uuid.NewSHA1(uuid.NameSpaceOID, []byte(cfg.RunID+"/registry-runner"))
	hash := sha256.Sum256([]byte(cfg.Token))
	if _, err := pool.Exec(ctx, `INSERT INTO runners (id, name, organization_id, identity_id, service_token_hash, status)
		VALUES ($1, 'checked-controller-fixture', $2, $3, $4, 'enrolled') ON CONFLICT (id) DO NOTHING`,
		cfg.RunnerID, cfg.OrganizationID, identity, hex.EncodeToString(hash[:])); err != nil {
		return fmt.Errorf("fixture runner seed failed")
	}
	var same bool
	if err := pool.QueryRow(ctx, `SELECT organization_id = $2::uuid AND identity_id = $3::uuid AND service_token_hash = $4 AND status = 'enrolled'
		FROM runners WHERE id = $1`, cfg.RunnerID, cfg.OrganizationID, identity, hex.EncodeToString(hash[:])).Scan(&same); err != nil || !same {
		return fmt.Errorf("fixture runner identity changed")
	}
	return nil
}
