// volume-migration is an operator-only, metadata-adoption command. It does not
// run agents. Cluster drain and restore-tested backup verification belong to the
// deployment coordinator and must precede this command.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/agents-orchestrator/internal/runnerdial"
	"github.com/agynio/agents-orchestrator/internal/volumemigration"
	"github.com/agynio/agents-orchestrator/internal/zitimanager"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		if errors.Is(err, volumemigration.ErrQuarantined) {
			os.Exit(3)
		}
		os.Exit(1)
	}
}

func run() error {
	planFile := flag.String("plan", "", "immutable BeginVolumeAnchorMigration JSON file, or - for stdin")
	batch := flag.Bool("batch", false, "plan contains a JSON array of immutable same-runner owner plans")
	inspectRunner := flag.String("inspect-runner", "", "read native volume inventory for this runner without migrating")
	continueQuarantined := flag.Bool("continue-quarantined", false, "retain failed owners blocked while processing the remaining batch")
	registryAddress := flag.String("runners-address", "", "private platform registry gRPC address")
	managementAddress := flag.String("ziti-management-address", "", "private platform Ziti management gRPC address")
	ack := flag.Bool("trusted-local-drained", false, "acknowledge trusted local writers are drained and backups restore-verified")
	timeout := flag.Duration("timeout", 5*time.Minute, "maximum duration; interruption retains the migration block")
	flag.Parse()
	if !*ack || (*planFile == "") == (*inspectRunner == "") || *registryAddress == "" || *managementAddress == "" || *timeout <= 0 || *timeout > 30*time.Minute || flag.NArg() != 0 {
		return errors.New("explicit trusted-local drain/backup acknowledgement, immutable plan and private service addresses required")
	}
	var plans []*runnersv1.BeginVolumeAnchorMigrationRequest
	runnerID := *inspectRunner
	if *planFile != "" {
		var input io.Reader = os.Stdin
		if *planFile != "-" {
			file, err := os.Open(*planFile)
			if err != nil {
				return fmt.Errorf("open migration plan: %w", err)
			}
			defer file.Close()
			input = file
		}
		data, err := io.ReadAll(io.LimitReader(input, 8*1024*1024+1))
		if err != nil {
			return errors.New("bounded migration plan required")
		}
		plans, err = parsePlans(data, *batch)
		if err != nil {
			return err
		}
		runnerID = plans[0].RunnerId
	} else if id, err := uuid.Parse(runnerID); err != nil || id == uuid.Nil || id.String() != runnerID {
		return errors.New("canonical inventory runner required")
	}
	parent, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(parent, *timeout)
	defer cancel()
	registry, err := grpc.NewClient(*registryAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer registry.Close()
	management, err := grpc.NewClient(*managementAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer management.Close()
	manager, err := zitimanager.New(ctx, zitimgmtv1.NewZitiManagementServiceClient(management), 2*time.Minute, time.Minute)
	if err != nil {
		return err
	}
	go manager.RunLeaseRenewal(ctx)
	go func() {
		select {
		case <-ctx.Done():
		case <-manager.IdentityLost():
			cancel()
		}
	}()
	dialer := runnerdial.NewDialer(manager)
	defer dialer.Close()
	native, err := dialer.Dial(ctx, runnerID)
	if err != nil {
		return err
	}
	output := json.NewEncoder(os.Stdout)
	if *inspectRunner != "" {
		inventory, err := native.ListVolumes(ctx, &runnerv1.ListVolumesRequest{})
		if err != nil {
			return err
		}
		body, err := protojson.Marshal(inventory)
		if err != nil {
			return err
		}
		return output.Encode(json.RawMessage(body))
	}
	c := volumemigration.Coordinator{Registry: runnersv1.NewRunnersServiceClient(registry), Native: native,
		Checkpoint: func(stage string, m *runnersv1.VolumeAnchorMigration) error {
			body, err := protojson.Marshal(m)
			if err != nil {
				return err
			}
			return output.Encode(struct {
				Stage     string          `json:"stage"`
				Migration json.RawMessage `json:"migration"`
			}{stage, body})
		}}
	quarantined := false
	for _, plan := range plans {
		_, err = c.Run(ctx, plan)
		if errors.Is(err, volumemigration.ErrQuarantined) && *continueQuarantined {
			quarantined = true
			continue
		}
		if err != nil {
			return err
		}
	}
	if quarantined {
		return volumemigration.ErrQuarantined
	}
	return nil
}

func parsePlans(data []byte, batch bool) ([]*runnersv1.BeginVolumeAnchorMigrationRequest, error) {
	if len(data) > 8*1024*1024 {
		return nil, errors.New("bounded migration batch required")
	}
	raw := []json.RawMessage{data}
	if batch && json.Unmarshal(data, &raw) != nil {
		return nil, errors.New("migration batch must be a JSON array")
	}
	if len(raw) == 0 || len(raw) > 1024 {
		return nil, errors.New("bounded nonempty migration batch required")
	}
	var plans []*runnersv1.BeginVolumeAnchorMigrationRequest
	seen := map[string]bool{}
	for _, body := range raw {
		plan := &runnersv1.BeginVolumeAnchorMigrationRequest{}
		if len(body) > 256*1024 || protojson.Unmarshal(body, plan) != nil {
			return nil, errors.New("invalid immutable migration plan JSON")
		}
		for _, value := range []string{plan.Id, plan.OwnerId, plan.RunnerId, plan.OrganizationId} {
			id, err := uuid.Parse(value)
			if err != nil || id == uuid.Nil || id.String() != value {
				return nil, errors.New("canonical migration identities required")
			}
		}
		key := plan.OwnerKind.String() + "/" + plan.OwnerId
		if seen[key] || len(plans) > 0 && plan.RunnerId != plans[0].RunnerId || len(plan.Sources) == 0 || len(plan.Sources) > 64 {
			return nil, errors.New("unique same-runner owner plans with complete sources required")
		}
		seen[key] = true
		plans = append(plans, plan)
	}
	return plans, nil
}
