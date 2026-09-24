package main

import (
	"encoding/json"
	"testing"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestImmutableMigrationInput(t *testing.T) {
	newPlan := func() *runnersv1.BeginVolumeAnchorMigrationRequest {
		return &runnersv1.BeginVolumeAnchorMigrationRequest{Id: uuid.NewString(), OwnerKind: runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
			OwnerId: uuid.NewString(), RunnerId: uuid.NewString(), OrganizationId: uuid.NewString(), Sources: []*runnersv1.VolumeAnchorMigrationSource{{VolumeId: uuid.NewString(), ExpectedRevision: 1}}}
	}
	first := newPlan()
	body, err := protojson.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if plans, err := parsePlans(body, false); err != nil || len(plans) != 1 {
		t.Fatalf("single plan: %v", err)
	}
	for _, mode := range []string{"valid", "duplicate", "mixed-runner", "missing-source", "unknown-field", "empty"} {
		t.Run(mode, func(t *testing.T) {
			second := newPlan()
			second.RunnerId = first.RunnerId
			if mode == "duplicate" {
				second = first
			}
			if mode == "mixed-runner" {
				second.RunnerId = uuid.NewString()
			}
			if mode == "missing-source" {
				second.Sources = nil
			}
			next, err := protojson.Marshal(second)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "unknown-field" {
				next = []byte(`{"unknown":true}`)
			}
			raw := []json.RawMessage{body, next}
			if mode == "empty" {
				raw = nil
			}
			input, _ := json.Marshal(raw)
			plans, err := parsePlans(input, true)
			if mode == "valid" {
				if err != nil || len(plans) != 2 {
					t.Fatalf("valid batch: %v", err)
				}
			} else if err == nil {
				t.Fatal("invalid batch accepted")
			}
		})
	}
}
