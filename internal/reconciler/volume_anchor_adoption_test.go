package reconciler

import (
	"context"
	"fmt"
	"strings"
	"testing"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

func seedAdoptedWorkspace(f *preparedControllerFixture) {
	f.created = nil
	f.v.BoundInstance = checkedTestInstance(f.v, f.infos[0].Spec.PersistentName, uuid.NewString())
	f.v.InstanceId, f.v.Status = stringPtr(f.v.BoundInstance.InstanceId), runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE
	f.seedAnchoredWorkspace()
	previous := proto.Clone(f.v.BoundInstance).(*runnerv1.VolumeListItem)
	previous.Anchor = nil
	f.v.AnchorReservation = nil
	f.v.AnchorAdoption = &runnerv1.VolumeAnchorAdoption{Id: uuid.NewString(), Previous: previous,
		Anchor: proto.Clone(f.v.ResourceAnchor).(*runnerv1.ResourceAnchor), InstanceUid: uuid.NewString(), PvcSpecSha256: strings.Repeat("a", 64)}
}

func TestAdoptedVolumeUsesExistingControllerWorkflow(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		t.Run(fmt.Sprintf("sandbox=%t", sandbox), func(t *testing.T) {
			f := newPreparedControllerFixture(t, sandbox)
			seedAdoptedWorkspace(f)
			original := proto.Clone(f.v).(*runnersv1.Volume)
			for turn := 0; turn < 2; turn++ {
				w, err := f.r.startPreparedWorkload(context.Background(), f.native, f.metadata, f.request, f.infos, f.created)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.r.stopPreparedWorkload(context.Background(), f.native, w); err != nil {
					t.Fatal(err)
				}
				if !proto.Equal(f.v, original) {
					t.Fatal("followup replaced original adopted volume or migration provenance")
				}
			}
		})
	}
}

func TestAdoptedVolumeRejectsChangedProvenance(t *testing.T) {
	mutations := map[string]func(*runnersv1.Volume){
		"missing-receipt": func(v *runnersv1.Volume) { v.AnchorAdoption = nil },
		"both-receipts": func(v *runnersv1.Volume) {
			v.AnchorReservation = &runnersv1.VolumeAnchorReservation{WorkloadId: uuid.NewString(), PreparationRevision: 1, ResourceRevision: 1}
		},
		"missing-anchor":        func(v *runnersv1.Volume) { v.ResourceAnchor = nil },
		"replacement-pvc":       func(v *runnersv1.Volume) { v.AnchorAdoption.Previous.InstanceUid = uuid.NewString() },
		"wrong-owner":           func(v *runnersv1.Volume) { v.AnchorAdoption.Anchor.InstanceUid = uuid.NewString() },
		"wrong-backend":         func(v *runnersv1.Volume) { v.AnchorAdoption.Previous.BackendId = "other" },
		"wrong-spec-hash":       func(v *runnersv1.Volume) { v.AnchorAdoption.PvcSpecSha256 = strings.Repeat("A", 64) },
		"missing-journal":       func(v *runnersv1.Volume) { v.AnchorAdoption.InstanceUid = "" },
		"anchored-source":       func(v *runnersv1.Volume) { v.AnchorAdoption.Previous.Anchor = v.ResourceAnchor },
		"unbound":               func(v *runnersv1.Volume) { v.BoundInstance = nil },
		"provisioning":          func(v *runnersv1.Volume) { v.Status = runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING },
		"failed":                func(v *runnersv1.Volume) { v.Status = runnersv1.VolumeStatus_VOLUME_STATUS_FAILED },
		"unknown-receipt-field": func(v *runnersv1.Volume) { v.AnchorAdoption.ProtoReflect().SetUnknown([]byte{0x30, 1}) },
	}
	for _, sandbox := range []bool{false, true} {
		for name, mutate := range mutations {
			t.Run(fmt.Sprintf("sandbox=%t/%s", sandbox, name), func(t *testing.T) {
				f := newPreparedControllerFixture(t, sandbox)
				seedAdoptedWorkspace(f)
				mutate(f.v)
				if err := validateCheckedVolume(f.v); err == nil {
					t.Fatal("accepted changed or incomplete adoption provenance")
				}
				if _, err := f.r.startPreparedWorkload(context.Background(), f.native, f.metadata, f.request, f.infos, f.created); err == nil || f.activations != 0 {
					t.Fatal("invalid adoption admitted execution")
				}
			})
		}
	}
}
