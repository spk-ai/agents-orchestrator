package reconciler

import (
	"context"
	"testing"
)

// A replacement waits for its predecessor's removal to be confirmed, so the
// confirmation must wake the start decision instead of leaving it to the next
// poll tick.
func TestPreparedRemovalConfirmationWakesStartDecision(t *testing.T) {
	f := newPreparedControllerFixture(t, false)
	w, err := f.r.startPreparedWorkload(context.Background(), f.native, f.metadata, f.request, f.infos, f.created)
	if err != nil {
		t.Fatal(err)
	}
	f.r.removalConfirmed = make(chan struct{}, 1)
	if err := f.r.stopPreparedWorkload(context.Background(), f.native, w); err != nil {
		t.Fatal(err)
	}
	if w.RemovalConfirmedAt == nil {
		t.Fatal("removal was not confirmed")
	}
	select {
	case <-f.r.removalConfirmed:
	default:
		t.Fatal("confirmed removal did not wake the start decision")
	}
	// An already removed workload has nothing new to report.
	if err := f.r.stopPreparedWorkload(context.Background(), f.native, w); err != nil {
		t.Fatal(err)
	}
	select {
	case <-f.r.removalConfirmed:
		t.Fatal("repeated stop of a removed workload woke the start decision")
	default:
	}
}

func TestSignalRemovalConfirmedNeverBlocks(t *testing.T) {
	(&Reconciler{}).signalRemovalConfirmed()
	r := &Reconciler{removalConfirmed: make(chan struct{}, 1)}
	r.signalRemovalConfirmed()
	r.signalRemovalConfirmed()
	if len(r.removalConfirmed) != 1 {
		t.Fatalf("expected one pending wake, got %d", len(r.removalConfirmed))
	}
}
