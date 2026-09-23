package m3ua

import (
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
)

func restartRetentionFixture(t *testing.T, limit int) (*Endpoint, *Association, *distributionCapture) {
	t.Helper()
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP, SGP: &SGPConfig{MaxSSNMDestinationRecords: limit}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	listener := addOwnedListener(t, endpoint, 7, 1, 2)
	association, sent := addActiveASP(t, listener, 7, 1, 2)
	return endpoint, association, sent
}

func TestMTP3RestartRetentionRejectsUnreservedSubranges(t *testing.T) {
	for _, operation := range []string{"update", "availability", "congestion"} {
		t.Run(operation, func(t *testing.T) {
			endpoint, association, sent := restartRetentionFixture(t, 1)
			restart, err := endpoint.BeginMTP3Restart(ownerDestination(7, 1, 0x123400, 8))
			if err != nil {
				t.Fatal(err)
			}
			sent.reset()
			switch operation {
			case "update":
				err = restart.Update(ownerDestination(7, 1, 0x123456, 0), availabilityState(DestinationAvailable))
			case "availability":
				err = reportAvailability(endpoint, testWireScope(7, true, 1), 0x123456, 0, DestinationAvailable)
			case "congestion":
				err = reportCongestion(endpoint, testWireScope(7, true, 1), 0x123456, 0, 2, true)
			}
			if !errors.Is(err, ErrSSNMDestinationRecordLimit) {
				t.Fatalf("unreserved subrange error = %v, want record limit", err)
			}
			if err := restart.Complete(); err != nil {
				t.Fatal(err)
			}
			assertSSNMKinds(t, sent.snapshot(), nil)
			auditFrom(t, association, 7, 1, 0x123456)
			assertSSNMKinds(t, sent.snapshot(), []any{(*messages.DestinationUnavailable)(nil)})
			if got := len(endpoint.DestinationStatuses()); got != 1 {
				t.Fatalf("retained records = %d, want 1", got)
			}
		})
	}
}

func TestMTP3RestartRetentionBeginRefusalIsAtomic(t *testing.T) {
	endpoint, _, sent := restartRetentionFixture(t, 2)
	if err := reportAvailability(endpoint, testWireScope(7, true, 1), 0x111111, 0, DestinationAvailable); err != nil {
		t.Fatal(err)
	}
	sent.reset()
	restart, err := endpoint.BeginMTP3Restart(
		ownerDestination(7, 1, 0x111111, 0),
		ownerDestination(7, 1, 0x222222, 0),
		ownerDestination(7, 1, 0x333333, 0),
	)
	if restart != nil || !errors.Is(err, ErrSSNMDestinationRecordLimit) {
		t.Fatalf("begin = (%v, %v), want nil handle and record limit", restart, err)
	}
	assertSSNMKinds(t, sent.snapshot(), nil)
	assertOwnerState(t, endpoint, ownerStatusKey(7, 1, 0x111111, 0), availabilityState(DestinationAvailable))
	if got := len(endpoint.DestinationStatuses()); got != 1 {
		t.Fatalf("refused begin retained %d records, want unchanged 1", got)
	}
	if _, err := endpoint.BeginMTP3Restart(ownerDestination(7, 1, 0x222222, 0)); err != nil {
		t.Fatalf("refused begin left a stale epoch: %v", err)
	}
}

func TestMTP3RestartRetentionSubrangeKeepsDimensionsAndRetry(t *testing.T) {
	endpoint, association, sent := restartRetentionFixture(t, 2)
	restart, err := endpoint.BeginMTP3Restart(ownerDestination(7, 1, 0x123400, 8))
	if err != nil {
		t.Fatal(err)
	}
	if err := reportCongestion(endpoint, testWireScope(7, true, 1), 0x123456, 0, 2, true); err != nil {
		t.Fatal(err)
	}
	if err := restart.Update(ownerDestination(7, 1, 0x123456, 0), availabilityState(DestinationAvailable)); err != nil {
		t.Fatal(err)
	}
	assertOwnerState(t, endpoint, ownerStatusKey(7, 1, 0x123456, 0), availabilityState(DestinationUnavailable))
	if err := reportAvailability(endpoint, testWireScope(7, true, 2), 0x123456, 0, DestinationAvailable); !errors.Is(err, ErrSSNMDestinationRecordLimit) {
		t.Fatalf("reserved slot was stolen by another scope: %v", err)
	}
	failWrites(association, sent, errors.New("recovery refused"))
	if err := restart.Complete(); err == nil {
		t.Fatal("failed delivery reported success")
	} else if _, typed := err.(*SSNMDeliveryError); !typed {
		t.Fatalf("delivery failure type = %T, want *SSNMDeliveryError", err)
	}
	assertAffectedDestinations(t, "outstanding", restart.Outstanding(), []uint32{0x123456})
	assertOwnerState(t, endpoint, ownerStatusKey(7, 1, 0x123456, 0), availabilityState(DestinationUnavailable))
	association.signalWriter = sent.write
	sent.reset()
	if err := restart.Complete(); err != nil {
		t.Fatal(err)
	}
	assertSSNMKinds(t, sent.snapshot(), []any{(*messages.SignallingCongestion)(nil), (*messages.DestinationAvailable)(nil)})
	assertOwnerState(t, endpoint, ownerStatusKey(7, 1, 0x123456, 0), DestinationNetworkState{
		Availability: DestinationAvailable,
		Congestion:   CongestionState{Congested: true, Level: 2, LevelSet: true},
	})
	sent.reset()
	auditFrom(t, association, 7, 1, 0x123456)
	assertSSNMKinds(t, sent.snapshot(), []any{(*messages.SignallingCongestion)(nil), (*messages.DestinationAvailable)(nil)})
	sent.reset()
	auditFrom(t, association, 7, 1, 0x123457)
	assertSSNMKinds(t, sent.snapshot(), []any{(*messages.DestinationUnavailable)(nil)})
}

func TestMTP3RestartRetentionCompletionRechecksForgottenReservation(t *testing.T) {
	endpoint, association, sent := restartRetentionFixture(t, 2)
	restart, err := endpoint.BeginMTP3Restart(ownerDestination(7, 1, 0x123400, 8))
	if err != nil {
		t.Fatal(err)
	}
	if err := restart.Update(ownerDestination(7, 1, 0x123456, 0), availabilityState(DestinationAvailable)); err != nil {
		t.Fatal(err)
	}
	association.ForgetDestinations()
	for _, pointCode := range []uint32{0x222222, 0x333333} {
		if err := reportAvailability(endpoint, testWireScope(7, true, 1), pointCode, 0, DestinationAvailable); err != nil {
			t.Fatal(err)
		}
	}
	sent.reset()
	if err := restart.Complete(); !errors.Is(err, ErrSSNMDestinationRecordLimit) {
		t.Fatalf("completion without retained capacity = %v, want record limit", err)
	}
	assertSSNMKinds(t, sent.snapshot(), nil)
	assertAffectedDestinations(t, "outstanding", restart.Outstanding(), []uint32{0x123456})
	auditFrom(t, association, 7, 1, 0x123456)
	assertSSNMKinds(t, sent.snapshot(), []any{(*messages.DestinationUnavailable)(nil)})
	association.ForgetDestinations()
	sent.reset()
	if err := restart.Complete(); err != nil {
		t.Fatalf("retry after space reclamation: %v", err)
	}
	assertSSNMKinds(t, sent.snapshot(), []any{(*messages.DestinationAvailable)(nil)})
	assertOwnerState(t, endpoint, ownerStatusKey(7, 1, 0x123456, 0), availabilityState(DestinationAvailable))
}

func TestMTP3RestartRetentionForgetWaitsForPublication(t *testing.T) {
	endpoint, association, _ := restartRetentionFixture(t, 2)
	restart, err := endpoint.BeginMTP3Restart(ownerDestination(7, 1, 0x123400, 8))
	if err != nil {
		t.Fatal(err)
	}
	if err := restart.Update(ownerDestination(7, 1, 0x123456, 0), availabilityState(DestinationAvailable)); err != nil {
		t.Fatal(err)
	}
	publishing := make(chan struct{})
	release := make(chan struct{})
	restart.target.publish = func([]stagedDestination, bool, bool) *SSNMDeliveryError {
		close(publishing)
		<-release
		return nil
	}
	completed := make(chan error, 1)
	go func() { completed <- restart.Complete() }()
	<-publishing
	forgetStarted := make(chan struct{})
	forgotten := make(chan int, 1)
	go func() {
		close(forgetStarted)
		forgotten <- association.ForgetDestinations()
	}()
	<-forgetStarted
	var premature bool
	select {
	case <-forgotten:
		premature = true
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-completed; err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if premature {
		t.Fatal("ForgetDestinations cleared the reserved state during publication")
	}
	if count := <-forgotten; count != 2 {
		t.Fatalf("forgot %d records after completion, want 2", count)
	}
	if got := len(endpoint.DestinationStatuses()); got != 0 {
		t.Fatalf("completion resurrected %d explicitly forgotten records", got)
	}
}
