package m3ua

import (
	"errors"
	"testing"
)

func TestAcceptedSGPDestinationReportsStopAtTheRecordBudget(t *testing.T) {
	listener, applicationServer, asp, sent := restartFixture(t, 1)
	restartActivateASP(applicationServer, asp, 1)
	listener.destinationRegistry().setRecordLimit(1)

	if err := reportAvailability(
		listenerEndpoint(t, listener), testWireScope(7, true, 1), 0x123456, 0, DestinationUnavailable,
	); err != nil {
		t.Fatalf("first destination report: %v", err)
	}
	sent.reset()

	err := reportAvailability(
		listenerEndpoint(t, listener), testWireScope(7, true, 1), 0x123457, 0, DestinationUnavailable,
	)
	if !errors.Is(err, ErrSSNMDestinationRecordLimit) {
		t.Errorf("destination report beyond the budget: error = %v, want ErrSSNMDestinationRecordLimit", err)
	}
	if got := len(ssnmMessages(sent.snapshot())); got != 0 {
		t.Errorf("refused destination report emitted %d SSNM messages, want 0", got)
	}
}

func TestSGPConfigBoundsRetainedDestinationRecords(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		SGP:  &SGPConfig{MaxSSNMDestinationRecords: 1},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()

	endpoint.destinations.mu.Lock()
	got := endpoint.destinations.recordLimitLocked()
	endpoint.destinations.mu.Unlock()
	if got != 1 {
		t.Errorf("SGP Endpoint destination record limit = %d, want 1 from SGPConfig", got)
	}
}

func TestListenerInheritsTheSGPDestinationRecordBudget(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		SGP:  &SGPConfig{MaxSSNMDestinationRecords: 3},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()

	listener := &Listener{endpoint: endpoint}
	store := listener.destinationRegistry()
	store.mu.Lock()
	got := store.recordLimitLocked()
	store.mu.Unlock()
	if got != 3 {
		t.Errorf("Listener destination record limit = %d, want 3 inherited from SGPConfig", got)
	}
}
