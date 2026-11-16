package main

import (
	"testing"

	"github.com/gomaja/go-m3ua"
)

func TestSSNMCongestionSourcesAccountedBytes(t *testing.T) {
	scope := m3ua.WireScope{RoutingContextSet: true, RoutingContexts: []uint32{1, 2}}
	event := m3ua.SSNMEvent{Kind: m3ua.SSNMCongestionExpiredEvent, DestinationUpdates: []m3ua.SSNMDestinationKnowledge{{CongestionSources: []m3ua.SSNMCongestion{{Scope: scope}, {Scope: scope}}}}}
	if got, want := ssnmEventBytes(event), 512+320+2*(256+8); got != want {
		t.Fatalf("source history accounted bytes = %d, want %d", got, want)
	}
}

func TestSSNMSourceInvalidationsAccountedIndependently(t *testing.T) {
	event := m3ua.SSNMEvent{Kind: m3ua.SSNMDestinationsInvalidatedEvent, Invalidated: make([]m3ua.SSNMDestinationInvalidation, 2048), CongestionSourcesInvalidated: make([]m3ua.SSNMCongestionSourceInvalidation, 2048)}
	if got, want := ssnmEventBytes(event), 512+2048*(16+48); got != want {
		t.Fatalf("source invalidation accounted bytes = %d, want %d", got, want)
	}
}

func TestSSNMExpiryIdentitiesAccountedIndependently(t *testing.T) {
	event := m3ua.SSNMEvent{Kind: m3ua.SSNMCongestionExpiredEvent, Expired: make([]m3ua.SSNMCongestionExpiry, 256)}
	if got, want := ssnmEventBytes(event), 512+256*56; got != want {
		t.Fatalf("expiry identity accounted bytes = %d, want %d", got, want)
	}
}

func TestSSNMAvailabilitySourcesAccountedIndependently(t *testing.T) {
	scope := m3ua.WireScope{RoutingContextSet: true, RoutingContexts: []uint32{1, 2}}
	event := m3ua.SSNMEvent{DestinationUpdates: []m3ua.SSNMDestinationKnowledge{{AvailabilitySources: []m3ua.SSNMAvailability{{Scope: scope}, {Scope: scope}}}}, AvailabilitySourcesInvalidated: make([]m3ua.SSNMAvailabilitySourceInvalidation, 3)}
	if got, want := ssnmEventBytes(event), 512+320+2*(256+8)+3*48; got != want {
		t.Fatal("availability source accounting disagrees", got, want)
	}
}
