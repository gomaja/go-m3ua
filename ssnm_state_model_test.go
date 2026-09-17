// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"runtime"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// DestinationRanges is documented as a lossless snapshot. RFC 4666 Section
// 3.4.4's Congestion Indications parameter is the only thing that distinguishes
// congestion abatement from a congestion report, so a snapshot that drops it
// cannot be used to reproduce what the peer said.
func TestInboundSCONRetainsCongestionLevelInRetainedRanges(t *testing.T) {
	for _, tt := range []struct {
		name      string
		level     *params.Param
		wantState DestinationState
		wantLevel uint8
		wantSet   bool
	}{
		{"explicit level", params.NewCongestionIndications(3), DestinationCongested, 3, true},
		{"explicit level zero", params.NewCongestionIndications(0), DestinationAvailable, 0, true},
		{"omitted level", nil, DestinationCongested, 0, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, _ := ssnmConn(t)
			if err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
				nil, nil, apc(0x1234), nil, tt.level, nil)); err != nil {
				t.Fatalf("handleSignallingCongestion() error = %v, want nil", err)
			}
			ranges := conn.DestinationRanges()
			if len(ranges) != 1 {
				t.Fatalf("retained ranges = %d, want 1", len(ranges))
			}
			retained := ranges[0]
			if retained.State != tt.wantState {
				t.Errorf("retained state = %v, want %v", retained.State, tt.wantState)
			}
			if retained.CongestionLevelSet != tt.wantSet {
				t.Errorf("retained CongestionLevelSet = %v, want %v",
					retained.CongestionLevelSet, tt.wantSet)
			}
			if retained.CongestionLevel != tt.wantLevel {
				t.Errorf("retained CongestionLevel = %d, want %d",
					retained.CongestionLevel, tt.wantLevel)
			}
		})
	}
}

// duna reports one destination unavailable over the SSNM receive path.
func duna(c *Association, pointCode uint32) error {
	return c.handleDestinationUnavailable(
		messages.NewDestinationUnavailable(nil, nil, apc(pointCode&0x00ffffff), nil))
}

// retainedDestinationRecords counts the records the state store holds.
func retainedDestinationRecords(c *Association) int {
	c.destinations.mu.RLock()
	defer c.destinations.mu.RUnlock()
	return len(c.destinations.state)
}

// The Affected Point Codes of an SSNM message are chosen by the peer and need
// not name anything this ASP has a route to, so MaxSSNMStateRecords and its
// siblings never see them: those count only records that intersect a
// provisioned MTP Route. Retained destination state therefore grew with
// whatever a peer SG chose to send, in well-formed legal DUNAs (CWE-770).
func TestSSNMDestinationRecordGrowthFlattensAtTheBound(t *testing.T) {
	conn, _ := ssnmConn(t)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	reported := 0
	var refusal error
	measure := func(total int) int {
		for ; reported < total; reported++ {
			if err := duna(conn, uint32(reported)); err != nil {
				refusal = err
			}
		}
		return retainedDestinationRecords(conn)
	}

	at1k := measure(1000)
	at10k := measure(10000)
	at100k := measure(100000)
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	t.Logf("retained records at 1k/10k/100k reported destinations = %d/%d/%d, heap delta = %.1f MB",
		at1k, at10k, at100k, float64(after.HeapAlloc-before.HeapAlloc)/(1<<20))

	if at1k != 1000 {
		t.Errorf("retained records below the bound = %d, want 1000", at1k)
	}
	if at10k != 10000 {
		t.Errorf("retained records below the bound = %d, want 10000", at10k)
	}
	if at100k != DefaultMaxSSNMDestinationRecords {
		t.Errorf("retained records after 100000 reported destinations = %d, want the %d-record bound",
			at100k, DefaultMaxSSNMDestinationRecords)
	}
	if !errors.Is(refusal, ErrSSNMDestinationRecordLimit) {
		t.Errorf("refusal = %v, want ErrSSNMDestinationRecordLimit", refusal)
	}
}

// boundedSSNMConn is an ASP Association bound to an ASP Endpoint whose
// ASPConfig sizes the retained-record budget.
func boundedSSNMConn(t *testing.T, records int) *Association {
	t.Helper()
	config := validASPConfig()
	config.MaxSSNMDestinationRecords = records
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	conn, _ := ssnmConn(t)
	conn.role = RoleASP
	conn.cfg.PeerSGP = &SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	if got := conn.destinationRecordLimit(); got != DefaultMaxSSNMDestinationRecords {
		t.Fatalf("unbound Association record limit = %d, want the package default %d",
			got, DefaultMaxSSNMDestinationRecords)
	}
	if !endpoint.trackAssociation(conn) {
		t.Fatal("trackAssociation")
	}
	if got := conn.destinationRecordLimit(); got != records {
		t.Fatalf("configured record limit = %d, want %d", got, records)
	}
	return conn
}

// The budget is configuration, not a constant: an ASP Endpoint sizes it for the
// network it serves. It refuses only destinations it has no room for, keeps
// every record it already holds, and is released by the reclaim path.
func TestSSNMDestinationRecordBudgetRefusesAndReclaims(t *testing.T) {
	conn := boundedSSNMConn(t, 4)

	for pointCode := uint32(1); pointCode <= 4; pointCode++ {
		if err := duna(conn, pointCode); err != nil {
			t.Fatalf("DUNA for %#x within the budget: %v", pointCode, err)
		}
	}
	if got := retainedDestinationRecords(conn); got != 4 {
		t.Fatalf("retained records = %d, want 4", got)
	}

	if err := duna(conn, 5); !errors.Is(err, ErrSSNMDestinationRecordLimit) {
		t.Errorf("DUNA beyond the budget: error = %v, want ErrSSNMDestinationRecordLimit", err)
	}
	if got := retainedDestinationRecords(conn); got != 4 {
		t.Errorf("retained records after a refusal = %d, want 4", got)
	}
	if got := conn.DestinationState(1); got != DestinationUnavailable {
		t.Errorf("state of a retained destination = %v, want %v: a refusal must not evict",
			got, DestinationUnavailable)
	}
	if got := conn.DestinationState(5); got != DestinationAvailable {
		t.Errorf("state of a refused destination = %v, want %v", got, DestinationAvailable)
	}

	// A destination already retained costs nothing more, so the budget never
	// blocks a state change the peer has already established.
	if err := conn.handleDestinationAvailable(
		messages.NewDestinationAvailable(nil, nil, apc(1), nil)); err != nil {
		t.Errorf("DAVA for a retained destination: %v", err)
	}
	if got := conn.DestinationState(1); got != DestinationAvailable {
		t.Errorf("state after DAVA = %v, want %v", got, DestinationAvailable)
	}

	if released := conn.ForgetDestinations(); released != 4 {
		t.Errorf("ForgetDestinations() = %d, want 4", released)
	}
	if got := retainedDestinationRecords(conn); got != 0 {
		t.Errorf("retained records after reclaim = %d, want 0", got)
	}
	if err := duna(conn, 5); err != nil {
		t.Errorf("DUNA after reclaim: %v", err)
	}
	if got := conn.DestinationState(5); got != DestinationUnavailable {
		t.Errorf("state after reclaim = %v, want %v", got, DestinationUnavailable)
	}
}

// The status a refused record describes still reaches the MTP3-User: the report
// is what the peer said, and it stands whether or not there was room to keep it.
func TestRefusedSSNMRecordIsStillReported(t *testing.T) {
	conn := boundedSSNMConn(t, 1)
	if err := duna(conn, 1); err != nil {
		t.Fatalf("DUNA within the budget: %v", err)
	}
	if status := nextStatus(t, conn); status.PointCode != 1 {
		t.Fatalf("status point code = %#x, want 1", status.PointCode)
	}
	if err := duna(conn, 2); !errors.Is(err, ErrSSNMDestinationRecordLimit) {
		t.Fatalf("DUNA beyond the budget: error = %v, want ErrSSNMDestinationRecordLimit", err)
	}
	status := nextStatus(t, conn)
	if status.PointCode != 2 || status.State != DestinationUnavailable {
		t.Errorf("status = %#x/%v, want the refused destination reported as unavailable",
			status.PointCode, status.State)
	}
}

// Zero is "not configured" throughout ASPConfig, so a store that was never
// given a limit uses the package default rather than refusing everything.
func TestDestinationStoreWithoutAConfiguredLimitUsesTheDefault(t *testing.T) {
	store := &destinations{}
	ranges := make([]DestinationRange, DefaultMaxSSNMDestinationRecords+1)
	for index := range ranges {
		ranges[index] = DestinationRange{PointCode: uint32(index)}
	}
	if err := store.setRangesWithinBudget(ranges); !errors.Is(err, ErrSSNMDestinationRecordLimit) {
		t.Fatalf("error = %v, want ErrSSNMDestinationRecordLimit", err)
	}
	if got := len(store.state); got != DefaultMaxSSNMDestinationRecords {
		t.Errorf("retained records = %d, want the default bound %d",
			got, DefaultMaxSSNMDestinationRecords)
	}

	// Zero means "not configured" on the way in as well, so it leaves a limit
	// that was configured alone rather than silently widening it.
	configured := newDestinations()
	configured.setRecordLimit(4)
	configured.setRecordLimit(0)
	if got := configured.recordLimitLocked(); got != 4 {
		t.Errorf("record limit after setRecordLimit(0) = %d, want the configured 4", got)
	}
}

// An SG that cannot retain a destination state must not announce it either: the
// audit it owes its ASPs afterwards would contradict the report it had sent.
func TestSGPDestinationReportsStopAtTheRecordBudget(t *testing.T) {
	endpoint, first, firstSent, _, _ := multiAssociationDialedSGPFixture(t)
	first.destinations.setRecordLimit(1)

	if err := first.ReportDestinationStateForNetworkAndRoutingContext(
		7, 1, 0x123456, DestinationUnavailable,
	); err != nil {
		t.Fatalf("first destination report: %v", err)
	}
	firstSent.reset()

	err := first.ReportDestinationStateForNetworkAndRoutingContext(
		7, 1, 0x123457, DestinationUnavailable,
	)
	if !errors.Is(err, ErrSSNMDestinationRecordLimit) {
		t.Errorf("destination report beyond the budget: error = %v, want ErrSSNMDestinationRecordLimit", err)
	}
	if got := len(ssnmMessages(firstSent.snapshot())); got != 0 {
		t.Errorf("refused destination report emitted %d SSNM messages, want 0", got)
	}

	firstSent.reset()
	congestion := endpoint.SignallingCongestion(SignallingCongestionRequest{
		Scope: SSNMScope{
			NetworkAppearance: 7, NetworkAppearanceSet: true,
			RoutingContexts: []uint32{1}, RoutingContextSet: true,
		},
		Destinations:       []PointCodeRange{{PointCode: 0x123458}},
		CongestionLevel:    2,
		CongestionLevelSet: true,
	})
	if !errors.Is(congestion, ErrSSNMDestinationRecordLimit) {
		t.Errorf("congestion report beyond the budget: error = %v, want ErrSSNMDestinationRecordLimit", congestion)
	}
	if got := len(ssnmMessages(firstSent.snapshot())); got != 0 {
		t.Errorf("refused congestion report emitted %d SSNM messages, want 0", got)
	}
}
