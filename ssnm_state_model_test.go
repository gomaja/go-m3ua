// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// Availability and congestion are two different statuses of the same
// destination. RFC 4666 Section 4.5.2.2 keeps them apart, and the Congestion
// Level table in Section 3.4.4 makes level 0 "No Congestion or Undefined" — a
// statement about congestion, never about reachability. Section 4.5.1 makes
// DUNA followed by SCON an ordinary sequence, and Sections 4.4.2 and 4.5.3 have
// the SG answer a later DAUD with DUNA until a DAVA arrives. Folding a SCON
// into the availability field told an ASP to resume traffic into a destination
// the SG had just reported unreachable.
func TestSCONNeverChangesDestinationAvailability(t *testing.T) {
	for _, tt := range []struct {
		name      string
		level     *params.Param
		wantState DestinationState
	}{
		{"explicit level zero", params.NewCongestionIndications(0), DestinationUnavailable},
		{"explicit level", params.NewCongestionIndications(2), DestinationUnavailable},
		{"omitted level", nil, DestinationUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, _ := ssnmConn(t)
			if err := conn.handleDestinationUnavailable(
				messages.NewDestinationUnavailable(nil, nil, apc(0x1234), nil)); err != nil {
				t.Fatalf("handleDestinationUnavailable() error = %v, want nil", err)
			}
			if got := conn.DestinationState(0x1234); got != DestinationUnavailable {
				t.Fatalf("state after DUNA = %v, want %v", got, DestinationUnavailable)
			}

			if err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
				nil, nil, apc(0x1234), nil, tt.level, nil)); err != nil {
				t.Fatalf("handleSignallingCongestion() error = %v, want nil", err)
			}
			if got := conn.DestinationState(0x1234); got != tt.wantState {
				t.Errorf("state after SCON = %v, want %v: only a DAVA restores reachability",
					got, tt.wantState)
			}
		})
	}
}

// A destination that is congested and then reported available keeps the
// availability the DAVA installed, and a SCON that follows a DAVA is still
// visible as congestion.
func TestSCONCongestsOnlyAnAvailableDestination(t *testing.T) {
	conn, _ := ssnmConn(t)
	if err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
		nil, nil, apc(0x1234), nil, params.NewCongestionIndications(2), nil)); err != nil {
		t.Fatalf("handleSignallingCongestion() error = %v, want nil", err)
	}
	if got := conn.DestinationState(0x1234); got != DestinationCongested {
		t.Fatalf("state after SCON = %v, want %v", got, DestinationCongested)
	}
	if err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
		nil, nil, apc(0x1234), nil, params.NewCongestionIndications(0), nil)); err != nil {
		t.Fatalf("handleSignallingCongestion() abatement error = %v, want nil", err)
	}
	if got := conn.DestinationState(0x1234); got != DestinationAvailable {
		t.Errorf("state after abatement = %v, want %v", got, DestinationAvailable)
	}
}

// The wire consequence at an SGP: a congestion report, including the explicit
// level zero that abates congestion, must not make a DAUD for an unavailable
// destination answer DAVA. RFC 4666 Section 4.4.2 answers the audit from the
// availability the SG holds.
func TestSGPCongestionReportKeepsDAUDAnsweredWithDUNA(t *testing.T) {
	endpoint, first, firstSent, _, _ := multiAssociationDialedSGPFixture(t)
	const pointCode = 0x123456
	if err := first.ReportDestinationStateForNetworkAndRoutingContext(
		7, 1, pointCode, DestinationUnavailable,
	); err != nil {
		t.Fatalf("report destination unavailable: %v", err)
	}
	if err := endpoint.SignallingCongestion(SignallingCongestionRequest{
		Scope: WireScope{
			NetworkAppearance: 7, NetworkAppearanceSet: true,
			RoutingContexts: []uint32{1}, RoutingContextSet: true,
		},
		Destinations:       []PointCodeRange{{PointCode: pointCode}},
		CongestionLevel:    0,
		CongestionLevelSet: true,
	}); err != nil {
		t.Fatalf("record congestion abatement: %v", err)
	}

	status, ok := endpoint.DestinationStatus(DestinationStatusKey{
		NetworkAppearance: 7, NetworkAppearanceSet: true,
		RoutingContext: 1, RoutingContextSet: true,
		PointCode: pointCode,
	})
	if !ok || status.State != DestinationUnavailable {
		t.Errorf("retained status = %+v, %v, want state %v",
			status, ok, DestinationUnavailable)
	}

	firstSent.reset()
	if err := first.handleDestinationStateAudit(messages.NewDestinationStateAudit(
		params.NewNetworkAppearance(7),
		params.NewRoutingContext(1),
		apc(pointCode),
		nil,
	)); err != nil {
		t.Fatalf("handle DAUD: %v", err)
	}
	got := typeNames(ssnmMessages(firstSent.snapshot()))
	want := []string{"Destination Unavailable"}
	if len(got) != len(want) || got[0] != want[0] {
		t.Errorf("DAUD answered with %v, want %v: the destination is still unreachable", got, want)
	}
}

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

// The Congestion Indications parameter is optional (RFC 4666 Section 3.4.4),
// and level 0 is "No Congestion or Undefined". Without a presence bit, a status
// carrying an explicit level 0, a status carrying no level at all, and a DAVA
// were three different reports that arrived on SignallingStatus looking the
// same, so an MTP3-User could not tell congestion abatement from a destination
// coming back.
func TestSignallingStatusDistinguishesCongestionLevelPresence(t *testing.T) {
	for _, tt := range []struct {
		name      string
		send      func(*Association) error
		wantState DestinationState
		wantLevel uint8
		wantSet   bool
	}{
		{
			name: "DAVA",
			send: func(c *Association) error {
				return c.handleDestinationAvailable(
					messages.NewDestinationAvailable(nil, nil, apc(0x1234), nil))
			},
			wantState: DestinationAvailable,
		},
		{
			name: "SCON with explicit level zero",
			send: func(c *Association) error {
				return c.handleSignallingCongestion(messages.NewSignallingCongestion(
					nil, nil, apc(0x1234), nil, params.NewCongestionIndications(0), nil))
			},
			wantState: DestinationAvailable,
			wantSet:   true,
		},
		{
			name: "SCON without a level",
			send: func(c *Association) error {
				return c.handleSignallingCongestion(messages.NewSignallingCongestion(
					nil, nil, apc(0x1234), nil, nil, nil))
			},
			wantState: DestinationCongested,
		},
		{
			name: "SCON with an explicit level",
			send: func(c *Association) error {
				return c.handleSignallingCongestion(messages.NewSignallingCongestion(
					nil, nil, apc(0x1234), nil, params.NewCongestionIndications(3), nil))
			},
			wantState: DestinationCongested,
			wantLevel: 3,
			wantSet:   true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, _ := ssnmConn(t)
			if err := tt.send(conn); err != nil {
				t.Fatalf("send: %v", err)
			}
			status := nextStatus(t, conn)
			if status.State != tt.wantState {
				t.Errorf("status.State = %v, want %v", status.State, tt.wantState)
			}
			if status.CongestionLevelSet != tt.wantSet {
				t.Errorf("status.CongestionLevelSet = %v, want %v",
					status.CongestionLevelSet, tt.wantSet)
			}
			if status.CongestionLevel != tt.wantLevel {
				t.Errorf("status.CongestionLevel = %d, want %d",
					status.CongestionLevel, tt.wantLevel)
			}
		})
	}
}

// An SGP passes an ASP's own SCON on without recording it, and the same
// presence distinction applies to that report.
func TestPeerReportedCongestionCarriesLevelPresence(t *testing.T) {
	conn, _ := newSSNMTestConn(t, StateASPActive, RoleSGP)
	if err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
		nil, nil, apc(0x222222), nil, params.NewCongestionIndications(0), nil)); err != nil {
		t.Fatalf("SCON from an ASP was rejected at an SGP: %v", err)
	}
	status := nextStatus(t, conn)
	if !status.PeerReported {
		t.Fatalf("status.PeerReported = false, want true")
	}
	if !status.CongestionLevelSet || status.CongestionLevel != 0 {
		t.Errorf("status congestion = %d/%v, want an explicit zero",
			status.CongestionLevel, status.CongestionLevelSet)
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
	// The Endpoint provisions sg-a/sgp-a1 for one Application Server, so the
	// Association has to name that Application Server's wire scope.
	setInventoryNetworkAppearance(&conn.cfg.ApplicationServers, params.NewNetworkAppearance(7))
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

// One SCON applies to every Routing Context it names, and those contexts need
// not share an availability: RFC 4666 Section 4.5 scopes destination state per
// Application Server traffic flow. The congestion report has to preserve each
// one separately.
func TestSCONPreservesPerRoutingContextAvailability(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 0, 1, 2)
	const pointCode = uint32(0x101001)

	if err := conn.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		nil, params.NewRoutingContext(1), apc(pointCode), nil)); err != nil {
		t.Fatalf("scoped DUNA: %v", err)
	}
	if err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
		nil, params.NewRoutingContext(0, 1, 2), apc(pointCode), nil,
		params.NewCongestionIndications(2), nil)); err != nil {
		t.Fatalf("multi-context SCON: %v", err)
	}

	for _, test := range []struct {
		routingContext uint32
		want           DestinationState
	}{
		{routingContext: 0, want: DestinationCongested},
		{routingContext: 1, want: DestinationUnavailable},
		{routingContext: 2, want: DestinationCongested},
	} {
		scope := conn.destinationKey(nil, pointCode)
		scope.routingContext = test.routingContext
		scope.routingContextSet = true
		state, known := conn.destinations.lookup(scope)
		if !known || state != test.want {
			t.Errorf("RC %d = (%v, known=%v), want %v and known",
				test.routingContext, state, known, test.want)
		}
	}
}

// Availability is resolved over the ranges that actually cover the congestion
// report, exactly as a lookup resolves it: a wider unavailable range covers a
// point code inside it, and a single unavailable point code does not make the
// range around it unavailable.
func TestSCONResolvesAvailabilityThroughCoveringRangesOnly(t *testing.T) {
	t.Run("a covering range is preserved", func(t *testing.T) {
		conn, _ := ssnmConn(t)
		if err := conn.handleDestinationUnavailable(messages.NewDestinationUnavailable(
			nil, nil, params.NewAffectedPointCodeWithMask(8, 0x123400), nil)); err != nil {
			t.Fatalf("range DUNA: %v", err)
		}
		if err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
			nil, nil, apc(0x123412), nil, params.NewCongestionIndications(2), nil)); err != nil {
			t.Fatalf("exact SCON: %v", err)
		}
		if got := conn.DestinationState(0x123412); got != DestinationUnavailable {
			t.Errorf("state = %v, want %v: the SCON sits inside an unavailable range",
				got, DestinationUnavailable)
		}
	})

	t.Run("a narrower record does not cover the report", func(t *testing.T) {
		conn, _ := ssnmConn(t)
		// The unavailable point code is the range's own base, so the two records
		// differ only by mask: a record narrower than the report never covers
		// it, however the point codes line up.
		if err := conn.handleDestinationUnavailable(messages.NewDestinationUnavailable(
			nil, nil, apc(0x123400), nil)); err != nil {
			t.Fatalf("exact DUNA: %v", err)
		}
		if err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
			nil, nil, params.NewAffectedPointCodeWithMask(8, 0x123400), nil,
			params.NewCongestionIndications(2), nil)); err != nil {
			t.Fatalf("range SCON: %v", err)
		}
		scope := conn.destinationKey(nil, 0x123400)
		scope.routingContext = 1
		scope.routingContextSet = true
		state, known := conn.destinations.lookupRange(scope, 0x123400, 8)
		if !known || state != DestinationCongested {
			t.Errorf("range state = (%v, known=%v), want %v: one point code does not make the range unreachable",
				state, known, DestinationCongested)
		}
		if got := conn.DestinationState(0x123412); got != DestinationCongested {
			t.Errorf("exact state = %v, want %v: the newer range covers it", got, DestinationCongested)
		}
	})

	t.Run("the newest covering record wins", func(t *testing.T) {
		conn, _ := ssnmConn(t)
		if err := conn.handleDestinationUnavailable(messages.NewDestinationUnavailable(
			nil, nil, params.NewAffectedPointCodeWithMask(8, 0x123400), nil)); err != nil {
			t.Fatalf("range DUNA: %v", err)
		}
		if err := conn.handleDestinationAvailable(messages.NewDestinationAvailable(
			nil, nil, apc(0x123412), nil)); err != nil {
			t.Fatalf("exact DAVA: %v", err)
		}
		if err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
			nil, nil, apc(0x123412), nil, params.NewCongestionIndications(2), nil)); err != nil {
			t.Fatalf("exact SCON: %v", err)
		}
		if got := conn.DestinationState(0x123412); got != DestinationCongested {
			t.Errorf("state = %v, want %v: the DAVA is newer than the range that covers it",
				got, DestinationCongested)
		}
	})
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
		Scope: WireScope{
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

// Resolving the availability a congestion report preserves must cost one pass
// over the retained records, not one per Affected Point Code. RFC 4666 Section
// 3.4.4 lets one SCON name as many destinations as the Affected Point Code
// parameter holds, so a per-point-code scan multiplies the work a peer can buy
// with a single message by the size of the store.
//
// This is a shape guard rather than a benchmark: the budget is two orders of
// magnitude above the measured cost, and a per-point-code scan overruns it.
func TestSCONCostStaysLinearInAffectedPointCodes(t *testing.T) {
	conn, _ := ssnmConn(t)
	affected := make([]uint32, DefaultMaxAffectedPointCodesPerSSNM)
	for index := range affected {
		affected[index] = uint32(index) + 1<<20
	}
	// Leave the congestion report room inside the budget, so what is measured is
	// the resolution work rather than a refusal.
	for pointCode := 0; pointCode < DefaultMaxSSNMDestinationRecords-len(affected); pointCode++ {
		if err := duna(conn, uint32(pointCode)); err != nil {
			t.Fatalf("filling the store: %v", err)
		}
	}

	const rounds = 10
	start := time.Now()
	for round := 0; round < rounds; round++ {
		if err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
			nil, nil, params.NewAffectedPointCode(affected...), nil,
			params.NewCongestionIndications(2), nil)); err != nil {
			t.Fatalf("SCON round %d: %v", round, err)
		}
	}
	elapsed := time.Since(start)
	t.Logf("SCON naming %d destinations against %d retained records: %v per message",
		len(affected), retainedDestinationRecords(conn), elapsed/rounds)
	if budget := time.Second; elapsed > budget {
		t.Errorf("%d SCON messages took %v, over the %v budget: the availability a congestion report preserves is being resolved per point code",
			rounds, elapsed, budget)
	}
}
