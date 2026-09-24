// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func affectedPointCodeParamOf(message messages.M3UA) *params.Param {
	switch value := message.(type) {
	case *messages.DestinationUnavailable:
		return value.AffectedPointCode
	case *messages.DestinationAvailable:
		return value.AffectedPointCode
	case *messages.DestinationRestricted:
		return value.AffectedPointCode
	case *messages.SignallingCongestion:
		return value.AffectedPointCode
	default:
		return nil
	}
}

func requireDestinationStateForScope(t *testing.T, conn *Association, networkAppearance, routingContext, pointCode uint32, want DestinationAvailability) {
	t.Helper()
	if got := retainedAvailabilityForNetworkAndRoutingContext(conn, networkAppearance, routingContext, pointCode); got != want {
		t.Fatalf("state for NA=%d RC=%d PC=%#x = %v, want %v",
			networkAppearance, routingContext, pointCode, got, want)
	}
}

// destinationScopeKey is one exact Network Appearance and Routing Context
// scope, the scope the removed per-scope setters and queries resolved.
func destinationScopeKey(networkAppearance, routingContext uint32) destinationKey {
	return destinationKey{
		networkAppearance:    networkAppearance,
		networkAppearanceSet: true,
		routingContext:       routingContext,
		routingContextSet:    true,
	}
}

// seedDestinationRangeInScope records one availability range in an exact
// Network Appearance and Routing Context.
func seedDestinationRangeInScope(
	store *destinations,
	networkAppearance, routingContext, pointCode uint32,
	mask uint8,
	state DestinationNetworkState,
) {
	store.setRanges([]destinationRange{{
		NetworkAppearance:    networkAppearance,
		NetworkAppearanceSet: true,
		RoutingContext:       routingContext,
		RoutingContextSet:    true,
		PointCode:            pointCode,
		Mask:                 mask,
		State:                state,
	}})
}

// seedDestinationRangeForNetwork records one availability range as an
// all-Routing-Context baseline in an explicit Network Appearance.
func seedDestinationRangeForNetwork(
	store *destinations,
	networkAppearance, pointCode uint32,
	mask uint8,
	state DestinationNetworkState,
) {
	store.setRanges([]destinationRange{{
		NetworkAppearance:    networkAppearance,
		NetworkAppearanceSet: true,
		PointCode:            pointCode,
		Mask:                 mask,
		State:                state,
	}})
}

// seedDestinationCongestionInScope records a congestion range. RFC 4666
// Section 4.5.2.2 keeps congestion separate from availability, so the record
// has to be written in the congestion dimension alone or it would restate a
// reachability nothing reported.
func seedDestinationCongestionInScope(
	store *destinations,
	networkAppearance, routingContext, pointCode uint32,
	mask uint8,
	state DestinationNetworkState,
) {
	_ = store.setCongestionRangesWithinBudget([]destinationRange{{
		NetworkAppearance:    networkAppearance,
		NetworkAppearanceSet: true,
		RoutingContext:       routingContext,
		RoutingContextSet:    true,
		PointCode:            pointCode,
		Mask:                 mask,
		State:                state,
	}})
}

// destinationRangesInScope reads back every range retained in one exact
// Network Appearance and Routing Context, in update order.
func destinationRangesInScope(store *destinations, networkAppearance, routingContext uint32) []destinationRange {
	return store.rangesForScope(destinationScopeKey(networkAppearance, routingContext))
}

// destinationStateInScope reads a destination's retained state in one exact
// Network Appearance and Routing Context, and whether it is one this node has
// been told about at all.
func destinationStateInScope(
	store *destinations,
	networkAppearance, routingContext, pointCode uint32,
) (DestinationNetworkState, bool) {
	return store.lookupRange(destinationScopeKey(networkAppearance, routingContext), pointCode, 0)
}

func TestDestinationStatusPreservesAffectedPointCodeMask(t *testing.T) {
	for _, mask := range []uint8{0, 3, 8, 14, 24, 255} {
		t.Run(maskName(mask), func(t *testing.T) {
			conn, _ := newObservedSSNMConn(t, StateASPActive, RoleASP, 1)
			const pointCode = uint32(0x123457)
			if err := conn.handleDestinationUnavailable(messages.NewDestinationUnavailable(
				params.NewNetworkAppearance(7), nil,
				params.NewAffectedPointCodeWithMask(mask, pointCode), nil,
			)); err != nil {
				t.Fatal(err)
			}
			status := nextSSNMReport(t, conn)
			if status.Destinations[0].PointCode != pointCode {
				t.Errorf("PointCode = %#x, want %#x", status.Destinations[0].PointCode, pointCode)
			}
			if status.Destinations[0].Mask != mask {
				t.Errorf("Mask = %d, want %d", status.Destinations[0].Mask, mask)
			}
		})
	}

	t.Run("positionally matches several APCs", func(t *testing.T) {
		conn, _ := newObservedSSNMConn(t, StateASPActive, RoleASP, 1)
		apc := params.NewAffectedPointCode(
			uint32(3)<<24|0x111117,
			uint32(14)<<24|0x222222,
		)
		if err := conn.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, nil, apc, nil)); err != nil {
			t.Fatal(err)
		}
		report := nextSSNMReport(t, conn)
		if len(report.Destinations) != 2 {
			t.Fatalf("report destinations = %v", report.Destinations)
		}
		first, second := report.Destinations[0], report.Destinations[1]
		if first.PointCode != 0x111117 || first.Mask != 3 {
			t.Errorf("first destination = %+v", first)
		}
		if second.PointCode != 0x222222 || second.Mask != 14 {
			t.Errorf("second destination = %+v", second)
		}
	})

	t.Run("peer congestion report", func(t *testing.T) {
		conn, _ := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
		conn.noteRoutingContextsActive([]uint32{1})
		if err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
			nil, nil, params.NewAffectedPointCodeWithMask(8, 0x1234aa), nil, nil, nil,
		)); err != nil {
			t.Fatal(err)
		}
		status := nextSSNMReport(t, conn)
		if status.Destinations[0].Mask != 8 || !status.PeerReported {
			t.Errorf("peer SCON status = %+v, want Mask 8 peer report", status)
		}
	})
}

func TestDestinationRangeMaskMatching(t *testing.T) {
	for _, test := range []struct {
		name    string
		mask    uint8
		stored  uint32
		inside  uint32
		outside uint32
		all     bool
	}{
		{name: "mask 0", mask: 0, stored: 0x123456, inside: 0x123456, outside: 0x123457},
		{name: "mask 3", mask: 3, stored: 0x123457, inside: 0x123450, outside: 0x123448},
		{name: "mask 8", mask: 8, stored: 0x1234aa, inside: 0x1234ff, outside: 0x123500},
		{name: "mask 14", mask: 14, stored: 0x123456, inside: 0x120001, outside: 0x127456},
		{name: "mask 24", mask: 24, stored: 0x123456, inside: 0xfedcba, all: true},
		{name: "mask 255", mask: 255, stored: 0x123456, inside: 0xfedcba, all: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn, _ := newObservedSSNMConn(t, StateASPActive, RoleASP, 1)
			if err := conn.handleDestinationUnavailable(messages.NewDestinationUnavailable(
				params.NewNetworkAppearance(7), nil,
				params.NewAffectedPointCodeWithMask(test.mask, test.stored), nil,
			)); err != nil {
				t.Fatal(err)
			}

			requireDestinationStateForScope(t, conn, 7, 1, test.inside, DestinationUnavailable)
			if !test.all {
				requireDestinationStateForScope(t, conn, 7, 1, test.outside, DestinationAvailable)
			}
		})
	}
}

func TestNewestMatchingDestinationRangeWins(t *testing.T) {
	conn, _ := newObservedSSNMConn(t, StateASPActive, RoleASP, 1)
	const networkAppearance = uint32(7)
	const pointCode = uint32(0x123456)

	if err := conn.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		params.NewNetworkAppearance(networkAppearance), nil,
		params.NewAffectedPointCodeWithMask(8, pointCode), nil,
	)); err != nil {
		t.Fatal(err)
	}
	requireDestinationStateForScope(t, conn, networkAppearance, 1, pointCode, DestinationUnavailable)
	requireDestinationStateForScope(t, conn, networkAppearance, 1, 0x1234aa, DestinationUnavailable)

	if err := conn.handleDestinationAvailable(messages.NewDestinationAvailable(
		params.NewNetworkAppearance(networkAppearance), nil,
		params.NewAffectedPointCodeWithMask(0, pointCode), nil,
	)); err != nil {
		t.Fatal(err)
	}
	requireDestinationStateForScope(t, conn, networkAppearance, 1, pointCode, DestinationAvailable)
	requireDestinationStateForScope(t, conn, networkAppearance, 1, 0x1234aa, DestinationUnavailable)

	if err := conn.handleDestinationRestricted(messages.NewDestinationRestricted(
		params.NewNetworkAppearance(networkAppearance), nil,
		params.NewAffectedPointCodeWithMask(3, pointCode), nil,
	)); err != nil {
		t.Fatal(err)
	}
	requireDestinationStateForScope(t, conn, networkAppearance, 1, pointCode, DestinationRestricted)

	if err := conn.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		params.NewNetworkAppearance(networkAppearance), nil,
		params.NewAffectedPointCodeWithMask(0, pointCode), nil,
	)); err != nil {
		t.Fatal(err)
	}
	requireDestinationStateForScope(t, conn, networkAppearance, 1, pointCode, DestinationUnavailable)
}

func TestEquivalentDestinationRangeUpdateReplacesItsCanonicalPrefix(t *testing.T) {
	conn, _ := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
	seedDestinationRangeInScope(
		conn.destinations, 7, 1, 0x1234aa, 8, availabilityState(DestinationUnavailable))
	seedDestinationRangeInScope(
		conn.destinations, 7, 1, 0x123455, 8, availabilityState(DestinationRestricted))

	ranges := destinationRangesInScope(conn.destinations, 7, 1)
	if len(ranges) != 1 {
		t.Fatalf("equivalent range records = %#v, want one latest record", ranges)
	}
	if ranges[0].PointCode != 0x123455 || ranges[0].Mask != 8 ||
		ranges[0].State != availabilityState(DestinationRestricted) {
		t.Errorf("latest range = %#v, want PC 0x123455 Mask 8 Restricted", ranges[0])
	}
	requireDestinationStateForScope(t, conn, 7, 1, 0x1234ff, DestinationRestricted)
}

func TestDestinationRangeSettersNormalizePointCodesTo24Bits(t *testing.T) {
	conn, _ := newObservedSSNMConn(t, StateASPActive, RoleSGP, 0)
	seedDestinationRangeInScope(
		conn.destinations, 0, 0, 0xff123456, 3, availabilityState(DestinationUnavailable))

	ranges := destinationRangesInScope(conn.destinations, 0, 0)
	if len(ranges) != 1 {
		t.Fatalf("ranges = %#v, want one", ranges)
	}
	if ranges[0].PointCode != 0x123456 || !ranges[0].NetworkAppearanceSet ||
		!ranges[0].RoutingContextSet {
		t.Errorf("normalized zero-valued scope = %#v, want explicit NA 0 RC 0 PC 0x123456", ranges[0])
	}
}

func TestDestinationStateIsScopedByNetworkAndRoutingContext(t *testing.T) {
	conn, _ := newObservedSSNMConn(t, StateASPActive, RoleASP, 1, 2)
	conn.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	const pointCode = uint32(0x234567)

	if err := conn.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		params.NewNetworkAppearance(7), params.NewRoutingContext(1, 2),
		params.NewAffectedPointCodeWithMask(3, pointCode), nil,
	)); err != nil {
		t.Fatal(err)
	}
	if err := conn.handleDestinationAvailable(messages.NewDestinationAvailable(
		params.NewNetworkAppearance(7), params.NewRoutingContext(2),
		params.NewAffectedPointCodeWithMask(3, pointCode), nil,
	)); err != nil {
		t.Fatal(err)
	}
	if err := conn.handleDestinationRestricted(messages.NewDestinationRestricted(
		params.NewNetworkAppearance(8), params.NewRoutingContext(1),
		params.NewAffectedPointCodeWithMask(3, pointCode), nil,
	)); err != nil {
		t.Fatal(err)
	}

	requireDestinationStateForScope(t, conn, 7, 1, pointCode, DestinationUnavailable)
	requireDestinationStateForScope(t, conn, 7, 2, pointCode, DestinationAvailable)
	requireDestinationStateForScope(t, conn, 8, 1, pointCode, DestinationRestricted)
	requireDestinationStateForScope(t, conn, 8, 2, pointCode, DestinationAvailable)

	if got := retainedAvailabilityForNetwork(conn, 7, pointCode); got != DestinationAvailable {
		t.Errorf("legacy multi-RC query = %v, want baseline/default Available", got)
	}

	t.Run("omitted RC resolves the single configured flow", func(t *testing.T) {
		single, _ := newObservedSSNMConn(t, StateASPActive, RoleASP, 9)
		if err := single.handleDestinationUnavailable(messages.NewDestinationUnavailable(
			params.NewNetworkAppearance(7), nil,
			params.NewAffectedPointCodeWithMask(0, pointCode), nil,
		)); err != nil {
			t.Fatal(err)
		}
		requireDestinationStateForScope(t, single, 7, 9, pointCode, DestinationUnavailable)
		ranges := destinationRangesInScope(single.destinations, 7, 9)
		if len(ranges) != 1 || !ranges[0].RoutingContextSet || ranges[0].RoutingContext != 9 {
			t.Errorf("omitted-RC range scope = %#v, want sole Routing Context 9", ranges)
		}
	})
}

func TestDestinationScopePresenceIsDistinctFromExplicitZero(t *testing.T) {
	conn, _ := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
	setInventoryNetworkAppearance(&conn.cfg.ApplicationServers, nil)
	const pointCode = uint32(0x234567)
	seedDestinationRangeInScope(
		conn.destinations, 0, 1, pointCode, 0, availabilityState(DestinationUnavailable))

	if got := retainedAvailability(conn, pointCode); got != DestinationAvailable {
		t.Errorf("absent-NA query = %v, want Available; explicit NA 0 must remain distinct", got)
	}
	requireDestinationStateForScope(t, conn, 0, 1, pointCode, DestinationUnavailable)

	t.Run("Routing Context", func(t *testing.T) {
		multi, _ := newObservedSSNMConn(t, StateASPActive, RoleSGP, 0, 1)
		seedDestinationRangeInScope(
			multi.destinations, 0, 0, pointCode, 0, availabilityState(DestinationUnavailable))
		if got := retainedAvailabilityForNetwork(multi, 0, pointCode); got != DestinationAvailable {
			t.Errorf("unscoped RC query = %v, want Available; explicit RC 0 must remain distinct", got)
		}
		requireDestinationStateForScope(t, multi, 0, 0, pointCode, DestinationUnavailable)
	})
}

func TestDestinationRangeSnapshotsPreserveScopeAndUpdateOrder(t *testing.T) {
	conn, _ := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1, 2)
	seedDestinationRangeForNetwork(
		conn.destinations, 7, 0x120001, 14, availabilityState(DestinationUnavailable))
	seedDestinationRangeInScope(
		conn.destinations, 7, 1, 0x1234aa, 8, availabilityState(DestinationRestricted))
	seedDestinationRangeInScope(
		conn.destinations, 7, 1, 0x123456, 0, availabilityState(DestinationAvailable))
	seedDestinationCongestionInScope(
		conn.destinations, 7, 2, 0x123456, 0, congestedState(2))

	got := destinationRangesInScope(conn.destinations, 7, 1)
	want := []destinationRange{
		{
			NetworkAppearance: 7, NetworkAppearanceSet: true,
			PointCode: 0x120001, Mask: 14, State: availabilityState(DestinationUnavailable),
		},
		{
			NetworkAppearance: 7, NetworkAppearanceSet: true,
			RoutingContext: 1, RoutingContextSet: true,
			PointCode: 0x1234aa, Mask: 8, State: availabilityState(DestinationRestricted),
		},
		{
			NetworkAppearance: 7, NetworkAppearanceSet: true,
			RoutingContext: 1, RoutingContextSet: true,
			PointCode: 0x123456, Mask: 0, State: availabilityState(DestinationAvailable),
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ranges for NA 7 RC 1 = %#v, want %#v", got, want)
	}
}

func TestListenerDestinationRangesProvideAllRCBaselineAndScopedOverride(t *testing.T) {
	config := newSGPAssociationConfigForTest(&HeartbeatInfo{Enabled: false}, 1, params.TrafficModeLoadshare, 7, []uint32{1, 2})
	listener := newSGPListener(NewListenerConfig(config))
	store := listenerEndpoint(t, listener).destinations
	seedDestinationRangeForNetwork(store, 7, 0x1234aa, 8, availabilityState(DestinationUnavailable))
	seedDestinationRangeInScope(store, 7, 2, 0x123456, 0, availabilityState(DestinationAvailable))

	if state, known := destinationStateInScope(store, 7, 1, 0x123456); !known ||
		state.Availability != DestinationUnavailable {
		t.Errorf("RC1 state = %v (known=%v), want Unavailable", state, known)
	}
	if state, known := destinationStateInScope(store, 7, 2, 0x123456); !known ||
		state.Availability != DestinationAvailable {
		t.Errorf("RC2 state = %v (known=%v), want Available", state, known)
	}

	seedDestinationRangeForNetwork(store, 7, 0x123456, 0, availabilityState(DestinationRestricted))
	if state, known := destinationStateInScope(store, 7, 2, 0x123456); !known ||
		state.Availability != DestinationRestricted {
		t.Errorf("newer baseline state = %v (known=%v), want Restricted", state, known)
	}
	seedDestinationRangeInScope(store, 7, 2, 0x123456, 0, availabilityState(DestinationAvailable))
	if state, known := destinationStateInScope(store, 7, 2, 0x123456); !known ||
		state.Availability != DestinationAvailable {
		t.Errorf("newest scoped state = %v (known=%v), want Available", state, known)
	}
}

func TestDAUDPreservesAffectedPointCodeRanges(t *testing.T) {
	for _, mask := range []uint8{0, 3, 8, 14, 24, 255} {
		t.Run(maskName(mask), func(t *testing.T) {
			conn, sent := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
			setInventoryNetworkAppearance(&conn.cfg.ApplicationServers, params.NewNetworkAppearance(7))
			const pointCode = uint32(0x123457)
			seedDestinationRangeInScope(
				conn.destinations, 7, 1, pointCode, mask,
				availabilityState(DestinationRestricted),
			)
			if err := conn.handleDestinationStateAudit(messages.NewDestinationStateAudit(
				params.NewNetworkAppearance(7), nil,
				params.NewAffectedPointCodeWithMask(mask, pointCode), nil,
			)); err != nil {
				t.Fatal(err)
			}
			if len(*sent) != 1 {
				t.Fatalf("DAUD responses = %v, want one DRST", typeNames(*sent))
			}
			if (*sent)[0].MessageTypeName() != "Destination Restricted" {
				t.Fatalf("DAUD response = %s, want Destination Restricted", (*sent)[0].MessageTypeName())
			}
			apc := affectedPointCodeParamOf((*sent)[0])
			if apc == nil {
				t.Fatal("DAUD response omitted Affected Point Code")
			}
			if got := apc.AffectedPointCodes(); !reflect.DeepEqual(got, []uint32{pointCode}) {
				t.Errorf("response point codes = %v, want [%#x]", got, pointCode)
			}
			if got := apc.AffectedPointCodeMasks(); !reflect.DeepEqual(got, []uint8{mask}) {
				t.Errorf("response masks = %v, want [%d]", got, mask)
			}

			wire, err := (*sent)[0].MarshalBinary()
			if err != nil {
				t.Fatalf("marshal DAUD response: %v", err)
			}
			decoded, err := messages.Parse(wire)
			if err != nil {
				t.Fatalf("parse DAUD response wire: %v", err)
			}
			decodedAffectedPointCode := affectedPointCodeParamOf(decoded)
			if decodedAffectedPointCode == nil {
				t.Fatal("wire response omitted Affected Point Code")
			}
			if got := decodedAffectedPointCode.AffectedPointCodeMasks(); !reflect.DeepEqual(got, []uint8{mask}) {
				t.Errorf("wire response masks = %v, want [%d]", got, mask)
			}
		})
	}
}

func TestDAUDSplitsRoutingContextsByResolvedState(t *testing.T) {
	conn, sent := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1, 2)
	setInventoryNetworkAppearance(&conn.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	conn.noteRoutingContextsActive([]uint32{1, 2})
	const pointCode = uint32(0x234567)
	seedDestinationRangeInScope(conn.destinations, 7, 1, pointCode, 3, availabilityState(DestinationUnavailable))
	seedDestinationRangeInScope(conn.destinations, 7, 2, pointCode, 3, availabilityState(DestinationAvailable))

	if err := conn.handleDestinationStateAudit(messages.NewDestinationStateAudit(
		params.NewNetworkAppearance(7), params.NewRoutingContext(1, 2),
		params.NewAffectedPointCodeWithMask(3, pointCode), nil,
	)); err != nil {
		t.Fatal(err)
	}
	if got := typeNames(*sent); !reflect.DeepEqual(got, []string{"Destination Unavailable", "Destination Available"}) {
		t.Fatalf("DAUD responses = %v, want [Destination Unavailable Destination Available]", got)
	}
	for index, wantRC := range []uint32{1, 2} {
		routingContext := routingContextOf((*sent)[index])
		if routingContext == nil || !reflect.DeepEqual(routingContext.RoutingContexts(), []uint32{wantRC}) {
			t.Errorf("response %d Routing Contexts = %v, want [%d]",
				index, routingContext.RoutingContexts(), wantRC)
		}
		if masks := affectedPointCodeParamOf((*sent)[index]).AffectedPointCodeMasks(); !reflect.DeepEqual(masks, []uint8{3}) {
			t.Errorf("response %d masks = %v, want [3]", index, masks)
		}
	}

	*sent = nil
	seedDestinationRangeInScope(conn.destinations, 7, 2, pointCode, 3, availabilityState(DestinationUnavailable))
	if err := conn.handleDestinationStateAudit(messages.NewDestinationStateAudit(
		params.NewNetworkAppearance(7), params.NewRoutingContext(1, 2),
		params.NewAffectedPointCodeWithMask(3, pointCode), nil,
	)); err != nil {
		t.Fatal(err)
	}
	if got := typeNames(*sent); !reflect.DeepEqual(got, []string{"Destination Unavailable"}) {
		t.Fatalf("same-state DAUD responses = %v, want one DUNA", got)
	}
	if routingContext := routingContextOf((*sent)[0]); routingContext == nil ||
		!reflect.DeepEqual(routingContext.RoutingContexts(), []uint32{1, 2}) {
		t.Errorf("combined response Routing Contexts = %v, want [1 2]", routingContext.RoutingContexts())
	}
}

func TestDAUDCongestedRangePreservesMaskOnBothReplies(t *testing.T) {
	conn, sent := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
	setInventoryNetworkAppearance(&conn.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	const pointCode = uint32(0x456789)
	// Congestion without an explicit level: the record the removed conflated
	// state stood for. Availability is untouched, so the audit answers SCON for
	// the congestion and DAVA for the reachability RFC 4666 Section 4.5.2.2
	// keeps separate from it.
	seedDestinationCongestionInScope(
		conn.destinations, 7, 1, pointCode, 14,
		DestinationNetworkState{Congestion: congestionStateFor(0, false)})

	if err := conn.handleDestinationStateAudit(messages.NewDestinationStateAudit(
		params.NewNetworkAppearance(7), nil,
		params.NewAffectedPointCodeWithMask(14, pointCode), nil,
	)); err != nil {
		t.Fatal(err)
	}
	if got := typeNames(*sent); !reflect.DeepEqual(got, []string{"Signalling Congestion", "Destination Available"}) {
		t.Fatalf("responses = %v, want [Signalling Congestion Destination Available]", got)
	}
	for index, response := range *sent {
		if masks := affectedPointCodeParamOf(response).AffectedPointCodeMasks(); !reflect.DeepEqual(masks, []uint8{14}) {
			t.Errorf("response %d masks = %v, want [14]", index, masks)
		}
	}
}

func TestDestinationRangeDAUDOverAssociation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	aspAssociation, sgpAssociation, err := setupConn(t, ctx, 3296)
	if err != nil {
		t.Fatal(err)
	}
	observeSSNM(t, aspAssociation)
	defer func() {
		_ = aspAssociation.Close()
		_ = sgpAssociation.Close()
	}()

	for _, test := range []struct {
		name      string
		pointCode uint32
		mask      uint8
		state     DestinationAvailability
	}{
		{name: "range", pointCode: 0x456789, mask: 14, state: DestinationRestricted},
		{name: "exact control", pointCode: 0x654321, mask: 0, state: DestinationAvailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			sgpAssociation.destinations.setRanges([]destinationRange{{
				NetworkAppearance:    0,
				NetworkAppearanceSet: true,
				RoutingContext:       1,
				RoutingContextSet:    true,
				PointCode:            test.pointCode,
				Mask:                 test.mask,
				State:                availabilityState(test.state),
			}})
			if _, err := aspAssociation.WriteSignal(messages.NewDestinationStateAudit(
				nil, params.NewRoutingContext(1),
				params.NewAffectedPointCodeWithMask(test.mask, test.pointCode), nil,
			)); err != nil {
				t.Fatalf("write DAUD: %v", err)
			}

			status := nextSSNMReport(t, aspAssociation)
			if status.Destinations[0].PointCode != test.pointCode || status.Destinations[0].Mask != test.mask ||
				reportedSSNMAvailability(t, status) != test.state || !status.Scope.RoutingContextSet ||
				!reflect.DeepEqual(status.Scope.RoutingContexts, []uint32{1}) {
				t.Errorf("wire status = %+v, want RC 1 point code %#x/%d %v",
					status, test.pointCode, test.mask, test.state)
			}
		})
	}
}

func TestDAUDRangeLookupDoesNotLetAnExactOverrideInventAWholeRangeState(t *testing.T) {
	conn, sent := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
	setInventoryNetworkAppearance(&conn.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	const pointCode = uint32(0x123456)
	seedDestinationRangeInScope(conn.destinations, 7, 1, pointCode, 8, availabilityState(DestinationUnavailable))
	seedDestinationRangeInScope(conn.destinations, 7, 1, pointCode, 0, availabilityState(DestinationAvailable))

	if err := conn.handleDestinationStateAudit(messages.NewDestinationStateAudit(
		params.NewNetworkAppearance(7), nil,
		params.NewAffectedPointCodeWithMask(8, pointCode), nil,
	)); err != nil {
		t.Fatal(err)
	}
	if got := typeNames(*sent); !reflect.DeepEqual(got, []string{"Destination Unavailable"}) {
		t.Fatalf("range DAUD responses = %v, want DUNA for the covering range record", got)
	}

	*sent = nil
	if err := conn.handleDestinationStateAudit(messages.NewDestinationStateAudit(
		params.NewNetworkAppearance(7), nil,
		params.NewAffectedPointCodeWithMask(0, pointCode), nil,
	)); err != nil {
		t.Fatal(err)
	}
	if got := typeNames(*sent); !reflect.DeepEqual(got, []string{"Destination Available"}) {
		t.Fatalf("exact DAUD responses = %v, want DAVA for the newer exact record", got)
	}
}

func TestSSNMMultiRoutingContextUpdateIsAtomic(t *testing.T) {
	conn, _ := newObservedSSNMConn(t, StateASPActive, RoleASP, 1, 2)
	conn.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	const networkAppearance = uint32(7)
	const pointCode = uint32(0x345678)
	routingContext := params.NewRoutingContext(1, 2)
	affectedPointCode := params.NewAffectedPointCodeWithMask(3, pointCode)

	if err := conn.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		params.NewNetworkAppearance(networkAppearance), routingContext.Copy(), affectedPointCode.Copy(), nil,
	)); err != nil {
		t.Fatal(err)
	}

	const updates = 500
	stop := make(chan struct{})
	failure := make(chan string, 1)
	var readers sync.WaitGroup
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}

				conn.destinations.mu.RLock()
				first, firstKnown := conn.destinations.lookupRangeLocked(destinationKey{
					networkAppearance: networkAppearance, networkAppearanceSet: true,
					routingContext: 1, routingContextSet: true,
				}, pointCode, 0)
				second, secondKnown := conn.destinations.lookupRangeLocked(destinationKey{
					networkAppearance: networkAppearance, networkAppearanceSet: true,
					routingContext: 2, routingContextSet: true,
				}, pointCode, 0)
				conn.destinations.mu.RUnlock()
				if firstKnown != secondKnown || first != second {
					select {
					case failure <- "Routing Contexts observed different states":
					default:
					}
					return
				}
			}
		}()
	}

	for index := range updates {
		var err error
		if index%2 == 0 {
			err = conn.handleDestinationAvailable(messages.NewDestinationAvailable(
				params.NewNetworkAppearance(networkAppearance), routingContext.Copy(), affectedPointCode.Copy(), nil))
		} else {
			err = conn.handleDestinationUnavailable(messages.NewDestinationUnavailable(
				params.NewNetworkAppearance(networkAppearance), routingContext.Copy(), affectedPointCode.Copy(), nil))
		}
		if err != nil {
			close(stop)
			readers.Wait()
			t.Fatal(err)
		}
	}
	close(stop)
	readers.Wait()
	select {
	case message := <-failure:
		t.Fatal(message)
	default:
	}
}

func TestPausePreservesDestinationRangeScope(t *testing.T) {
	conn, _ := newObservedSSNMConn(t, StateASPActive, RoleASP, 1, 2)
	seedDestinationRangeInScope(
		conn.destinations, 7, 2, 0x5678ab, 8, availabilityState(DestinationAvailable))

	conn.pauseDestinations()
	ranges := destinationRangesInScope(conn.destinations, 7, 2)
	if len(ranges) != 1 {
		t.Fatalf("paused ranges = %+v, want one", ranges)
	}
	retained := ranges[0]
	if retained.PointCode != 0x5678ab || retained.Mask != 8 ||
		retained.NetworkAppearance != 7 || !retained.NetworkAppearanceSet ||
		!retained.RoutingContextSet || retained.RoutingContext != 2 ||
		retained.State.Availability != DestinationUnavailable {
		t.Errorf("paused range = %+v, want scoped 0x5678ab/8 unavailable", retained)
	}
	requireDestinationStateForScope(t, conn, 7, 2, 0x5678ff, DestinationUnavailable)
	requireDestinationStateForScope(t, conn, 7, 1, 0x5678ff, DestinationAvailable)
	assertNoSSNMReport(t, conn)
}

func FuzzDestinationRangeCovers(f *testing.F) {
	for _, seed := range []struct {
		storedPointCode uint32
		storedMask      uint8
		queryPointCode  uint32
		queryMask       uint8
	}{
		{0x123456, 0, 0x123456, 0},
		{0x123457, 3, 0x123450, 0},
		{0x1234aa, 8, 0x1234ff, 3},
		{0x123456, 14, 0x120001, 8},
		{0x123456, 24, 0xfedcba, 24},
		{0x123456, 255, 0xfedcba, 24},
		{0x123456, 24, 0xfedcba, 255},
		{0x123456, 255, 0xfedcba, 255},
		{0x123456, 8, 0x123456, 14},
	} {
		f.Add(seed.storedPointCode, seed.storedMask, seed.queryPointCode, seed.queryMask)
	}

	f.Fuzz(func(t *testing.T, storedPointCode uint32, storedMask uint8, queryPointCode uint32, queryMask uint8) {
		storedBits := int(storedMask)
		if storedBits > 24 {
			storedBits = 24
		}
		queryBits := int(queryMask)
		if queryBits > 24 {
			queryBits = 24
		}
		want := storedBits >= queryBits
		if want && storedBits < 24 {
			lowBits := uint32(1<<storedBits) - 1
			want = storedPointCode&0x00ffffff&^lowBits ==
				queryPointCode&0x00ffffff&^lowBits
		}

		got := destinationRangeCovers(destinationRange{
			PointCode: storedPointCode,
			Mask:      storedMask,
		}, queryPointCode, queryMask)
		if got != want {
			t.Fatalf("covers(%#x/%d, %#x/%d) = %v, want %v",
				storedPointCode, storedMask, queryPointCode, queryMask, got, want)
		}
	})
}

func maskName(mask uint8) string {
	return "mask " + strconv.Itoa(int(mask))
}
