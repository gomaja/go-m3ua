// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// RFC 4666 Section 3.4.1 permits both Routing Context and Affected Point Code
// to be variable-length 32-bit lists. Their cross product is not part of the
// wire meaning: each point-code update applies to one shared set of contexts.
// Retaining one record per pair lets a small legal peer message allocate a
// quadratic destination table.
func TestInboundSSNMBatchedScopesRemainLinear(t *testing.T) {
	const entries = 256

	routingContexts := make([]uint32, entries)
	affectedPointCodes := make([]uint32, entries)
	for index := range routingContexts {
		// Keep explicit zero in the scope list: zero is a valid Routing Context
		// and must not be confused with an absent Routing Context parameter.
		routingContexts[index] = uint32(index)
		affectedPointCodes[index] = 0x00100000 + uint32(index)
	}

	message := messages.NewDestinationUnavailable(
		nil,
		params.NewRoutingContext(routingContexts...),
		params.NewAffectedPointCode(affectedPointCodes...),
		nil,
	)
	if got, want := message.MarshalLen(), 2064; got != want {
		t.Fatalf("DUNA wire length = %d, want %d", got, want)
	}
	wire, err := message.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal DUNA: %v", err)
	}
	decoded, err := messages.Parse(wire)
	if err != nil {
		t.Fatalf("parse DUNA: %v", err)
	}
	duna, ok := decoded.(*messages.DestinationUnavailable)
	if !ok {
		t.Fatalf("decoded message = %T, want *messages.DestinationUnavailable", decoded)
	}

	connection, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, routingContexts...)
	if err := connection.handleDestinationUnavailable(duna); err != nil {
		t.Fatalf("handle DUNA: %v", err)
	}

	if got, want := len(connection.destinations.state), entries; got != want {
		t.Fatalf("retained destination records = %d, want one per affected point code (%d); "+
			"the Routing Context list must be shared rather than materialized as a cross product", got, want)
	}
	for _, routingContext := range []uint32{0, entries / 2, entries - 1} {
		for _, pointCode := range []uint32{
			affectedPointCodes[0],
			affectedPointCodes[entries/2],
			affectedPointCodes[entries-1],
		} {
			scope := connection.destinationKey(nil, pointCode)
			scope.routingContext = routingContext
			scope.routingContextSet = true
			state, known := connection.destinations.lookup(scope)
			if !known || state != DestinationUnavailable {
				t.Errorf("RC %d PC %#x = (%v, known=%v), want Unavailable and known",
					routingContext, pointCode, state, known)
			}
		}
	}

	unknownScope := connection.destinationKey(nil, affectedPointCodes[0])
	unknownScope.routingContext = entries
	unknownScope.routingContextSet = true
	if _, known := connection.destinations.lookup(unknownScope); known {
		t.Fatal("a Routing Context absent from the DUNA scope inherited its destination state")
	}
}

func TestInboundSSNMBatchedScopesCanonicalizeAndRespectNewerSubset(t *testing.T) {
	connection, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 0, 1, 2)
	const firstPointCode = uint32(0x101001)
	const secondPointCode = uint32(0x101002)

	if err := connection.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		nil,
		params.NewRoutingContext(2, 0, 1, 1),
		params.NewAffectedPointCode(firstPointCode, firstPointCode, secondPointCode),
		nil,
	)); err != nil {
		t.Fatalf("batched DUNA: %v", err)
	}
	if got, want := len(connection.destinations.state), 2; got != want {
		t.Fatalf("duplicate DUNA retained %d records, want %d canonical point-code records", got, want)
	}

	for _, routingContext := range []uint32{0, 1, 2} {
		for _, pointCode := range []uint32{firstPointCode, secondPointCode} {
			scope := connection.destinationKey(nil, pointCode)
			scope.routingContext = routingContext
			scope.routingContextSet = true
			state, known := connection.destinations.lookup(scope)
			if !known || state != DestinationUnavailable {
				t.Errorf("initial DUNA RC %d PC %#x = (%v, known=%v), want Unavailable and known",
					routingContext, pointCode, state, known)
			}
		}
	}

	if err := connection.handleDestinationAvailable(messages.NewDestinationAvailable(
		nil,
		params.NewRoutingContext(1),
		params.NewAffectedPointCode(firstPointCode),
		nil,
	)); err != nil {
		t.Fatalf("subset DAVA: %v", err)
	}
	if got, want := len(connection.destinations.state), 3; got != want {
		t.Fatalf("subset DAVA retained %d records, want initial scope plus one newer point-code record", got)
	}
	for _, test := range []struct {
		routingContext uint32
		want           DestinationState
	}{
		{routingContext: 0, want: DestinationUnavailable},
		{routingContext: 1, want: DestinationAvailable},
		{routingContext: 2, want: DestinationUnavailable},
	} {
		scope := connection.destinationKey(nil, firstPointCode)
		scope.routingContext = test.routingContext
		scope.routingContextSet = true
		state, known := connection.destinations.lookup(scope)
		if !known || state != test.want {
			t.Errorf("subset DAVA RC %d = (%v, known=%v), want %v and known",
				test.routingContext, state, known, test.want)
		}
	}
}
