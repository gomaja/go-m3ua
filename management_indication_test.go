// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"reflect"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// ManagementIndication carried two projections of information it already
// reported in full: a scalar "first Routing Context" beside the complete
// RoutingContexts scope, and an unmasked AffectedPointCodes list beside the
// AffectedDestinations that keep each point code's mask and its exact Network
// Appearance and Routing Context.
//
// Both were lossy by construction. RFC 4666 Section 3.8.1 makes the Routing
// Context "Mandatory*" on Error and says of the "Invalid Routing Context"
// error that "the invalid Routing Context(s) MUST be included in the Error
// message" -- plural, because a peer may refuse several at once, and reading
// only the first hides the rest. The Affected Point Code parameter is a
// sequence of Mask/Point Code words (Section 3.4.1), so a list of bare point
// codes turns "everything under this mask" into one destination.
//
// A reader that had both had to know which one was authoritative. Now there is
// one representation of each: the exact wire scope, and the resolved AS
// membership.
func TestManagementIndicationHasNoCompatibilityProjections(t *testing.T) {
	indicationType := reflect.TypeOf(ManagementIndication{})

	for _, removed := range []struct{ field, replacement string }{
		{"RoutingContext", "RoutingContexts carries every Routing Context the peer named"},
		{"RoutingContextSet", "RoutingContexts is empty when the peer named none"},
		{"AffectedPointCodes", "AffectedDestinations carries every point code with its mask and scope"},
	} {
		if field, exists := indicationType.FieldByName(removed.field); exists {
			t.Errorf("ManagementIndication still projects %s %s; %s",
				field.Name, field.Type, removed.replacement)
		}
	}

	for _, retained := range []string{
		"Kind", "Association", "ASKeys", "StatusType", "StatusInfo", "ErrorCode",
		"RoutingContexts", "ASPIdentifier", "ASPIdentifierSet",
		"NetworkAppearance", "NetworkAppearanceSet", "AffectedDestinations",
		"Cause", "Description",
	} {
		if _, exists := indicationType.FieldByName(retained); !exists {
			t.Errorf("ManagementIndication lost %s, which carries information no other field does", retained)
		}
	}
}

// RFC 4666 Section 3.8.1: "The 'Invalid Routing Context' error is sent if a
// message is received from a peer with an invalid (unconfigured) Routing
// Context value. For this error, the invalid Routing Context(s) MUST be
// included in the Error message."
//
// So a perfectly well-formed ERROR arrives naming a Routing Context this
// association is not configured for -- that is the whole point of the error --
// and the indication must say which context the peer refused. Resolving the
// wire scope into the association's configured AS membership answers a
// different question and loses the only value the message carried.
func TestManagementErrorKeepsAnInvalidRoutingContextOffResolvedMembership(t *testing.T) {
	association, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	setInventoryNetworkAppearance(&association.cfg.ApplicationServers, params.NewNetworkAppearance(10))
	association.noteRoutingContextsActive([]uint32{1, 2})

	raw, err := messages.NewError(
		params.NewErrorCode(params.ErrInvalidRoutingContext),
		params.NewRoutingContext(99, 100),
		nil,
		nil,
		nil,
	).MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	association.dispatchRaw(context.Background(), inbound{data: raw, ppid: M3UAPPID})
	if rejected := firstErr(association); rejected != nil {
		t.Fatalf("a conformant Invalid Routing Context ERROR was rejected: %v", rejected)
	}

	indication := <-association.ManagementIndications()
	if indication.Kind != ManagementError {
		t.Fatalf("Kind = %v, want ManagementError", indication.Kind)
	}
	if indication.ErrorCode != params.ErrInvalidRoutingContext {
		t.Errorf("ErrorCode = %#x, want %#x", indication.ErrorCode, params.ErrInvalidRoutingContext)
	}
	if !equalNotifyScope(indication.RoutingContexts, []uint32{99, 100}) {
		t.Errorf("RoutingContexts = %v, want [99 100]; the peer said which contexts it refused",
			indication.RoutingContexts)
	}

	// The wire named two contexts that resolve to no configured Application
	// Server, so the exact scope is all there is. Substituting the configured
	// membership would report the two Application Servers the peer did not
	// mention and drop the two it did.
	wantKeys := []ASKey{
		{RoutingContext: 99, RoutingContextSet: true},
		{RoutingContext: 100, RoutingContextSet: true},
	}
	if !reflect.DeepEqual(indication.ASKeys, wantKeys) {
		t.Errorf("ASKeys = %+v, want %+v; an invalid Routing Context must not be "+
			"collapsed into resolved AS membership", indication.ASKeys, wantKeys)
	}
	for _, key := range indication.ASKeys {
		if key.RoutingContextSet && (key.RoutingContext == 1 || key.RoutingContext == 2) {
			t.Errorf("ASKeys reported configured membership %+v for an ERROR that named "+
				"neither context", key)
		}
	}

	// Section 3.8.1: "Error messages MUST NOT be generated in response to other
	// Error messages", and the refusal concerns one message, not the link.
	for _, message := range *sent {
		if _, isError := message.(*messages.Error); isError {
			t.Fatalf("answered a peer ERROR with an ERROR: %v", message)
		}
	}
	if got := association.State(); got != StateASPActive {
		t.Errorf("state = %v after an ERROR naming unconfigured contexts, want %v",
			got, StateASPActive)
	}
}

// The same message with an Affected Point Code: the destinations the peer named
// must stay in the scope the peer named them in, masks included.
func TestManagementErrorScopesAffectedDestinationsToTheRefusedContext(t *testing.T) {
	_, association := trackedManagementAssociation(t, StateASPActive, 10, 1, 2)

	if err := association.handleError(messages.NewError(
		params.NewErrorCode(params.ErrDestinationStatusUnknown),
		params.NewRoutingContext(99),
		nil,
		params.NewAffectedPointCode(uint32(4)<<24|0x123456),
		nil,
	)); err != nil {
		t.Fatalf("handleError: %v", err)
	}

	indication := <-association.ManagementIndications()
	want := []AffectedDestination{{
		RoutingContext: 99, RoutingContextSet: true,
		PointCode: 0x123456, Mask: 4,
	}}
	if !reflect.DeepEqual(indication.AffectedDestinations, want) {
		t.Errorf("AffectedDestinations = %+v, want %+v", indication.AffectedDestinations, want)
	}
	if !equalNotifyScope(indication.RoutingContexts, []uint32{99}) {
		t.Errorf("RoutingContexts = %v, want [99]", indication.RoutingContexts)
	}
}

// A Routing Context parameter whose value cannot be decoded is not the same as
// an omitted one. Omission means "configuration identifies the Application
// Servers"; an undecodable value means nothing at all is known, and inferring
// the configured membership from it reports Application Servers the peer never
// referred to.
//
// The wire cannot produce this: RFC 4666 gives the parameter as "Routing
// Context: n x 32 bits (unsigned integer)" (Section 3.4.1), so the parameter
// codec rejects a value that is not a non-zero multiple of four octets before
// any handler sees it. The guard is against a caller-built message reaching the
// same code path.
func TestManagementErrorWithAnUndecodableRoutingContextInfersNoMembership(t *testing.T) {
	_, association := trackedManagementAssociation(t, StateASPActive, 10, 1, 2)

	message := messages.NewError(
		params.NewErrorCode(params.ErrInvalidRoutingContext), nil, nil, nil, nil)
	message.RoutingContext = params.NewParam(int(params.RoutingContext), []byte{0x00, 0x00, 0x09})

	if err := association.handleError(message); err != nil {
		t.Fatalf("handleError: %v", err)
	}
	indication := <-association.ManagementIndications()
	if len(indication.RoutingContexts) != 0 {
		t.Errorf("RoutingContexts = %v for an undecodable value, want none", indication.RoutingContexts)
	}
	if len(indication.ASKeys) != 0 {
		t.Errorf("ASKeys = %+v for an undecodable Routing Context, want none; the "+
			"configured membership is not what the peer named", indication.ASKeys)
	}
}

// An ERROR that names no Routing Context does mean "configuration identifies
// the Application Servers", so the resolved membership is the right answer
// there -- and the exact wire scope stays empty to say the peer named none.
func TestManagementErrorWithoutARoutingContextReportsConfiguredMembership(t *testing.T) {
	_, association := trackedManagementAssociation(t, StateASPActive, 10, 1, 2)

	if err := association.handleError(messages.NewError(
		params.NewErrorCode(params.UnexpectedMessageError), nil, nil, nil, nil)); err != nil {
		t.Fatalf("handleError: %v", err)
	}
	indication := <-association.ManagementIndications()
	if len(indication.RoutingContexts) != 0 {
		t.Errorf("RoutingContexts = %v for an ERROR that named none, want empty",
			indication.RoutingContexts)
	}
	wantKeys := []ASKey{
		{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true},
		{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 2, RoutingContextSet: true},
	}
	if !reflect.DeepEqual(indication.ASKeys, wantKeys) {
		t.Errorf("ASKeys = %+v, want %+v", indication.ASKeys, wantKeys)
	}
}
