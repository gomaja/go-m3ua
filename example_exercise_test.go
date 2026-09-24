// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// example_test.go documents eight paths an application has to get right. Its
// example functions run, but two of them — the direct send and the inbound
// classification — can only show the decision an application makes, because an
// Association is reached through Dial or Accept and those need a peer.
//
// This file closes that gap. It drives the same eight paths over real
// Associations and a real Endpoint, and asserts exactly what the examples
// claim, so a documented path that stops behaving as written fails here rather
// than misleading a reader.

func exampleASKey() ASKey {
	return ASKey{
		NetworkAppearance: 7, NetworkAppearanceSet: true,
		RoutingContext: 1, RoutingContextSet: true,
	}
}

func exampleASPConfig(withRouting bool) *ASPConfig {
	asKey := exampleASKey()
	config := &ASPConfig{
		SignallingGateways: []SignallingGatewayConfig{{
			ID: "sg-a",
			SGPs: []SignallingGatewayProcessConfig{{
				ID: "sgp-a1",
				ApplicationServers: []RemoteASConfig{{
					ID:    "as-core",
					ASKey: &asKey,
				}},
			}},
		}},
	}
	if withRouting {
		config.Routing = &ASPRoutingConfig{
			SignallingGatewaySelection: RouteSelectionPrimaryBackup,
			SignallingGatewayProcessSelection: map[SignallingGatewayID]RouteSelectionMode{
				"sg-a": RouteSelectionPrimaryBackup,
			},
			Paths: []MTPRoutePath{{
				ID:                 "via-sg-a",
				SignallingGateway:  "sg-a",
				ApplicationServers: []RemoteASID{"as-core"},
			}},
			MTPRoutes: []MTPRouteConfig{{
				ID:                   "sccp",
				DestinationPointCode: 0x220000,
				Mask:                 16,
				ServiceIndicators:    []uint8{params.ServiceIndSCCP},
				Paths:                []MTPRoutePathID{"via-sg-a"},
			}},
		}
	}
	return config
}

// TestExampleStartupBuildsTheDocumentedEndpoint is the startup example: the
// configuration it shows is accepted, and the role it prints is the Endpoint's
// own rather than anything about the transport.
func TestExampleStartupBuildsTheDocumentedEndpoint(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: exampleASPConfig(true)})
	if err != nil {
		t.Fatalf("NewEndpoint() error = %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	if got := endpoint.Role(); got != RoleASP {
		t.Errorf("Role() = %v, want %v", got, RoleASP)
	}

	peer := SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	config := NewAssociationConfig().
		EnableHeartbeat(3*time.Second, 10*time.Second).
		SetASPIdentifier(1).
		SetApplicationServers(ASConfig{ASKey: exampleASKey(), TrafficMode: params.TrafficModeLoadshare})
	config.PeerSGP = &peer
	if err := endpoint.validateAssociationConfig(config); err != nil {
		t.Fatalf("the documented AssociationConfig was rejected: %v", err)
	}

	// The example says an association must name a provisioned SGP. An
	// unprovisioned one is refused before any socket work.
	unknown := SGPIdentity{SignallingGateway: "sg-b", SignallingGatewayProcess: "sgp-b1"}
	stray := NewAssociationConfig().
		SetApplicationServers(ASConfig{ASKey: exampleASKey()})
	stray.PeerSGP = &unknown
	if err := endpoint.validateAssociationConfig(stray); !errors.Is(err, ErrUnknownSGP) {
		t.Errorf("an unprovisioned SGP was accepted: %v", err)
	}
}

// TestExampleDirectSendPutsTheRequestOnTheWire is the direct-send example. The
// request the example builds is the whole of the message: the scope, the
// routing label and the stream all come from it, and nothing is completed from
// association-wide state.
func TestExampleDirectSendPutsTheRequestOnTheWire(t *testing.T) {
	association, capture := newDataWriteAssociation(t, 1)

	request := DataRequest{
		AS: exampleASKey(),
		ProtocolData: params.ProtocolDataPayload{
			OriginatingPointCode:    0x111111,
			DestinationPointCode:    0x222222,
			ServiceIndicator:        params.ServiceIndSCCP,
			SignallingLinkSelection: 1,
			Data:                    []byte("payload"),
		},
	}
	written, err := association.WriteData(request)
	if err != nil {
		t.Fatalf("WriteData() error = %v", err)
	}
	if want := len(request.ProtocolData.Data); written != want {
		t.Errorf("WriteData() = %d user octets, want %d", written, want)
	}

	sent := capture.messages(t)
	if len(sent) != 1 {
		t.Fatalf("submitted %d messages, want 1", len(sent))
	}
	data := sent[0]
	if got := data.RoutingContext.RoutingContexts(); len(got) != 1 || got[0] != request.AS.RoutingContext {
		t.Errorf("wire Routing Context = %v, want [%d]", got, request.AS.RoutingContext)
	}
	if got := data.NetworkAppearance.NetworkAppearance(); got != request.AS.NetworkAppearance {
		t.Errorf("wire Network Appearance = %d, want %d", got, request.AS.NetworkAppearance)
	}
	payload, err := data.ProtocolData.ProtocolData()
	if err != nil {
		t.Fatalf("decoding Protocol Data: %v", err)
	}
	if payload.OriginatingPointCode != request.ProtocolData.OriginatingPointCode ||
		payload.DestinationPointCode != request.ProtocolData.DestinationPointCode ||
		payload.SignallingLinkSelection != request.ProtocolData.SignallingLinkSelection {
		t.Errorf("routing label on the wire = %+v, want the request's", payload)
	}

	// The example says a zero Stream asks for the stream this message's own SLS
	// maps to, and that stream 0 is never used for DATA.
	capture.mu.Lock()
	stream := capture.streams[0]
	capture.mu.Unlock()
	if stream == 0 {
		t.Error("DATA was submitted on stream 0, which RFC 4666 Section 1.4.7 forbids")
	}
	if want := association.streamFor(request.ProtocolData.SignallingLinkSelection); stream != want {
		t.Errorf("stream = %d, want the SLS-derived %d", stream, want)
	}
}

// TestExampleDirectSendClassifiesFailureOutcomes is the other half of the
// direct-send example: DataNotSent and DataSendIndeterminate are produced by
// real failures, and the cause stays matchable through the classification.
func TestExampleDirectSendClassifiesFailureOutcomes(t *testing.T) {
	request := DataRequest{
		AS: exampleASKey(),
		ProtocolData: params.ProtocolDataPayload{
			OriginatingPointCode:    0x111111,
			DestinationPointCode:    0x222222,
			ServiceIndicator:        params.ServiceIndSCCP,
			SignallingLinkSelection: 1,
			Data:                    []byte("payload"),
		},
	}

	t.Run("not sent", func(t *testing.T) {
		association, capture := newDataWriteAssociation(t, 1)
		if err := association.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}

		_, err := association.WriteData(request)
		var writeErr *DataWriteError
		if !errors.As(err, &writeErr) {
			t.Fatalf("WriteData() on a closed association returned %T, want *DataWriteError", err)
		}
		if writeErr.Outcome != DataNotSent {
			t.Errorf("Outcome = %v, want %v", writeErr.Outcome, DataNotSent)
		}
		if !errors.Is(err, ErrNotEstablished) {
			t.Errorf("the cause is not reachable with errors.Is: %v", err)
		}
		if writeErr.AS != request.AS {
			t.Errorf("AS = %+v, want the request's %+v", writeErr.AS, request.AS)
		}
		if got := capture.submissions(); got != 0 {
			t.Errorf("a DataNotSent failure submitted %d messages; nothing may reach the transport", got)
		}
	})

	t.Run("indeterminate", func(t *testing.T) {
		association, capture := newDataWriteAssociation(t, 1)
		// A short submission: the transport has seen the message, so its fate
		// is unknown and a resend may duplicate.
		capture.short = 1

		_, err := association.WriteData(request)
		var writeErr *DataWriteError
		if !errors.As(err, &writeErr) {
			t.Fatalf("WriteData() returned %T, want *DataWriteError", err)
		}
		if writeErr.Outcome != DataSendIndeterminate {
			t.Errorf("Outcome = %v, want %v", writeErr.Outcome, DataSendIndeterminate)
		}
		if !errors.Is(err, ErrPartialDataWrite) {
			t.Errorf("the cause is not reachable with errors.Is: %v", err)
		}
	})
}

// TestExampleInboundClassificationSeparatesWireScopeFromIdentity is the
// inbound-classification example, driven by real deliveries. The wire scope,
// the resolved Application Server, the arrival stream and the epoch are four
// separate answers, and the example's switch depends on all four.
func TestExampleInboundClassificationSeparatesWireScopeFromIdentity(t *testing.T) {
	t.Run("routing context present", func(t *testing.T) {
		association, _ := newDataWriteAssociation(t, 1)
		deliver(t, association, 3, inboundData(1, "scoped"))

		message, err := readPayload(association, time.Second)
		if err != nil {
			t.Fatalf("ReadData() error = %v", err)
		}
		if !message.Scope.RoutingContextSet {
			t.Fatal("RoutingContextSet is false for a message that carried one")
		}
		if got := message.Scope.RoutingContexts; len(got) != 1 || got[0] != 1 {
			t.Errorf("wire Routing Contexts = %v, want [1]", got)
		}
		if !message.Scope.NetworkAppearanceSet || message.Scope.NetworkAppearance != 7 {
			t.Errorf("wire Network Appearance = %d/%t, want 7/true",
				message.Scope.NetworkAppearance, message.Scope.NetworkAppearanceSet)
		}
		if want := exampleASKey(); message.AS != want {
			t.Errorf("resolved AS = %+v, want %+v", message.AS, want)
		}
		if message.Stream != 3 {
			t.Errorf("Stream = %d, want the arrival stream 3", message.Stream)
		}
		if message.Epoch != association.Epoch() {
			t.Errorf("Epoch = %d, want the association's %d", message.Epoch, association.Epoch())
		}
	})

	t.Run("contextless application server", func(t *testing.T) {
		// An association that coordinates no Routing Context at all: the peer
		// omits the parameter and the message resolves to the single
		// contextless Application Server.
		association, _ := newDataWriteAssociation(t)
		contextless := messages.NewData(
			nil, nil,
			params.NewProtocolData(0x111111, 0x222222, params.ServiceIndSCCP, 0, 0, 1, []byte("bare")),
			nil,
		)
		deliver(t, association, 2, contextless)

		message, err := readPayload(association, time.Second)
		if err != nil {
			t.Fatalf("ReadData() error = %v", err)
		}
		if message.Scope.RoutingContextSet {
			t.Error("RoutingContextSet is true for a message that carried no Routing Context")
		}
		if message.AS.RoutingContextSet {
			t.Errorf("resolved AS = %+v, want the contextless Application Server", message.AS)
		}
		if message.Stream != 2 {
			t.Errorf("Stream = %d, want the arrival stream 2", message.Stream)
		}
	})
}

// TestExampleSSNMSubscriptionIsAtomicWithItsSnapshot is the SSNM-consumption
// example. The snapshot and the subscription are taken together, Resync
// reports the same view, and a cancelled Next ends that one call rather than
// the subscription.
func TestExampleSSNMSubscriptionIsAtomicWithItsSnapshot(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: exampleASPConfig(false)})
	if err != nil {
		t.Fatalf("NewEndpoint() error = %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	snapshot, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("SubscribeSSNM() error = %v", err)
	}
	t.Cleanup(func() { _ = subscription.Close() })

	if got := endpoint.SSNMKnowledge().Revision; got != snapshot.Revision {
		t.Errorf("SSNMKnowledge revision = %d, subscribe snapshot = %d", got, snapshot.Revision)
	}

	resynced, err := subscription.Resync()
	if err != nil {
		t.Fatalf("Resync() error = %v", err)
	}
	if resynced.Revision != snapshot.Revision {
		t.Errorf("Resync revision = %d, want %d", resynced.Revision, snapshot.Revision)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := subscription.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Next() = %v, want context.Canceled", err)
	}
	// The subscription survives its caller's cancellation.
	if _, err := subscription.Resync(); err != nil {
		t.Errorf("Resync() after a cancelled Next: %v", err)
	}

	// Closing ends the stream for good, and says so rather than blocking.
	if err := subscription.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := subscription.Next(context.Background()); !errors.Is(err, ErrSSNMSubscriptionClosed) {
		t.Errorf("Next() after Close = %v, want ErrSSNMSubscriptionClosed", err)
	}
}

// TestExampleSSNMOverflowIsReportedAndRecoveredByResync drives the overflow
// branch of the SSNM example for real: a consumer that stops reading is told
// that its stream is no longer a complete history, and Resync is what replaces
// the view rather than patching it.
//
// The store refuses rather than evicting, so the hole is announced instead of
// being handed over as though nothing had happened.
func TestExampleSSNMOverflowIsReportedAndRecoveredByResync(t *testing.T) {
	const queue = 2
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(),
		&SSNMStateConfig{SubscriptionQueueSize: queue})
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)

	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("SubscribeSSNM() error = %v", err)
	}
	t.Cleanup(func() { _ = subscription.Close() })

	// More reports than the consumer's queue can hold, with nothing reading.
	const reports = queue + 4
	for index := range reports {
		sendDUNA(t, association, 7, 1, 0x300000+uint32(index))
	}

	// The example's loop reads until ContinuityLost, then resyncs. What was
	// queued before the overflow is still delivered, and the marker follows it
	// exactly once.
	var delivered int
	for {
		event, err := drainSSNMEvent(t, subscription)
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
		if event.ContinuityLost {
			if event.Kind != SSNMContinuityLostEvent {
				t.Errorf("ContinuityLost carried by %v, want %v", event.Kind, SSNMContinuityLostEvent)
			}
			break
		}
		delivered++
		if delivered > queue {
			t.Fatalf("a queue of %d delivered %d events before reporting loss", queue, delivered)
		}
	}

	// Resync replaces the view, and it holds every report the stream lost.
	resynced, err := subscription.Resync()
	if err != nil {
		t.Fatalf("Resync() error = %v", err)
	}
	var destinations int
	for _, partition := range resynced.Partitions {
		destinations += len(partition.Destinations)
	}
	if destinations != reports {
		t.Errorf("Resync holds %d destinations, want the %d reported", destinations, reports)
	}

	// The stream resumes from the snapshot, at a strictly greater revision.
	sendDUNA(t, association, 7, 1, 0x3000FF)
	event, err := drainSSNMEvent(t, subscription)
	if err != nil {
		t.Fatalf("Next() after Resync error = %v", err)
	}
	if event.ContinuityLost {
		t.Error("continuity loss was reported again after a successful Resync")
	}
	if event.Revision <= resynced.Revision {
		t.Errorf("event revision %d is not greater than the resync revision %d",
			event.Revision, resynced.Revision)
	}
}

// TestExampleApplicationManagedRoutingNeedsNoRouteInventory is the
// application-owned discovery example: with ASPConfig.Routing nil the Endpoint
// still provisions peers and retains SSNM knowledge, the library-managed
// transfer says plainly that it is not configured, and no dummy route has to be
// invented to reach that state.
func TestExampleApplicationManagedRoutingNeedsNoRouteInventory(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: exampleASPConfig(false)})
	if err != nil {
		t.Fatalf("NewEndpoint() error = %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	_, err = endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: params.NewProtocolDataPayload(
			0x111111, 0x220001, params.ServiceIndSCCP, 0, 0, 1, []byte("payload"),
		),
	})
	if !errors.Is(err, ErrRoutingNotConfigured) {
		t.Errorf("MTPTransfer() without a route inventory = %v, want ErrRoutingNotConfigured", err)
	}

	// Discovery is still there.
	if _, _, err := endpoint.SubscribeSSNM(); err != nil {
		t.Errorf("SubscribeSSNM() on an application-managed ASP: %v", err)
	}
	// And the derived indication stream exists but has nothing to derive from.
	select {
	case indication := <-endpoint.MTPIndications():
		t.Errorf("an application-managed ASP published %+v", indication)
	default:
	}

	// Direct sending is unaffected: an Association of such an Endpoint carries
	// DATA with no route inventory anywhere.
	association, capture := newDataWriteAssociation(t, 1)
	if _, err := association.WriteData(DataRequest{
		AS:           exampleASKey(),
		ProtocolData: testProtocolData([]byte("payload")),
	}); err != nil {
		t.Fatalf("WriteData() on an application-managed ASP: %v", err)
	}
	if got := capture.submissions(); got != 1 {
		t.Errorf("submitted %d messages, want 1", got)
	}
}

// TestMTPIndicationsNilnessMatchesItsDocumentation pins the rule the GoDoc and
// the README both state, because prose cannot be checked and this claim has
// been wrong twice: first as "nil for an ASP that left Routing nil", then as
// "non-nil for an ASP with an ASPConfig".
//
// It is neither. Every ASP Endpoint has the channel, an ASPConfig or not; no
// SGP or IPSP Endpoint has one. What varies is whether anything is ever put on
// it, which is a different question and is covered above.
func TestMTPIndicationsNilnessMatchesItsDocumentation(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		config EndpointConfig
		wantCh bool
	}{
		{"ASP with routes", EndpointConfig{Role: RoleASP, ASP: exampleASPConfig(true)}, true},
		{"ASP without routes", EndpointConfig{Role: RoleASP, ASP: exampleASPConfig(false)}, true},
		{"ASP without an ASPConfig", EndpointConfig{Role: RoleASP}, true},
		{"SGP", EndpointConfig{Role: RoleSGP}, false},
		{"IPSP", EndpointConfig{Role: RoleIPSP}, false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			endpoint, err := NewEndpoint(testCase.config)
			if err != nil {
				t.Fatalf("NewEndpoint() error = %v", err)
			}
			indications := endpoint.MTPIndications()
			if got := indications != nil; got != testCase.wantCh {
				t.Fatalf("MTPIndications() non-nil = %t, want %t", got, testCase.wantCh)
			}
			if !testCase.wantCh {
				_ = endpoint.Close()
				return
			}

			// Open and empty: a receive finds nothing while the Endpoint runs.
			select {
			case indication, open := <-indications:
				t.Fatalf("received %+v (channel open: %t) from an idle Endpoint", indication, open)
			default:
			}

			// Closed by Endpoint.Close, which is what ends a range over it.
			if err := endpoint.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			select {
			case _, open := <-indications:
				if open {
					t.Error("MTPIndications delivered a value after Endpoint.Close")
				}
			case <-time.After(time.Second):
				t.Error("MTPIndications was not closed by Endpoint.Close")
			}
		})
	}
}

// TestGracefulWithdrawalNeedsALiveAssociationContext pins the contract the ASP
// example depends on, and the defect it used to have.
//
// The association's monitor selects on the context given to Dial and closes the
// association as soon as it is done. An application that hands Dial its signal
// context and then calls ShutdownContext on the way out gets no withdrawal at
// all: the association is already ASP-DOWN, so neither ASP Inactive nor ASP
// Down is sent. RFC 4666 Section 4.9 option (a) becomes option (b).
// ShutdownContext used to return nil for it, as if the withdrawal had
// succeeded; it now returns the cancellation that ended the association.
func TestGracefulWithdrawalNeedsALiveAssociationContext(t *testing.T) {
	t.Run("after the lifetime context ended", func(t *testing.T) {
		association, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
		association.noteRoutingContextsAcked(params.NewRoutingContext(1))
		signalled, cancel := context.WithCancel(context.Background())
		cancel()

		// What the monitor does with a cancelled lifetime context.
		_ = association.closeWith(signalled.Err())
		before := len(*sent)

		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), time.Second)
		defer cancelShutdown()
		// Nothing is sent, and the error says why: the association had already
		// ended, and an application that only checks the error is not told the
		// withdrawal succeeded.
		if err := association.ShutdownContext(shutdownCtx); !errors.Is(err, context.Canceled) {
			t.Fatalf("ShutdownContext() error = %v, want the cancellation that ended the association", err)
		}
		if got := (*sent)[before:]; len(got) != 0 {
			t.Errorf("a withdrawal on a closed association sent %v, want nothing", got)
		}
		if err := association.Err(); !errors.Is(err, context.Canceled) {
			t.Errorf("Err() = %v, want the cancellation that closed it", err)
		}
		if state := association.State(); state != StateASPDown {
			t.Errorf("State() = %v, want %v", state, StateASPDown)
		}
	})

	t.Run("while the lifetime context is live", func(t *testing.T) {
		// The example's shape: the signal ends the traffic loop, the
		// association's own context is still live, and the withdrawal is
		// written before anything is cancelled.
		association, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
		association.noteRoutingContextsAcked(params.NewRoutingContext(1))
		if state := association.State(); state != StateASPActive {
			t.Fatalf("State() = %v, want %v", state, StateASPActive)
		}

		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancelShutdown()
		// No peer answers the T(ack), so this reports the expiry rather than
		// succeeding. What matters is that the withdrawal reached the wire at
		// all, which is exactly what the closed association above could not do.
		_ = association.ShutdownContext(shutdownCtx)

		var withdrew bool
		for _, message := range *sent {
			if message.MessageClass() == messages.MsgClassASPTM &&
				message.MessageType() == messages.MsgTypeAspInactive {
				withdrew = true
			}
		}
		if !withdrew {
			t.Errorf("a withdrawal on a live association sent %v, want an ASP Inactive", *sent)
		}
	})
}

// TestExampleAlternatePathRecoveryReportsEveryRefusal is the alternate-path
// example. Selection reports each candidate and its reason in the route's own
// candidate order, and Unwrap separates "nothing is known about the
// destination" from "this route cannot be used".
func TestExampleAlternatePathRecoveryReportsEveryRefusal(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: exampleASPConfig(true)})
	if err != nil {
		t.Fatalf("NewEndpoint() error = %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	_, err = endpoint.MTPTransfer(MTPTransferRequest{
		MTPRoute: "sccp",
		ProtocolData: params.NewProtocolDataPayload(
			0x111111, 0x220001, params.ServiceIndSCCP, 0, 0, 1, []byte("payload"),
		),
	})

	var selection *MTPSelectionError
	if !errors.As(err, &selection) {
		t.Fatalf("MTPTransfer() with no established association returned %T, want *MTPSelectionError", err)
	}
	if selection.MTPRoute != "sccp" {
		t.Errorf("MTPRoute = %q, want %q", selection.MTPRoute, "sccp")
	}
	if len(selection.Rejections) != 1 {
		t.Fatalf("Rejections = %d, want the route's one provisioned candidate", len(selection.Rejections))
	}
	rejection := selection.Rejections[0]
	if rejection.Path != "via-sg-a" || rejection.ApplicationServer != "as-core" ||
		rejection.SGP.SignallingGatewayProcess != "sgp-a1" {
		t.Errorf("rejection = %+v, want the provisioned candidate", rejection)
	}
	if rejection.Reason != MTPCandidateNotBound {
		t.Errorf("Reason = %v, want %v", rejection.Reason, MTPCandidateNotBound)
	}
	if got := rejection.Reason.String(); got != "not-bound" {
		t.Errorf("Reason.String() = %q, want %q", got, "not-bound")
	}

	// Nothing is bound, so this is a route that cannot be used rather than a
	// destination nobody has reported on.
	if errors.Is(err, ErrDestinationStateUnknown) {
		t.Error("a not-bound candidate was reported as an unknown destination")
	}
	if !errors.Is(err, ErrNoMTPRoute) {
		t.Errorf("errors.Is(err, ErrNoMTPRoute) = false for %v", err)
	}

	// An unroutable label is a different failure again, and is named as one.
	_, err = endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: params.NewProtocolDataPayload(
			0x111111, 0x330001, params.ServiceIndSCCP, 0, 0, 1, []byte("payload"),
		),
	})
	if !errors.Is(err, ErrNoMatchingMTPRoute) {
		t.Errorf("MTPTransfer() outside every route = %v, want ErrNoMatchingMTPRoute", err)
	}
}

// TestExampleShutdownClosesOnlyItsOwnScope is the shutdown example, and the
// lifecycle contract the documentation states: an Association closes itself,
// and the Endpoint closes everything it owns, idempotently, after which the
// operations that would start work say so.
func TestExampleShutdownClosesOnlyItsOwnScope(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: exampleASPConfig(true)})
	if err != nil {
		t.Fatalf("NewEndpoint() error = %v", err)
	}

	if err := endpoint.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := endpoint.Close(); err != nil {
		t.Errorf("second Close() = %v, want the same nil result", err)
	}

	if _, _, err := endpoint.SubscribeSSNM(); !errors.Is(err, ErrEndpointClosed) {
		t.Errorf("SubscribeSSNM() after Close = %v, want ErrEndpointClosed", err)
	}
	if _, err := endpoint.MTPTransfer(MTPTransferRequest{
		MTPRoute: "sccp",
		ProtocolData: params.NewProtocolDataPayload(
			0x111111, 0x220001, params.ServiceIndSCCP, 0, 0, 1, []byte("payload"),
		),
	}); !errors.Is(err, ErrEndpointClosed) {
		t.Errorf("MTPTransfer() after Close = %v, want ErrEndpointClosed", err)
	}
	if got := len(endpoint.AssociationStatuses()); got != 0 {
		t.Errorf("AssociationStatuses() after Close = %d, want 0", got)
	}

	// An Association closes only itself: it reports the close as its own cause
	// and nothing else is asked to stop.
	association, _ := newDataWriteAssociation(t, 1)
	if err := association.Close(); err != nil {
		t.Fatalf("Association.Close() error = %v", err)
	}
	select {
	case <-association.Done():
	default:
		t.Fatal("Done() is not closed after Close()")
	}
	if err := association.Err(); !errors.Is(err, ErrAssociationClosed) {
		t.Errorf("Err() = %v, want ErrAssociationClosed", err)
	}
	if err := association.Close(); err != nil {
		t.Errorf("second Association.Close() = %v, want nil", err)
	}

	// Close is RFC 4666 Section 4.9 option (b), so a Shutdown afterwards has no
	// procedures left to run, and reports the Close that ended the association.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := association.ShutdownContext(shutdownCtx); !errors.Is(err, ErrAssociationClosed) {
		t.Errorf("ShutdownContext() after Close = %v, want ErrAssociationClosed", err)
	}
}
