// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// dataFrameCapture stands in for the SCTP transport on the direct DATA send
// path, recording the octets and the stream each message was submitted with.
type dataFrameCapture struct {
	mu      sync.Mutex
	frames  [][]byte
	streams []uint16
	calls   int

	// err is reported instead of accepting the message; short reports a byte
	// count lower than the frame without an error.
	err   error
	short int
}

func (c *dataFrameCapture) write(data []byte, info *sctp.SndRcvInfo) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.err != nil {
		return 0, c.err
	}
	c.frames = append(c.frames, append([]byte(nil), data...))
	c.streams = append(c.streams, info.Stream)
	if c.short > 0 {
		return c.short, nil
	}
	return len(data), nil
}

func (c *dataFrameCapture) submissions() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *dataFrameCapture) messages(t *testing.T) []*messages.Data {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	decoded := make([]*messages.Data, 0, len(c.frames))
	for index, frame := range c.frames {
		message, err := messages.Parse(frame)
		if err != nil {
			t.Fatalf("frame %d did not decode: %v", index, err)
		}
		data, ok := message.(*messages.Data)
		if !ok {
			t.Fatalf("frame %d decoded as %T, want *messages.Data", index, message)
		}
		decoded = append(decoded, data)
	}
	return decoded
}

// newDataWriteAssociation is an ASP-ACTIVE ASP association carrying the named
// Routing Contexts in Network Appearance 7, with the transport replaced by a
// capture. It owns no route inventory of any kind.
func newDataWriteAssociation(t *testing.T, routingContexts ...uint32) (*Association, *dataFrameCapture) {
	t.Helper()
	conn, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, routingContexts...)
	setInventoryNetworkAppearance(&conn.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	conn.noteRoutingContextsAcked(params.NewRoutingContext(routingContexts...))
	capture := &dataFrameCapture{}
	conn.dataWriter = capture.write
	return conn, capture
}

func writeScope(routingContext uint32) ASKey {
	return ASKey{
		NetworkAppearance:    7,
		NetworkAppearanceSet: true,
		RoutingContext:       routingContext,
		RoutingContextSet:    true,
	}
}

// Acceptance bullet 1: concurrent messages with varied OPC, DPC, SI, NI,
// priority, SLS and scope, no shared configuration mutation, and no route
// inventory anywhere.
//
// RFC 4666 Section 3.3.1 puts the whole MTP3 routing label in the Protocol Data
// of each message and makes the Routing Context "the traffic flow" that message
// belongs to, so neither may be taken from association-wide state that another
// goroutine can change between this goroutine's choice and its write.
func TestConcurrentWriteDataCarriesEachMessagesOwnLabelAndScope(t *testing.T) {
	conn, capture := newDataWriteAssociation(t, 1, 2, 3)

	// The configuration is shared by every association a Listener accepts, so a
	// send that writes to it corrupts the others. Pinned by value here.
	inventoryBefore := fmt.Sprint(conn.cfg.ApplicationServers)

	const writers, perWriter = 8, 64
	type sent struct {
		request DataRequest
		octets  int
	}
	results := make([][]sent, writers)
	var wait sync.WaitGroup
	for writer := 0; writer < writers; writer++ {
		wait.Add(1)
		go func(writer int) {
			defer wait.Done()
			results[writer] = make([]sent, 0, perWriter)
			for index := 0; index < perWriter; index++ {
				message := writer*perWriter + index
				request := DataRequest{
					AS: writeScope(uint32(message%3) + 1),
					ProtocolData: params.ProtocolDataPayload{
						OriginatingPointCode:    uint32(0x110000 + message),
						DestinationPointCode:    uint32(0x220000 + message),
						ServiceIndicator:        uint8(message%2)*2 + params.ServiceIndSCCP,
						NetworkIndicator:        uint8(message % 4),
						MessagePriority:         uint8((message / 4) % 4),
						SignallingLinkSelection: uint8(message % 16),
						Data:                    []byte(fmt.Sprintf("message-%04d", message)),
					},
				}
				octets, err := conn.WriteData(request)
				if err != nil {
					t.Errorf("WriteData(message %d): %v", message, err)
					return
				}
				results[writer] = append(results[writer], sent{request: request, octets: octets})
			}
		}(writer)
	}
	wait.Wait()

	decoded := capture.messages(t)
	if len(decoded) != writers*perWriter {
		t.Fatalf("%d messages reached the transport, want %d", len(decoded), writers*perWriter)
	}

	// Index the wire by payload, which identifies the request that produced it.
	onWire := make(map[string]*messages.Data, len(decoded))
	streams := make(map[string]uint16, len(decoded))
	capture.mu.Lock()
	for index, data := range decoded {
		payload, err := data.ProtocolData.ProtocolData()
		if err != nil {
			t.Fatalf("message %d carried undecodable Protocol Data: %v", index, err)
		}
		key := string(payload.Data)
		if _, duplicate := onWire[key]; duplicate {
			t.Fatalf("payload %q was sent twice", key)
		}
		onWire[key] = data
		streams[key] = capture.streams[index]
	}
	capture.mu.Unlock()

	for _, writerResults := range results {
		for _, result := range writerResults {
			key := string(result.request.ProtocolData.Data)
			data, ok := onWire[key]
			if !ok {
				t.Fatalf("payload %q never reached the transport", key)
			}
			if result.octets != len(result.request.ProtocolData.Data) {
				t.Errorf("WriteData(%q) = %d octets, want %d",
					key, result.octets, len(result.request.ProtocolData.Data))
			}
			payload, err := data.ProtocolData.ProtocolData()
			if err != nil {
				t.Fatalf("Protocol Data of %q: %v", key, err)
			}
			want := result.request.ProtocolData
			if payload.OriginatingPointCode != want.OriginatingPointCode ||
				payload.DestinationPointCode != want.DestinationPointCode ||
				payload.ServiceIndicator != want.ServiceIndicator ||
				payload.NetworkIndicator != want.NetworkIndicator ||
				payload.MessagePriority != want.MessagePriority ||
				payload.SignallingLinkSelection != want.SignallingLinkSelection {
				t.Errorf("payload %q went out with routing label %s, want %s",
					key, payload.String(), want.String())
			}
			if data.RoutingContext == nil {
				t.Fatalf("payload %q went out with no Routing Context", key)
			}
			if got := data.RoutingContext.RoutingContexts(); len(got) != 1 ||
				got[0] != result.request.AS.RoutingContext {
				t.Errorf("payload %q named Routing Context %v, want [%d]",
					key, got, result.request.AS.RoutingContext)
			}
			if data.NetworkAppearance == nil || data.NetworkAppearance.NetworkAppearance() != 7 {
				t.Errorf("payload %q went out with Network Appearance %v, want 7",
					key, data.NetworkAppearance)
			}
			wantStream := conn.streamFor(want.SignallingLinkSelection)
			if streams[key] != wantStream {
				t.Errorf("payload %q went out on stream %d, want %d (its own SLS %d)",
					key, streams[key], wantStream, want.SignallingLinkSelection)
			}
			if streams[key] == 0 {
				t.Errorf("payload %q went out on stream 0, which RFC 4666 Section 1.4.7 rule 1 forbids", key)
			}
		}
	}

	if got := fmt.Sprint(conn.cfg.ApplicationServers); got != inventoryBefore {
		t.Errorf("the shared Application Server inventory changed from %s to %s", inventoryBefore, got)
	}
}

// requireDataWriteError pins the two things every failed send must report: the
// outcome the caller has to act on, and the cause it was derived from.
func requireDataWriteError(t *testing.T, err error, outcome DataSendOutcome, cause error) {
	t.Helper()
	if err == nil {
		t.Fatalf("WriteData succeeded, want %v (%v)", cause, outcome)
	}
	var writeError *DataWriteError
	if !errors.As(err, &writeError) {
		t.Fatalf("WriteData error %v (%T) is not a *DataWriteError", err, err)
	}
	if writeError.Outcome != outcome {
		t.Errorf("outcome = %v, want %v (error %v)", writeError.Outcome, outcome, err)
	}
	if cause != nil && !errors.Is(err, cause) {
		t.Errorf("error %v does not report cause %v", err, cause)
	}
	if writeError.Unwrap() == nil {
		t.Errorf("error %v discarded its cause", err)
	}
}

func simpleProtocolData(payload string) params.ProtocolDataPayload {
	return params.ProtocolDataPayload{
		OriginatingPointCode:    0x111111,
		DestinationPointCode:    0x222222,
		ServiceIndicator:        params.ServiceIndSCCP,
		SignallingLinkSelection: 1,
		Data:                    []byte(payload),
	}
}

// Acceptance bullet 2: the scope a message names is matched exactly. A Routing
// Context or Network Appearance that is omitted is not the same as one that is
// explicitly zero, and neither is completed from association-wide state.
//
// RFC 4666 Section 3.3.1 makes the Routing Context Conditional — "Where a
// Routing Key has not been coordinated between the SGP and ASP, sending of
// Routing Context is not required" — and mandatory where several are shared:
// "Where multiple Routing Keys and Routing Contexts are used across a common
// association, the Routing Context MUST be sent to identify the traffic flow".
// Section 3.6.1 makes the Network Appearance part of Routing Key identity, so
// the pair is the Application Server's identity and has to match one.
func TestWriteDataRequiresAnExactApplicationServerScope(t *testing.T) {
	for _, test := range []struct {
		name          string
		appearance    *params.Param
		contexts      []uint32
		scope         ASKey
		wantCause     error
		wantContext   uint32
		wantNoContext bool
	}{
		{
			name:       "exact scope is accepted",
			appearance: params.NewNetworkAppearance(7),
			contexts:   []uint32{1, 2},
			scope:      ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 2, RoutingContextSet: true},

			wantContext: 2,
		},
		{
			name:       "an omitted Routing Context is not completed from configuration",
			appearance: params.NewNetworkAppearance(7),
			contexts:   []uint32{1},
			scope:      ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true},
			wantCause:  ErrMissingRoutingContext,
		},
		{
			name:        "an explicitly zero Routing Context that is configured is carried",
			appearance:  params.NewNetworkAppearance(7),
			contexts:    []uint32{0, 1},
			scope:       ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true},
			wantContext: 0,
		},
		{
			name:       "an explicitly zero Routing Context that is not configured is refused",
			appearance: params.NewNetworkAppearance(7),
			contexts:   []uint32{1, 2},
			scope:      ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true},
			wantCause:  ErrInvalidRoutingContext,
		},
		{
			name:       "an unconfigured Routing Context is refused",
			appearance: params.NewNetworkAppearance(7),
			contexts:   []uint32{1},
			scope:      ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 9, RoutingContextSet: true},
			wantCause:  ErrInvalidRoutingContext,
		},
		{
			name:       "an omitted Network Appearance is not completed from configuration",
			appearance: params.NewNetworkAppearance(7),
			contexts:   []uint32{1},
			scope:      ASKey{RoutingContext: 1, RoutingContextSet: true},
			wantCause:  ErrUnknownApplicationServerScope,
		},
		{
			name:       "an explicitly zero Network Appearance is not an omitted one",
			appearance: nil,
			contexts:   []uint32{1},
			scope:      ASKey{NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true},
			// Naming an appearance this association does not have is the
			// Section 3.8.1 "Invalid Network Appearance" condition, and the
			// offending value is kept with the error.
			wantCause: ErrInvalidNetworkAppearance,
		},
		{
			name:       "an unset Network Appearance is omitted whatever its value field holds",
			appearance: nil,
			contexts:   []uint32{1},
			// The presence flag is what the wire and the binding follow, so a
			// value left behind in an unset field names nothing.
			scope: ASKey{NetworkAppearance: 5, RoutingContext: 1, RoutingContextSet: true},

			wantContext: 1,
		},
		{
			name:       "a different Network Appearance is refused",
			appearance: params.NewNetworkAppearance(7),
			contexts:   []uint32{1},
			scope:      ASKey{NetworkAppearance: 8, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true},
			wantCause:  ErrInvalidNetworkAppearance,
		},
		{
			name:          "a contextless Application Server carries no Routing Context",
			appearance:    params.NewNetworkAppearance(7),
			contexts:      nil,
			scope:         ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true},
			wantNoContext: true,
		},
		{
			name:       "a contextless Application Server refuses a named Routing Context",
			appearance: params.NewNetworkAppearance(7),
			contexts:   nil,
			scope:      ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true},
			wantCause:  ErrInvalidRoutingContext,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, test.contexts...)
			setInventoryNetworkAppearance(&conn.cfg.ApplicationServers, test.appearance)
			capture := &dataFrameCapture{}
			conn.dataWriter = capture.write
			if len(test.contexts) == 0 {
				conn.noteRoutingContextsAcked(nil)
			} else {
				conn.noteRoutingContextsAcked(params.NewRoutingContext(test.contexts...))
			}

			octets, err := conn.WriteData(DataRequest{
				AS:           test.scope,
				ProtocolData: simpleProtocolData("scope"),
			})
			if test.wantCause != nil {
				requireDataWriteError(t, err, DataNotSent, test.wantCause)
				if capture.submissions() != 0 {
					t.Errorf("a refused scope still reached the transport %d times", capture.submissions())
				}
				return
			}
			if err != nil {
				t.Fatalf("WriteData: %v", err)
			}
			if octets != len("scope") {
				t.Errorf("WriteData = %d, want %d user octets", octets, len("scope"))
			}
			sent := capture.messages(t)
			if len(sent) != 1 {
				t.Fatalf("%d messages reached the transport, want 1", len(sent))
			}
			switch {
			case test.wantNoContext:
				if sent[0].RoutingContext != nil {
					t.Errorf("DATA carried Routing Context %v, want none",
						sent[0].RoutingContext.RoutingContexts())
				}
			default:
				if sent[0].RoutingContext == nil {
					t.Fatal("DATA carried no Routing Context")
				}
				if got := sent[0].RoutingContext.RoutingContexts(); len(got) != 1 || got[0] != test.wantContext {
					t.Errorf("DATA named Routing Context %v, want [%d]", got, test.wantContext)
				}
			}
			if test.appearance == nil {
				if sent[0].NetworkAppearance != nil {
					t.Errorf("DATA carried Network Appearance %v, want none", sent[0].NetworkAppearance)
				}
			} else if sent[0].NetworkAppearance == nil ||
				sent[0].NetworkAppearance.NetworkAppearance() != test.appearance.NetworkAppearance() {
				t.Errorf("DATA carried Network Appearance %v, want %v",
					sent[0].NetworkAppearance, test.appearance)
			}
		})
	}
}

// A scope that is configured but not yet carrying traffic is refused: RFC 4666
// Section 4.3.4.3 makes activation per Routing Context, so a configured context
// is not an active one.
func TestWriteDataRefusesAScopeThatIsNotActive(t *testing.T) {
	conn, capture := newDataWriteAssociation(t, 1, 2)
	// Only context 1 was acknowledged by the SGP.
	conn.noteNoRoutingContextsAcked()
	conn.noteRoutingContextsAcked(params.NewRoutingContext(1))

	_, err := conn.WriteData(DataRequest{
		AS:           writeScope(2),
		ProtocolData: simpleProtocolData("inactive"),
	})
	requireDataWriteError(t, err, DataNotSent, ErrRoutingContextNotActive)
	if capture.submissions() != 0 {
		t.Errorf("an inactive scope reached the transport %d times", capture.submissions())
	}
	if _, err := conn.WriteData(DataRequest{
		AS:           writeScope(1),
		ProtocolData: simpleProtocolData("active"),
	}); err != nil {
		t.Fatalf("WriteData on the active scope: %v", err)
	}
}

// Every state other than ASP-ACTIVE refuses DATA before anything else is
// considered. RFC 4666 Section 4.3.1 defines ASP-ACTIVE as the state in which
// "application traffic is active (for a particular Routing Context or set of
// Routing Contexts)", and Section 3.8.1 states the receiving end of the same
// rule: "silent discard is used by an ASP if it received a DATA message from an
// SGP while it was in the ASP-INACTIVE state".
func TestWriteDataRefusesWhileTheAssociationIsNotActive(t *testing.T) {
	for _, state := range []State{StateASPDown, StateASPInactive} {
		t.Run(state.String(), func(t *testing.T) {
			conn, capture := newDataWriteAssociation(t, 1)
			conn.setState(state)
			_, err := conn.WriteData(DataRequest{
				AS:           writeScope(1),
				ProtocolData: simpleProtocolData("too early"),
			})
			requireDataWriteError(t, err, DataNotSent, ErrNotEstablished)
			if capture.submissions() != 0 {
				t.Errorf("DATA in %v reached the transport", state)
			}
		})
	}
}

// Acceptance bullet 2, stream constraints. RFC 4666 Section 1.4.7 rule 1:
// "The DATA message MUST NOT be sent on stream 0", and the same section limits
// selection to "the maximum number of streams supported by the underlying SCTP
// association".
func TestWriteDataValidatesTheRequestedStream(t *testing.T) {
	t.Run("a zero request selects the stream of the message's own SLS", func(t *testing.T) {
		conn, capture := newDataWriteAssociation(t, 1)
		for sls := 0; sls < 32; sls++ {
			payload := simpleProtocolData(fmt.Sprintf("sls-%d", sls))
			payload.SignallingLinkSelection = uint8(sls)
			if _, err := conn.WriteData(DataRequest{AS: writeScope(1), ProtocolData: payload}); err != nil {
				t.Fatalf("WriteData(SLS %d): %v", sls, err)
			}
			capture.mu.Lock()
			got := capture.streams[len(capture.streams)-1]
			capture.mu.Unlock()
			if want := conn.streamFor(uint8(sls)); got != want {
				t.Errorf("SLS %d went out on stream %d, want %d", sls, got, want)
			}
			if got == 0 {
				t.Fatalf("SLS %d selected stream 0", sls)
			}
		}
	})

	t.Run("an explicit stream is used as given", func(t *testing.T) {
		conn, capture := newDataWriteAssociation(t, 1)
		for stream := uint16(1); stream <= conn.MaxMessageStreamID(); stream++ {
			if _, err := conn.WriteData(DataRequest{
				AS:           writeScope(1),
				ProtocolData: simpleProtocolData("explicit"),
				Stream:       stream,
			}); err != nil {
				t.Fatalf("WriteData(stream %d): %v", stream, err)
			}
			capture.mu.Lock()
			got := capture.streams[len(capture.streams)-1]
			capture.mu.Unlock()
			if got != stream {
				t.Errorf("explicit stream %d went out on stream %d", stream, got)
			}
		}
	})

	t.Run("a stream above the negotiated maximum is refused", func(t *testing.T) {
		conn, capture := newDataWriteAssociation(t, 1)
		_, err := conn.WriteData(DataRequest{
			AS:           writeScope(1),
			ProtocolData: simpleProtocolData("too high"),
			Stream:       conn.MaxMessageStreamID() + 1,
		})
		requireDataWriteError(t, err, DataNotSent, nil)
		var streamError *InvalidSCTPStreamIDError
		if !errors.As(err, &streamError) {
			t.Fatalf("error %v is not an *InvalidSCTPStreamIDError", err)
		}
		if capture.submissions() != 0 {
			t.Errorf("an invalid stream reached the transport")
		}
	})

	t.Run("an association with no data stream refuses DATA", func(t *testing.T) {
		conn, capture := newDataWriteAssociation(t, 1)
		conn.maxMessageStreamID = 0
		_, err := conn.WriteData(DataRequest{
			AS:           writeScope(1),
			ProtocolData: simpleProtocolData("nowhere to go"),
		})
		requireDataWriteError(t, err, DataNotSent, ErrNoDataStream)
		if capture.submissions() != 0 {
			t.Errorf("DATA reached the transport with no legal stream")
		}
	})
}

// Acceptance bullet 2, last clause: a direct association write consults no
// route inventory. The Endpoint's own MTP-TRANSFER refuses for want of routes
// in exactly the same configuration.
func TestWriteDataIsIndependentOfLocalRouteAvailability(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: inventoryOnlyASPConfig()})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	conn, capture := newDataWriteAssociation(t, 1)
	conn.cfg.PeerSGP = &SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	if !endpoint.trackAssociation(conn) {
		t.Fatal("the association was not attached to the Endpoint")
	}

	if _, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: params.NewProtocolDataPayload(0x111111, 0x222222, params.ServiceIndSCCP, 0, 0, 1, []byte("routed")),
	}); !errors.Is(err, ErrRoutingNotConfigured) {
		t.Fatalf("MTPTransfer error = %v, want %v", err, ErrRoutingNotConfigured)
	}

	if _, err := conn.WriteData(DataRequest{
		AS:           writeScope(1),
		ProtocolData: simpleProtocolData("direct"),
	}); err != nil {
		t.Fatalf("WriteData with no route inventory: %v", err)
	}
	if capture.submissions() != 1 {
		t.Errorf("the direct write reached the transport %d times, want 1", capture.submissions())
	}
}

// Acceptance bullet 4: a successful send is local transport acceptance and
// nothing more. RFC 4666 has no acknowledgement for DATA, so the library cannot
// and does not claim the peer received it.
func TestWriteDataReportsLocalAcceptanceOnly(t *testing.T) {
	conn, capture := newDataWriteAssociation(t, 1)
	octets, err := conn.WriteData(DataRequest{
		AS:           writeScope(1),
		ProtocolData: simpleProtocolData("accepted locally"),
	})
	if err != nil {
		t.Fatalf("WriteData: %v", err)
	}
	if octets != len("accepted locally") {
		t.Errorf("WriteData = %d, want %d", octets, len("accepted locally"))
	}
	if capture.submissions() != 1 {
		t.Fatalf("the message was submitted %d times, want 1", capture.submissions())
	}
	// Nothing about the peer is known: no acknowledgement was awaited and none
	// is outstanding.
	if got := pendingAcknowledgements(conn); got != 0 {
		t.Errorf("a DATA send left %d acknowledgement waits outstanding", got)
	}
}

// pendingAcknowledgements counts the tracked procedure acknowledgement waits a
// send left behind. DATA has no acknowledgement in RFC 4666, so the answer is
// always zero for a payload write.
func pendingAcknowledgements(c *Association) int {
	if c.tack == nil {
		return 0
	}
	c.tack.mu.Lock()
	defer c.tack.mu.Unlock()
	return len(c.tack.pending)
}

// A failure the transport reported is conservatively indeterminate: submission
// had begun, so the library cannot say whether octets reached the peer, and it
// must not imply that a retry is safe.
func TestWriteDataClassifiesTransportFailureAsIndeterminate(t *testing.T) {
	conn, capture := newDataWriteAssociation(t, 1)
	transportFailure := errors.New("transport refused the message")
	capture.err = transportFailure

	_, err := conn.WriteData(DataRequest{
		AS:           writeScope(1),
		ProtocolData: simpleProtocolData("submitted"),
	})
	requireDataWriteError(t, err, DataSendIndeterminate, transportFailure)
	if capture.submissions() != 1 {
		t.Errorf("the message was submitted %d times, want exactly 1 with no retry",
			capture.submissions())
	}
}

// A transport that accepted only part of the message is the same case: some of
// it may be on the wire.
func TestWriteDataClassifiesPartialSubmissionAsIndeterminate(t *testing.T) {
	conn, capture := newDataWriteAssociation(t, 1)
	capture.short = 4

	_, err := conn.WriteData(DataRequest{
		AS:           writeScope(1),
		ProtocolData: simpleProtocolData("partially submitted"),
	})
	requireDataWriteError(t, err, DataSendIndeterminate, ErrPartialDataWrite)
	if capture.submissions() != 1 {
		t.Errorf("a partial submission was retried: %d submissions", capture.submissions())
	}
}

// Every refusal decided before submission is DataNotSent, and the transport
// never saw the message. These are the only outcomes from which a caller may
// safely resend.
func TestWriteDataClassifiesPreSubmissionFailuresAsNotSent(t *testing.T) {
	for _, test := range []struct {
		name    string
		prepare func(*Association)
		request DataRequest
		cause   error
	}{
		{
			name:    "unconfigured scope",
			request: DataRequest{AS: writeScope(9), ProtocolData: simpleProtocolData("x")},
			cause:   ErrInvalidRoutingContext,
		},
		{
			name: "inactive scope",
			// At an ASP the SGP's acknowledgement is what starts traffic, so
			// withdrawing it is what makes the scope inactive.
			prepare: func(c *Association) { c.noteNoRoutingContextsAcked() },
			request: DataRequest{AS: writeScope(1), ProtocolData: simpleProtocolData("x")},
			cause:   ErrRoutingContextNotActive,
		},
		{
			name:    "not established",
			prepare: func(c *Association) { c.setState(StateASPInactive) },
			request: DataRequest{AS: writeScope(1), ProtocolData: simpleProtocolData("x")},
			cause:   ErrNotEstablished,
		},
		{
			name:    "invalid stream",
			request: DataRequest{AS: writeScope(1), ProtocolData: simpleProtocolData("x"), Stream: 99},
			cause:   nil,
		},
		{
			name: "protocol data larger than the parameter length field",
			request: DataRequest{
				AS: writeScope(1),
				ProtocolData: params.ProtocolDataPayload{
					OriginatingPointCode: 1, DestinationPointCode: 2,
					ServiceIndicator: params.ServiceIndSCCP,
					Data:             make([]byte, maxProtocolDataOctets+1),
				},
			},
			cause: ErrProtocolDataTooLarge,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn, capture := newDataWriteAssociation(t, 1)
			if test.prepare != nil {
				test.prepare(conn)
			}
			_, err := conn.WriteData(test.request)
			requireDataWriteError(t, err, DataNotSent, test.cause)
			if capture.submissions() != 0 {
				t.Errorf("a pre-submission refusal reached the transport %d times",
					capture.submissions())
			}
		})
	}
}

// An asynchronous refusal from the peer arrives long after the send returned.
// It is reported to Layer Management as RFC 4666 Section 4.2's M-ERROR, it does
// not retroactively fail the accepted send, and nothing is retransmitted:
// resending a DATA the peer has already refused would be the unsafe retry.
func TestPeerErrorAfterAnAcceptedSendIsReportedWithoutRetransmission(t *testing.T) {
	conn, capture := newDataWriteAssociation(t, 1)
	if _, err := conn.WriteData(DataRequest{
		AS:           writeScope(1),
		ProtocolData: simpleProtocolData("already accepted"),
	}); err != nil {
		t.Fatalf("WriteData: %v", err)
	}
	submitted := capture.submissions()

	if err := conn.handleError(messages.NewError(
		params.NewErrorCode(params.ErrInvalidRoutingContext),
		params.NewRoutingContext(1), nil, nil, nil,
	)); err != nil {
		t.Fatalf("handleError: %v", err)
	}

	select {
	case indication := <-conn.ManagementIndications():
		if indication.Kind != ManagementError {
			t.Errorf("indication kind = %v, want %v", indication.Kind, ManagementError)
		}
		if indication.ErrorCode != params.ErrInvalidRoutingContext {
			t.Errorf("indication error code = %#x, want %#x",
				indication.ErrorCode, params.ErrInvalidRoutingContext)
		}
	default:
		t.Error("the peer's Error was not reported to Layer Management")
	}

	if capture.submissions() != submitted {
		t.Errorf("the peer's Error caused %d further submissions; the DATA must not be retransmitted",
			capture.submissions()-submitted)
	}
}

// WriteSignal is the escape hatch for a caller that has built the M3UA message
// itself. For DATA it must be held to exactly the same admission and outcome
// rules as WriteData, or the typed path is not the canonical one — it is merely
// the polite one. Its own return convention stays: the encoded message length,
// not the user octets.
func TestWriteSignalOfDataUsesTheSameAdmissionAndOutcomeRules(t *testing.T) {
	// A caller-built message leaves through the signal seam rather than the
	// transport seam, so the same capture is installed on both and the two
	// paths are measured identically.
	captureRawWrites := func(conn *Association, capture *dataFrameCapture) {
		conn.signalWriter = func(message messages.M3UA) (int, error) {
			frame, err := message.MarshalBinary()
			if err != nil {
				return 0, err
			}
			stream, err := conn.outboundDataStream(message.(*messages.Data))
			if err != nil {
				return 0, err
			}
			if _, err := capture.write(frame, &sctp.SndRcvInfo{Stream: stream}); err != nil {
				return 0, err
			}
			return len(frame), nil
		}
	}

	build := func(routingContext uint32, appearance *params.Param) *messages.Data {
		return messages.NewData(
			appearance,
			params.NewRoutingContext(routingContext),
			params.NewProtocolData(0x111111, 0x222222, params.ServiceIndSCCP, 0, 0, 1, []byte("raw")),
			nil,
		)
	}

	t.Run("an admitted scope is accepted and reports the encoded length", func(t *testing.T) {
		conn, capture := newDataWriteAssociation(t, 1)
		captureRawWrites(conn, capture)
		message := build(1, params.NewNetworkAppearance(7))
		n, err := conn.WriteSignal(message)
		if err != nil {
			t.Fatalf("WriteSignal(DATA): %v", err)
		}
		if n != message.MarshalLen() {
			t.Errorf("WriteSignal = %d, want the encoded length %d", n, message.MarshalLen())
		}
		if capture.submissions() != 1 {
			t.Errorf("the message was submitted %d times, want 1", capture.submissions())
		}
		if got := pendingAcknowledgements(conn); got != 0 {
			t.Errorf("a raw DATA write left %d tracked acknowledgement waits", got)
		}
	})

	// The seam above stands in for the transport in the tests that observe
	// decoded messages; the count a production write reports comes from the
	// path that has no seam at all.
	t.Run("the encoded length is reported by the transport path too", func(t *testing.T) {
		conn, capture := newDataWriteAssociation(t, 1)
		conn.signalWriter = nil
		message := build(1, params.NewNetworkAppearance(7))
		n, err := conn.WriteSignal(message)
		if err != nil {
			t.Fatalf("WriteSignal(DATA): %v", err)
		}
		if n != message.MarshalLen() {
			t.Errorf("WriteSignal = %d, want the encoded length %d", n, message.MarshalLen())
		}
		if capture.submissions() != 1 {
			t.Errorf("the message was submitted %d times, want 1", capture.submissions())
		}
	})

	t.Run("a scope that is not configured is refused as not sent", func(t *testing.T) {
		conn, capture := newDataWriteAssociation(t, 1)
		captureRawWrites(conn, capture)
		_, err := conn.WriteSignal(build(9, params.NewNetworkAppearance(7)))
		requireDataWriteError(t, err, DataNotSent, ErrInvalidRoutingContext)
		if capture.submissions() != 0 {
			t.Errorf("a refused raw DATA reached the transport")
		}
	})

	t.Run("a transport failure is indeterminate", func(t *testing.T) {
		conn, capture := newDataWriteAssociation(t, 1)
		captureRawWrites(conn, capture)
		transportFailure := errors.New("transport refused the message")
		capture.err = transportFailure
		_, err := conn.WriteSignal(build(1, params.NewNetworkAppearance(7)))
		requireDataWriteError(t, err, DataSendIndeterminate, transportFailure)
		if capture.submissions() != 1 {
			t.Errorf("submissions = %d, want exactly 1", capture.submissions())
		}
	})

	// Raw control messages keep their own return and acquire no procedure
	// acknowledgement wait: T(ack) belongs to the ASPSM and ASPTM procedures of
	// RFC 4666 Sections 4.3.4.1 to 4.3.4.4, which a raw write does not run.
	t.Run("a raw control write starts no acknowledgement wait", func(t *testing.T) {
		conn, _ := newTestConn(t, StateASPInactive, RoleASP)
		message := messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), nil, nil)
		n, err := conn.WriteSignal(message)
		if err != nil {
			t.Fatalf("WriteSignal(ASP Active): %v", err)
		}
		if n != message.MarshalLen() {
			t.Errorf("WriteSignal = %d, want the encoded length %d", n, message.MarshalLen())
		}
		if got := pendingAcknowledgements(conn); got != 0 {
			t.Errorf("a raw ASP Active write left %d tracked acknowledgement waits", got)
		}
	})
}

// The typed send path encodes the DATA itself rather than building the four
// parameters and marshalling them, which is what keeps a message from copying
// its payload twice on the busiest path in the library. That is only safe while
// the bytes are identical to the codec's, so the two are compared here over
// every combination of the optional parameters and a range of payload lengths —
// including the ones that exercise RFC 4666 Section 3.2 padding, where the
// parameter length excludes the pad octets that are nevertheless sent.
func TestEncodedDataMatchesTheCodec(t *testing.T) {
	conn, _ := newDataWriteAssociation(t, 1)

	for _, appearance := range []struct {
		value uint32
		set   bool
	}{{set: false}, {value: 0, set: true}, {value: 7, set: true}} {
		for _, routingContext := range []struct {
			value uint32
			set   bool
		}{{set: false}, {value: 0, set: true}, {value: 0x01020304, set: true}} {
			for _, correlation := range []struct {
				value uint32
				set   bool
			}{{set: false}, {value: 0, set: true}, {value: 4242, set: true}} {
				for _, payloadSize := range []int{0, 1, 2, 3, 4, 5, 7, 8, 255, 1024} {
					payload := make([]byte, payloadSize)
					for index := range payload {
						payload[index] = byte(index + 1)
					}
					request := DataRequest{
						AS: ASKey{
							NetworkAppearance:    appearance.value,
							NetworkAppearanceSet: appearance.set,
							RoutingContext:       routingContext.value,
							RoutingContextSet:    routingContext.set,
						},
						ProtocolData: params.ProtocolDataPayload{
							OriginatingPointCode:    0x0a0b0c0d,
							DestinationPointCode:    0x01020304,
							ServiceIndicator:        params.ServiceIndISUP,
							NetworkIndicator:        2,
							MessagePriority:         3,
							SignallingLinkSelection: 9,
							Data:                    payload,
						},
						CorrelationID:    correlation.value,
						CorrelationIDSet: correlation.set,
					}

					var appearanceParam, routingContextParam, correlationParam *params.Param
					if appearance.set {
						appearanceParam = params.NewNetworkAppearance(appearance.value)
					}
					if routingContext.set {
						routingContextParam = params.NewRoutingContext(routingContext.value)
					}
					if correlation.set {
						correlationParam = params.NewCorrelationID(correlation.value)
					}
					want, err := messages.NewData(
						appearanceParam,
						routingContextParam,
						params.NewProtocolData(
							request.ProtocolData.OriginatingPointCode,
							request.ProtocolData.DestinationPointCode,
							request.ProtocolData.ServiceIndicator,
							request.ProtocolData.NetworkIndicator,
							request.ProtocolData.MessagePriority,
							request.ProtocolData.SignallingLinkSelection,
							payload,
						),
						correlationParam,
					).MarshalBinary()
					if err != nil {
						t.Fatalf("codec MarshalBinary: %v", err)
					}

					got := conn.encodeDataFrame(&request)
					if string(got) != string(want) {
						t.Fatalf("encoded DATA differs from the codec\n"+
							"NA %v/%v RC %v/%v correlation %v/%v payload %d octets\n got % x\nwant % x",
							appearance.value, appearance.set,
							routingContext.value, routingContext.set,
							correlation.value, correlation.set, payloadSize, got, want)
					}
					// And it decodes back to the same message.
					decoded, err := messages.Parse(got)
					if err != nil {
						t.Fatalf("the encoded DATA did not decode: %v", err)
					}
					data, ok := decoded.(*messages.Data)
					if !ok {
						t.Fatalf("the encoded DATA decoded as %T", decoded)
					}
					back, err := data.ProtocolData.ProtocolData()
					if err != nil {
						t.Fatalf("decoding the Protocol Data: %v", err)
					}
					if string(back.Data) != string(payload) {
						t.Fatalf("payload survived as % x, want % x", back.Data, payload)
					}
				}
			}
		}
	}
}

// An ASP authorized for nothing carries nothing, including the contextless
// Application Server. RFC 4666 Section 4.3.4.3 has the SGP acknowledge "the
// Application Servers for which the ASP can be activated"; an empty
// authorization names none of them.
func TestWriteDataRefusesAnASPAuthorizedForNothing(t *testing.T) {
	conn, capture := newDataWriteAssociation(t)
	conn.role = RoleSGP
	conn.cfg.AuthorizeASP = func(ASPIdentity) []uint32 { return nil }
	if err := conn.resolveASPAuthorization(params.NewAspIdentifier(7)); err != nil {
		t.Fatalf("resolveASPAuthorization: %v", err)
	}

	_, err := conn.WriteData(DataRequest{
		AS:           ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true},
		ProtocolData: simpleProtocolData("unauthorized"),
	})
	requireDataWriteError(t, err, DataNotSent, ErrUnknownApplicationServerScope)
	if capture.submissions() != 0 {
		t.Errorf("an unauthorized ASP reached the transport %d times", capture.submissions())
	}
}

// A contextless Application Server is activated like any other: RFC 4666
// Section 4.3.4.3 makes the SGP's acknowledgement what starts traffic, and with
// no Routing Context coordinated the acknowledgement that names none is the one
// that covers it.
func TestWriteDataRefusesAContextlessScopeBeforeItIsAcknowledged(t *testing.T) {
	conn, capture := newDataWriteAssociation(t)
	scope := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true}

	conn.noteNoRoutingContextsAcked()
	_, err := conn.WriteData(DataRequest{AS: scope, ProtocolData: simpleProtocolData("too early")})
	requireDataWriteError(t, err, DataNotSent, ErrRoutingContextNotActive)
	if capture.submissions() != 0 {
		t.Fatalf("an unacknowledged contextless scope reached the transport")
	}

	conn.noteRoutingContextsAcked(nil)
	if _, err := conn.WriteData(DataRequest{
		AS: scope, ProtocolData: simpleProtocolData("acknowledged"),
	}); err != nil {
		t.Fatalf("WriteData after the acknowledgement: %v", err)
	}
}
