// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// RFC 4666 Section 3.3.1 makes the Routing Context a property of the message:
//
//	The Routing Context parameter contains the Routing Context value
//	associated with the DATA message.  ...  Where multiple Routing Keys
//	and Routing Contexts are used across a common association, the
//	Routing Context MUST be sent to identify the traffic flow, assisting
//	in the internal distribution of Data messages.
//
// There is no longer an association-wide selection for a second goroutine to
// overwrite between one goroutine naming its flow and writing on it: the scope
// is a field of DataRequest, and the association holds no outbound scope state
// at all. The tests that pinned that window, and the four parallel write forms
// that each resolved the context on their own path, are gone with the API they
// described. What they were protecting is tested here and in datawrite_test.go:
// a write names its own scope exactly, that scope is validated, and concurrent
// flows keep their own.

// Naming a context is not a way around the checks the association-wide
// selection had to pass.
func TestAPerMessageContextIsStillValidated(t *testing.T) {
	t.Run("a context the association does not carry is refused", func(t *testing.T) {
		conn, capture := newDataWriteAssociation(t, 1, 2)
		_, err := conn.WriteData(DataRequest{
			AS:           writeScope(9),
			ProtocolData: testProtocolData([]byte("x")),
		})
		requireDataWriteError(t, err, DataNotSent, ErrInvalidRoutingContext)
		var rcErr *RoutingContextError
		if !errors.As(err, &rcErr) {
			t.Errorf("error = %v (%T), want a RoutingContextError", err, err)
		}
		if capture.submissions() != 0 {
			t.Error("a DATA naming an uncoordinated Routing Context reached the transport")
		}
	})

	// Section 4.3.4.3 has the SGP acknowledge "the Application Servers for
	// which the ASP can be activated", so a partial Ack leaves the rest
	// inactive and naming one of those explicitly must not send traffic for it.
	t.Run("a context the peer never acknowledged is refused", func(t *testing.T) {
		asp, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP, 1, 2)
		setInventoryNetworkAppearance(&asp.cfg.ApplicationServers, params.NewNetworkAppearance(7))
		capture := &dataFrameCapture{}
		asp.dataWriter = capture.write
		if err := asp.handleAspActiveAck(messages.NewAspActiveAck(
			params.NewTrafficModeType(params.TrafficModeLoadshare),
			params.NewRoutingContext(1), nil)); err != nil {
			t.Fatalf("handleAspActiveAck: %v", err)
		}
		asp.setState(StateASPActive)

		if _, err := asp.WriteData(DataRequest{
			AS:           writeScope(1),
			ProtocolData: testProtocolData([]byte("acknowledged")),
		}); err != nil {
			t.Errorf("DATA refused for the acknowledged Routing Context: %v", err)
		}
		_, err := asp.WriteData(DataRequest{
			AS:           writeScope(2),
			ProtocolData: testProtocolData([]byte("unacknowledged")),
		})
		requireDataWriteError(t, err, DataNotSent, ErrRoutingContextNotActive)
	})

	t.Run("an inactive or overridden context is refused", func(t *testing.T) {
		sgpAssociation, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1, 2)
		setInventoryNetworkAppearance(&sgpAssociation.cfg.ApplicationServers, params.NewNetworkAppearance(7))
		sgpAssociation.dataWriter = (&dataFrameCapture{}).write
		sgpAssociation.noteRoutingContextsActive([]uint32{1})
		_, err := sgpAssociation.WriteData(DataRequest{
			AS:           writeScope(2),
			ProtocolData: testProtocolData([]byte("x")),
		})
		requireDataWriteError(t, err, DataNotSent, ErrRoutingContextNotActive)

		aspAssociation, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
		setInventoryNetworkAppearance(&aspAssociation.cfg.ApplicationServers, params.NewNetworkAppearance(7))
		aspAssociation.dataWriter = (&dataFrameCapture{}).write
		aspAssociation.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
		aspAssociation.noteRoutingContextsOverridden([]uint32{2})
		_, err = aspAssociation.WriteData(DataRequest{
			AS:           writeScope(2),
			ProtocolData: testProtocolData([]byte("x")),
		})
		requireDataWriteError(t, err, DataNotSent, ErrRoutingContextNotActive)
	})

	// With no Routing Key coordinated the parameter is omitted, which Section
	// 3.3.1 permits. Naming a flow anyway is a different statement and must not
	// be quietly downgraded to the omission.
	t.Run("naming a context with none coordinated is refused", func(t *testing.T) {
		conn, capture := newDataWriteAssociation(t)
		conn.noteRoutingContextsAcked(nil)
		if _, err := conn.WriteData(DataRequest{
			AS:           ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true},
			ProtocolData: testProtocolData([]byte("omitted")),
		}); err != nil {
			t.Fatalf("WriteData with the parameter omitted: %v", err)
		}
		sent := capture.messages(t)
		if len(sent) != 1 || sent[0].RoutingContext != nil {
			t.Fatalf("the contextless write did not omit the Routing Context: %v", sent)
		}
		_, err := conn.WriteData(DataRequest{
			AS:           writeScope(1),
			ProtocolData: testProtocolData([]byte("named")),
		})
		requireDataWriteError(t, err, DataNotSent, ErrInvalidRoutingContext)
	})
}

// The read side of the same sentence. "Assisting in the internal distribution of
// Data messages" is something only the receiving application can do, and it
// needs to be told which flow the message arrived on to do it — including to
// answer on that same flow.
func TestReceivedDataReportsTheTrafficFlowItNamed(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 7, 8)

	conn.handleData(context.Background(), messages.NewData(
		nil,
		params.NewRoutingContext(8),
		params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
		nil,
	), nil)

	d, err := conn.ReadData(context.Background())
	if err != nil {
		t.Fatalf("ReadData: %v", err)
	}
	if !d.Scope.RoutingContextSet {
		t.Fatal("the DATA named Routing Context 8 and arrived with none; the " +
			"application cannot distribute it to a traffic flow")
	}
	if wireRoutingContext(d.Scope) != 8 {
		t.Errorf("RoutingContext = %d, want 8", wireRoutingContext(d.Scope))
	}
	if string(d.ProtocolData.Data) != "x" {
		t.Errorf("payload = %q, want %q", d.ProtocolData.Data, "x")
	}
}

// ASP state is maintained per Application Server. An association may remain
// ASP-ACTIVE because one Routing Context is active while another on the same
// association is ASP-INACTIVE; DATA must be judged against the named flow, not
// only the compatibility State() value.
func TestReceivedDataHonoursPerRoutingContextActivation(t *testing.T) {
	newData := func(rtCtx uint32) *messages.Data {
		return messages.NewData(
			nil,
			params.NewRoutingContext(rtCtx),
			params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
			nil,
		)
	}

	t.Run("SGP rejects traffic from an ASP inactive in that AS", func(t *testing.T) {
		conn, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 7, 8)
		conn.noteRoutingContextsActive([]uint32{7})

		conn.handleData(context.Background(), newData(8), nil)

		err := firstErr(conn)
		var unexpected *UnexpectedMessageError
		if !errors.As(err, &unexpected) {
			t.Fatalf("error = %v (%T), want UnexpectedMessageError", err, err)
		}
		if len(conn.dataChan) != 0 {
			t.Error("DATA for the inactive AS reached the MTP3-User")
		}

		conn.handleData(context.Background(), newData(7), nil)
		if err := firstErr(conn); err != nil {
			t.Fatalf("DATA for the active AS was rejected: %v", err)
		}
		if len(conn.dataChan) != 1 {
			t.Error("DATA for the active AS was not delivered")
		}
	})

	t.Run("ASP silently discards traffic for an inactive AS", func(t *testing.T) {
		conn, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 7, 8)
		conn.noteRoutingContextsAcked(params.NewRoutingContext(7))

		conn.handleData(context.Background(), newData(8), nil)

		if err := firstErr(conn); err != nil {
			t.Fatalf("inactive ASP reflected an Error instead of silently discarding DATA: %v", err)
		}
		if len(conn.dataChan) != 0 {
			t.Error("DATA for the inactive AS reached the MTP3-User")
		}

		conn.handleData(context.Background(), newData(7), nil)
		if err := firstErr(conn); err != nil {
			t.Fatalf("DATA for the active AS was rejected: %v", err)
		}
		if len(conn.dataChan) != 1 {
			t.Error("DATA for the active AS was not delivered")
		}
	})
}

// The parameter is Conditional, so its absence has to be distinguishable from a
// context that happens to be zero.
func TestReceivedDataWithoutARoutingContextSaysSo(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 7)

	conn.handleData(context.Background(), messages.NewData(
		nil, nil,
		params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
		nil,
	), nil)

	d, err := conn.ReadData(context.Background())
	if err != nil {
		t.Fatalf("ReadData: %v", err)
	}
	if d.Scope.RoutingContextSet {
		t.Errorf("a DATA carrying no Routing Context reported one (%d)", wireRoutingContext(d.Scope))
	}
}

// A peer may legitimately use Routing Context 0, so the zero value alone cannot
// mean "absent".
func TestRoutingContextZeroIsReportedAsPresent(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 0)

	conn.handleData(context.Background(), messages.NewData(
		nil,
		params.NewRoutingContext(0),
		params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
		nil,
	), nil)

	d, err := conn.ReadData(context.Background())
	if err != nil {
		t.Fatalf("ReadData: %v", err)
	}
	if !d.Scope.RoutingContextSet {
		t.Error("Routing Context 0 was reported as absent; it is a context like any other")
	}
}

// End to end over a real association, in the reporting caller's own operating
// mode: one goroutine per message, all writing to a single shared Association, each
// naming the traffic flow its message belongs to.
//
// Every payload carries the context it was sent for, so the receiver can check
// the pairing without trusting the sender's bookkeeping. Any mismatch is a
// message that went out under another flow's Routing Context.
func TestConcurrentFlowsOnOneAssociationKeepTheirRoutingContexts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cliConn, srvConn, err := setupConn(t, ctx, 3215)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cliConn.Close()
		_ = srvConn.Close()
	}()

	// setupConn configures Routing Contexts 1 and 2 on both ends.
	const perFlow = 200
	flows := []uint32{1, 2}
	total := len(flows) * perFlow

	// Read while the senders are still sending. Draining only afterwards fills
	// the receiver's socket buffer, SCTP flow control backpressures the sender,
	// and the write reports EAGAIN — the dependency sends with MSG_DONTWAIT on
	// purpose so a peer that stops reading cannot park a write for minutes. A
	// caller that wants to wait for buffer space sets a write deadline instead,
	// which is what the senders below do.
	type received struct {
		payload []byte
		rc      uint32
		set     bool
	}
	inbox := make(chan received, total)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for i := 0; i < total; i++ {
			d, err := srvConn.ReadData(context.Background())
			if err != nil {
				return
			}
			inbox <- received{d.ProtocolData.Data, wireRoutingContext(d.Scope), d.Scope.RoutingContextSet}
		}
	}()

	var wg sync.WaitGroup
	for _, rc := range flows {
		for i := 0; i < perFlow; i++ {
			wg.Add(1)
			go func(rc uint32, i int) {
				defer wg.Done()
				// The payload names the flow it was sent for, so the receiver
				// checks the pairing without trusting the sender's bookkeeping.
				payload := []byte(fmt.Sprintf("rc=%d seq=%d", rc, i))
				if err := cliConn.SetWriteDeadline(time.Now().Add(20 * time.Second)); err != nil {
					t.Errorf("SetWriteDeadline: %v", err)
					return
				}
				if _, err := cliConn.WriteData(DataRequest{
					AS:           associationScope(cliConn, rc),
					ProtocolData: testProtocolData(payload),
				}); err != nil {
					t.Errorf("WriteData(rc=%d): %v", rc, err)
				}
			}(rc, i)
		}
	}
	wg.Wait()

	select {
	case <-readerDone:
	case <-time.After(30 * time.Second):
		t.Fatalf("only %d of %d messages arrived within 30s", len(inbox), total)
	}

	got := make(map[uint32]int)
	for i := 0; i < total; i++ {
		r := <-inbox
		if !r.set {
			t.Fatalf("payload %q arrived with no Routing Context", r.payload)
		}
		want := fmt.Sprintf("rc=%d ", r.rc)
		if len(r.payload) < len(want) || string(r.payload[:len(want)]) != want {
			t.Fatalf("payload %q went out under Routing Context %d; the traffic "+
				"flow is mis-identified", r.payload, r.rc)
		}
		got[r.rc]++
	}

	for _, rc := range flows {
		if got[rc] != perFlow {
			t.Errorf("Routing Context %d carried %d messages, want %d", rc, got[rc], perFlow)
		}
	}
}

// Both remaining ways a payload reaches the wire have to carry the scope the
// caller named. They resolve it on different code paths — the typed request
// names it outright, while a caller-built DATA carries it in the message — so a
// fix applied to one is not a fix applied to the other.
func TestEveryPayloadWriteCarriesTheNamedFlow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cliConn, srvConn, err := setupConn(t, ctx, 3219)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cliConn.Close()
		_ = srvConn.Close()
	}()

	writes := []struct {
		name  string
		write func(payload string, routingContext uint32) (int, error)
	}{
		{"WriteData", func(p string, routingContext uint32) (int, error) {
			return cliConn.WriteData(DataRequest{
				AS:           associationScope(cliConn, routingContext),
				ProtocolData: testProtocolData([]byte(p)),
			})
		}},
		{"WriteData on an explicit stream", func(p string, routingContext uint32) (int, error) {
			return cliConn.WriteData(DataRequest{
				AS:           associationScope(cliConn, routingContext),
				ProtocolData: testProtocolData([]byte(p)),
				Stream:       1,
			})
		}},
		{"WriteSignal", func(p string, routingContext uint32) (int, error) {
			scope := associationScope(cliConn, routingContext)
			var appearance *params.Param
			if scope.NetworkAppearanceSet {
				appearance = params.NewNetworkAppearance(scope.NetworkAppearance)
			}
			return cliConn.WriteSignal(messages.NewData(
				appearance,
				params.NewRoutingContext(routingContext),
				params.NewProtocolData(0x11111111, 0x22222222, params.ServiceIndSCCP, 0, 0, 1, []byte(p)),
				nil,
			))
		}},
	}

	// setupConn's association carries Routing Contexts 1 and 2. Each round has
	// every write name the other one, so a write that resolved the scope from
	// the association's configuration rather than from the caller is caught
	// whichever context that configuration would have produced.
	want := make(map[string]uint32, len(writes)*2)
	for _, named := range []uint32{2, 1} {
		for _, w := range writes {
			payload := fmt.Sprintf("%s-rc%d", w.name, named)
			if _, err := w.write(payload, named); err != nil {
				t.Fatalf("%s naming Routing Context %d: %v", w.name, named, err)
			}
			want[payload] = named
		}
	}

	for i := 0; i < len(writes)*2; i++ {
		done := make(chan *DataMessage, 1)
		go func() {
			d, err := srvConn.ReadData(context.Background())
			if err == nil {
				done <- d
			}
		}()
		select {
		case d := <-done:
			payload := string(d.ProtocolData.Data)
			expect, ok := want[payload]
			if !ok {
				t.Fatalf("unexpected payload %q", payload)
			}
			if !d.Scope.RoutingContextSet {
				t.Errorf("payload %q arrived with no Routing Context", payload)
			} else if got := wireRoutingContext(d.Scope); got != expect {
				t.Errorf("payload %q arrived under Routing Context %d, want %d",
					payload, got, expect)
			}
			delete(want, payload)
		case <-time.After(20 * time.Second):
			t.Fatalf("only %d of %d messages arrived", i, len(writes)*2)
		}
	}
}

// Section 3.3.1 declares the DATA field singular — "Routing Context: 32 bits
// (unsigned integer)" — unlike the n x 32-bit field in SSNM messages. Taking
// the first of several silently reattributes a malformed message to a flow the
// sender did not name alone.
func TestReceivedDataWithSeveralRoutingContextsIsRejected(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 7, 8)

	conn.handleData(context.Background(), messages.NewData(
		nil,
		params.NewRoutingContext(7, 8),
		params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
		nil,
	), nil)

	err := firstErr(conn)
	var routingContextError *RoutingContextError
	if !errors.As(err, &routingContextError) {
		t.Fatalf("error = %v (%T), want a RoutingContextError", err, err)
	}
	if routingContextError.Code != params.ErrInvalidRoutingContext {
		t.Errorf("error code = %d, want %d (Invalid Routing Context)",
			routingContextError.Code, params.ErrInvalidRoutingContext)
	}
	if got := routingContextError.Contexts; len(got) != 2 || got[0] != 7 || got[1] != 8 {
		t.Errorf("offending contexts = %v, want [7 8]", got)
	}
	if len(conn.dataChan) != 0 {
		t.Error("the malformed DATA was delivered to the application")
	}
}

// RFC 4666 Section 3.8.1: "The 'Invalid Routing Context' error is sent if a
// message is received with an invalid or unconfigured routing context value."
//
// A Routing Context parameter that is present and decodes to nothing -- empty,
// or not a whole number of 32-bit words -- is invalid on its face. It used to be
// read as though the peer had sent no context at all, so a DATA carrying one was
// delivered to the application as unattributed traffic on an association that
// may serve several Application Servers. This test replaces one that pinned that
// behaviour and was written to fail once it improved; it did.
func TestDataWithAMalformedRoutingContextIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"empty value", []byte{}},
		{"three octets", []byte{0x00, 0x00, 0x07}},
		{"five octets", []byte{0x00, 0x00, 0x00, 0x07, 0x00}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 7)

			conn.handleData(context.Background(), messages.NewData(
				nil,
				params.NewParam(int(params.RoutingContext), tc.data),
				params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
				nil,
			), nil)

			err := firstErr(conn)
			if err == nil {
				t.Fatal("a DATA with a malformed Routing Context was accepted")
			}
			var rcErr *RoutingContextError
			if !errors.As(err, &rcErr) {
				t.Fatalf("error = %v (%T), want a RoutingContextError", err, err)
			}
			if rcErr.Code != params.ErrInvalidRoutingContext {
				t.Errorf("error code = %d, want %d (Invalid Routing Context)",
					rcErr.Code, params.ErrInvalidRoutingContext)
			}
			if len(conn.dataChan) != 0 {
				t.Error("the payload was delivered to the user anyway")
			}
		})
	}

	// A DATA that omits the parameter entirely is still fine: Section 3.3.1
	// says "Where a Routing Key has not been coordinated between the SGP and
	// ASP, sending of Routing Context is not required."
	t.Run("an omitted parameter is still accepted", func(t *testing.T) {
		conn, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 7)
		conn.handleData(context.Background(), messages.NewData(
			nil, nil,
			params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")), nil), nil)
		if err := firstErr(conn); err != nil {
			t.Fatalf("a DATA with no Routing Context was refused: %v", err)
		}
		if len(conn.dataChan) != 1 {
			t.Error("the payload was not delivered")
		}
	})

	// The exception above applies where no Routing Key was coordinated. Once
	// several Routing Keys share one association, Section 3.3.1 says Routing
	// Context MUST be sent so the receiver can identify the traffic flow.
	t.Run("an omitted parameter with several configured flows is rejected", func(t *testing.T) {
		conn, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 7, 8)
		conn.handleData(context.Background(), messages.NewData(
			nil, nil,
			params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")), nil), nil)

		if err := firstErr(conn); !errors.Is(err, ErrMissingRoutingContext) {
			t.Fatalf("error = %v, want ErrMissingRoutingContext", err)
		}
		if len(conn.dataChan) != 0 {
			t.Error("the unattributed DATA was delivered to the application")
		}
	})
}

// The inbound Routing Context is peer-controlled bytes reaching a decoder and
// then an index into its output, which is the shape that panics on a message
// nobody thought to write by hand. handleData runs on the dispatch goroutine and
// the package installs no recover(), so a panic here takes down every
// association the process serves.
func FuzzDataRoutingContext(f *testing.F) {
	for _, seed := range [][]byte{
		nil,
		{},
		{0x00, 0x00, 0x00, 0x07}, // the configured one
		{0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x08}, // two contexts
		{0x00, 0x00, 0x07},       // not a whole word
		{0xff, 0xff, 0xff, 0xff}, // the largest context
		{0x00, 0x00, 0x00, 0x00}, // context zero
		make([]byte, 1024),       // many zero contexts
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, rcData []byte) {
		conn, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 7, 8)

		conn.handleData(context.Background(), messages.NewData(
			nil,
			params.NewParam(int(params.RoutingContext), rcData),
			params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
			nil,
		), nil)

		// Either the message was refused or it was delivered; both are correct
		// answers, and neither may leave the two disagreeing.
		select {
		case d := <-conn.dataChan:
			if d.ProtocolData == nil {
				t.Fatal("a payload was delivered with no Protocol Data")
			}
			// Anything delivered names a context this association serves, or
			// none at all: passing traffic up under a context we do not carry is
			// what Section 3.8.1's Invalid Routing Context exists to prevent.
			if d.Scope.RoutingContextSet && wireRoutingContext(d.Scope) != 7 && wireRoutingContext(d.Scope) != 8 {
				t.Fatalf("delivered under Routing Context %d, which this "+
					"association does not serve", wireRoutingContext(d.Scope))
			}
		default:
			if err := firstErr(conn); err == nil {
				t.Fatal("the DATA was neither delivered nor reported")
			}
		}
	})
}
