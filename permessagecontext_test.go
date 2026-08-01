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
// SelectRoutingContext stores it on the Conn instead, so naming a flow and
// sending on it are two steps with a window between them. This states that
// window without any timing: the two selections are the two flows' choices, and
// the read that follows is the first flow's write. It gets the second flow's
// context, and the payload goes out mis-attributed.
//
// The window is not closable from outside the package. A caller can only wrap
// select-and-write in its own lock, which serialises every send on the
// association — a throughput cost paid for a field that is per-message by
// definition.
func TestTheAssociationWideSelectionLosesTheCallersChoice(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient, 1, 2)

	// Flow A names its context, then flow B names its own before flow A gets
	// to write. Two goroutines produce this ordering; stating it directly is
	// the same sequence without the timing.
	if err := conn.SelectRoutingContext(1); err != nil {
		t.Fatalf("SelectRoutingContext(1): %v", err)
	}
	if err := conn.SelectRoutingContext(2); err != nil {
		t.Fatalf("SelectRoutingContext(2): %v", err)
	}

	rc, err := conn.dataRoutingContext()
	if err != nil {
		t.Fatalf("dataRoutingContext: %v", err)
	}
	if got := rc.RoutingContexts(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("association-wide selection resolved to %v, want [2]; the "+
			"premise of this test no longer holds", got)
	}

	// The per-message form takes flow A's own context through the identical
	// interleaving, because it reads nothing the other flow can write.
	flowA := uint32(1)
	rc, err = conn.resolveRoutingContext(&flowA)
	if err != nil {
		t.Fatalf("resolveRoutingContext: %v", err)
	}
	if got := rc.RoutingContexts(); len(got) != 1 || got[0] != 1 {
		t.Errorf("the message named Routing Context 1 and went out as %v; the "+
			"traffic flow is mis-identified", got)
	}
}

// A write that names no context keeps the association-wide behaviour exactly:
// this is the single-flow caller the selection was designed for, and it must
// not be disturbed by the per-message form existing.
func TestAWriteThatNamesNoContextStillUsesTheSelection(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient, 1, 2)

	if _, err := conn.resolveRoutingContext(nil); !errors.Is(err, ErrAmbiguousRoutingContext) {
		t.Errorf("error = %v, want ErrAmbiguousRoutingContext with several "+
			"contexts and none selected", err)
	}

	if err := conn.SelectRoutingContext(2); err != nil {
		t.Fatalf("SelectRoutingContext: %v", err)
	}
	rc, err := conn.resolveRoutingContext(nil)
	if err != nil {
		t.Fatalf("resolveRoutingContext: %v", err)
	}
	if got := rc.RoutingContexts(); len(got) != 1 || got[0] != 2 {
		t.Errorf("Routing Context = %v, want [2]", got)
	}
}

// Naming a context is not a way around the checks the selection had to pass.
func TestAPerMessageContextIsStillValidated(t *testing.T) {
	t.Run("a context the association does not carry is refused", func(t *testing.T) {
		conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient, 1, 2)

		_, err := conn.routingContextFor(9)
		if err == nil {
			t.Fatal("a DATA named a Routing Context this association never coordinated")
		}
		var rcErr *RoutingContextError
		if !errors.As(err, &rcErr) {
			t.Errorf("error = %v (%T), want a RoutingContextError", err, err)
		}
	})

	// Section 4.3.4.3 has the SGP acknowledge "the Application Servers for
	// which the ASP can be activated", so a partial Ack leaves the rest
	// inactive and naming one of those explicitly must not send traffic for it.
	t.Run("a context the peer never acknowledged is refused", func(t *testing.T) {
		asp, _ := newTestConnWithContexts(t, StateAspInactive, modeClient, 1, 2)
		if err := asp.handleAspActiveAck(messages.NewAspActiveAck(
			params.NewTrafficModeType(params.TrafficModeLoadshare),
			params.NewRoutingContext(1), nil)); err != nil {
			t.Fatalf("handleAspActiveAck: %v", err)
		}
		asp.setState(StateAspActive)

		if _, err := asp.routingContextFor(1); err != nil {
			t.Errorf("DATA refused for the acknowledged Routing Context: %v", err)
		}
		if _, err := asp.routingContextFor(2); !errors.Is(err, ErrRoutingContextNotActive) {
			t.Errorf("error = %v, want ErrRoutingContextNotActive for a context "+
				"no ASP Active Ack acknowledged", err)
		}
	})

	t.Run("server and selected paths enforce per-AS activity", func(t *testing.T) {
		server, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 1, 2)
		server.noteRoutingContextsActive([]uint32{1})
		if _, err := server.routingContextFor(2); !errors.Is(err, ErrRoutingContextNotActive) {
			t.Errorf("server explicit inactive RC error = %v, want ErrRoutingContextNotActive", err)
		}
		if err := server.SelectRoutingContext(2); err != nil {
			t.Fatal(err)
		}
		if _, err := server.dataRoutingContext(); !errors.Is(err, ErrRoutingContextNotActive) {
			t.Errorf("server selected inactive RC error = %v, want ErrRoutingContextNotActive", err)
		}

		client, _ := newTestConnWithContexts(t, StateAspActive, modeClient, 1, 2)
		client.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
		client.noteRoutingContextsOverridden([]uint32{2})
		if err := client.SelectRoutingContext(2); err != nil {
			t.Fatal(err)
		}
		if _, err := client.dataRoutingContext(); !errors.Is(err, ErrRoutingContextNotActive) {
			t.Errorf("client selected overridden RC error = %v, want ErrRoutingContextNotActive", err)
		}
	})

	// With no Routing Key coordinated the parameter is omitted, which Section
	// 3.3.1 permits. Naming a flow anyway is a different statement and must not
	// be quietly downgraded to the omission.
	t.Run("naming a context with none coordinated is refused", func(t *testing.T) {
		conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient)

		if rc, err := conn.resolveRoutingContext(nil); err != nil || rc != nil {
			t.Errorf("resolveRoutingContext(nil) = %v, %v; want the parameter omitted", rc, err)
		}
		if _, err := conn.routingContextFor(1); err == nil {
			t.Error("a DATA named a Routing Context although no Routing Key was coordinated")
		}
	})
}

// The read side of the same sentence. "Assisting in the internal distribution of
// Data messages" is something only the receiving application can do, and it
// needs to be told which flow the message arrived on to do it — including to
// answer on that same flow.
func TestReceivedDataReportsTheTrafficFlowItNamed(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 7, 8)

	conn.handleData(context.Background(), messages.NewData(
		nil,
		params.NewRoutingContext(8),
		params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
		nil,
	))

	d, err := conn.ReadData()
	if err != nil {
		t.Fatalf("ReadData: %v", err)
	}
	if !d.RoutingContextSet {
		t.Fatal("the DATA named Routing Context 8 and arrived with none; the " +
			"application cannot distribute it to a traffic flow")
	}
	if d.RoutingContext != 8 {
		t.Errorf("RoutingContext = %d, want 8", d.RoutingContext)
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
		conn, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 7, 8)
		conn.noteRoutingContextsActive([]uint32{7})

		conn.handleData(context.Background(), newData(8))

		err := firstErr(conn)
		var unexpected *UnexpectedMessageError
		if !errors.As(err, &unexpected) {
			t.Fatalf("error = %v (%T), want UnexpectedMessageError", err, err)
		}
		if len(conn.dataChan) != 0 {
			t.Error("DATA for the inactive AS reached the MTP3-User")
		}

		conn.handleData(context.Background(), newData(7))
		if err := firstErr(conn); err != nil {
			t.Fatalf("DATA for the active AS was rejected: %v", err)
		}
		if len(conn.dataChan) != 1 {
			t.Error("DATA for the active AS was not delivered")
		}
	})

	t.Run("ASP silently discards traffic for an inactive AS", func(t *testing.T) {
		conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient, 7, 8)
		conn.noteRoutingContextsAcked(params.NewRoutingContext(7))

		conn.handleData(context.Background(), newData(8))

		if err := firstErr(conn); err != nil {
			t.Fatalf("inactive ASP reflected an Error instead of silently discarding DATA: %v", err)
		}
		if len(conn.dataChan) != 0 {
			t.Error("DATA for the inactive AS reached the MTP3-User")
		}

		conn.handleData(context.Background(), newData(7))
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
	conn, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 7)

	conn.handleData(context.Background(), messages.NewData(
		nil, nil,
		params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
		nil,
	))

	d, err := conn.ReadData()
	if err != nil {
		t.Fatalf("ReadData: %v", err)
	}
	if d.RoutingContextSet {
		t.Errorf("a DATA carrying no Routing Context reported one (%d)", d.RoutingContext)
	}
}

// A peer may legitimately use Routing Context 0, so the zero value alone cannot
// mean "absent".
func TestRoutingContextZeroIsReportedAsPresent(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 0)

	conn.handleData(context.Background(), messages.NewData(
		nil,
		params.NewRoutingContext(0),
		params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
		nil,
	))

	d, err := conn.ReadData()
	if err != nil {
		t.Fatalf("ReadData: %v", err)
	}
	if !d.RoutingContextSet {
		t.Error("Routing Context 0 was reported as absent; it is a context like any other")
	}
}

// End to end over a real association, in the reporting caller's own operating
// mode: one goroutine per message, all writing to a single shared Conn, each
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
			d, err := srvConn.ReadData()
			if err != nil {
				return
			}
			inbox <- received{d.ProtocolData.Data, d.RoutingContext, d.RoutingContextSet}
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
				pd := params.NewProtocolData(
					0x11111111, 0x22222222, params.ServiceIndSCCP, 0, 0, 1, payload)
				if err := cliConn.SetWriteDeadline(time.Now().Add(20 * time.Second)); err != nil {
					t.Errorf("SetWriteDeadline: %v", err)
					return
				}
				if _, err := cliConn.WritePDWithRoutingContext(pd, rc); err != nil {
					t.Errorf("WritePDWithRoutingContext(rc=%d): %v", rc, err)
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

// Every one of the four new writes has to name the flow, not just the one the
// reporting caller happened to use. They differ in how the stream and the
// routing label are chosen, and each resolves the Routing Context on its own
// path, so a fix applied to one of them is not a fix applied to all.
func TestEveryWithRoutingContextWriteNamesTheFlow(t *testing.T) {
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

	newPD := func(payload string) *params.Param {
		return params.NewProtocolData(
			0x11111111, 0x22222222, params.ServiceIndSCCP, 0, 0, 1, []byte(payload))
	}

	writes := []struct {
		name  string
		write func(payload string, rtCtx uint32) (int, error)
	}{
		{"Write", func(p string, rc uint32) (int, error) {
			return cliConn.WriteWithRoutingContext([]byte(p), rc)
		}},
		{"WriteToStream", func(p string, rc uint32) (int, error) {
			return cliConn.WriteToStreamWithRoutingContext([]byte(p), 1, rc)
		}},
		{"WritePD", func(p string, rc uint32) (int, error) {
			return cliConn.WritePDWithRoutingContext(newPD(p), rc)
		}},
		{"WritePDToStream", func(p string, rc uint32) (int, error) {
			return cliConn.WritePDToStreamWithRoutingContext(newPD(p), 1, rc)
		}},
	}

	// setupConn's association carries Routing Contexts 1 and 2, and the helper
	// leaves one of them selected association-wide -- so a write that ignored
	// its argument would still resolve to a context and succeed. That is not
	// hypothetical: an earlier version of this test named the same context the
	// helper had selected, and two of the four methods passed it with their
	// argument deleted.
	//
	// So each round pins the selection to a decoy and has every write name the
	// other context. Dropping the argument falls back to the decoy and is
	// caught; hardcoding either context is caught by the round that names the
	// other one.
	want := make(map[string]uint32, len(writes)*2)
	for _, round := range []struct{ decoy, named uint32 }{{1, 2}, {2, 1}} {
		if err := cliConn.SelectRoutingContext(round.decoy); err != nil {
			t.Fatalf("SelectRoutingContext(%d): %v", round.decoy, err)
		}
		for _, w := range writes {
			payload := fmt.Sprintf("%s-rc%d", w.name, round.named)
			if _, err := w.write(payload, round.named); err != nil {
				t.Fatalf("%s naming Routing Context %d: %v", w.name, round.named, err)
			}
			want[payload] = round.named
		}
	}

	for i := 0; i < len(writes)*2; i++ {
		done := make(chan *DataMessage, 1)
		go func() {
			d, err := srvConn.ReadData()
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
			if !d.RoutingContextSet {
				t.Errorf("payload %q arrived with no Routing Context", payload)
			} else if d.RoutingContext != expect {
				t.Errorf("payload %q arrived under Routing Context %d, want %d",
					payload, d.RoutingContext, expect)
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
	conn, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 7, 8)

	conn.handleData(context.Background(), messages.NewData(
		nil,
		params.NewRoutingContext(7, 8),
		params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
		nil,
	))

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
			conn, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 7)

			conn.handleData(context.Background(), messages.NewData(
				nil,
				params.NewParam(int(params.RoutingContext), tc.data),
				params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
				nil,
			))

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
		conn, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 7)
		conn.handleData(context.Background(), messages.NewData(
			nil, nil,
			params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")), nil))
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
		conn, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 7, 8)
		conn.handleData(context.Background(), messages.NewData(
			nil, nil,
			params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")), nil))

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
		conn, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 7, 8)

		conn.handleData(context.Background(), messages.NewData(
			nil,
			params.NewParam(int(params.RoutingContext), rcData),
			params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
			nil,
		))

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
			if d.RoutingContextSet && d.RoutingContext != 7 && d.RoutingContext != 8 {
				t.Fatalf("delivered under Routing Context %d, which this "+
					"association does not serve", d.RoutingContext)
			}
		default:
			if err := firstErr(conn); err == nil {
				t.Fatal("the DATA was neither delivered nor reported")
			}
		}
	})
}
