// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// newTestConnWithContexts is newTestConn with the association's Routing
// Contexts chosen by the test, since what DATA may carry depends on how many
// have been coordinated.
func newTestConnWithContexts(t *testing.T, state State, m mode, rtCtxs ...uint32) (*Conn, *[]messages.M3UA) {
	t.Helper()
	conn, sent := newTestConn(t, state, m)
	conn.cfg.RoutingContexts = params.NewRoutingContext(rtCtxs...)
	// DATA must not go out on stream 0 (Section 1.4.7 rule 1), so give the
	// association a data stream to use and record arrivals on it.
	conn.maxMessageStreamID = 4
	conn.recvStream.Store(1)
	return conn, sent
}

// firstErr drains one error from the Conn's error channel, or nil.
func firstErr(c *Conn) error {
	select {
	case e := <-c.errChan:
		return e
	default:
		return nil
	}
}

// TestDataCarriesTheOneRoutingContextIdentifyingTheFlow covers RFC 4666 Section
// 3.3.1, which declares the field singular — "Routing Context: 32 bits
// (unsigned integer)" — and says what it is for:
//
//	The Routing Context parameter contains the Routing Context value
//	associated with the DATA message.  ...  Where multiple Routing Keys
//	and Routing Contexts are used across a common association, the
//	Routing Context MUST be sent to identify the traffic flow, assisting
//	in the internal distribution of Data messages.
//
// Sending every configured context defeats the parameter's purpose: it
// identifies no flow, so the receiver cannot distribute the message, and the
// case the sentence is actually about — several contexts on one association —
// is the one it gets most wrong.
func TestDataCarriesTheOneRoutingContextIdentifyingTheFlow(t *testing.T) {
	t.Run("one configured context is used", func(t *testing.T) {
		conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient, 7)
		rc, err := conn.dataRoutingContext()
		if err != nil {
			t.Fatalf("dataRoutingContext: %v", err)
		}
		if rc == nil {
			t.Fatal("DATA would carry no Routing Context although one is configured")
		}
		if got := rc.RoutingContexts(); len(got) != 1 || got[0] != 7 {
			t.Errorf("Routing Context = %v, want [7]", got)
		}
	})

	t.Run("several configured contexts need one selected", func(t *testing.T) {
		conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient, 7, 8, 9)
		rc, err := conn.dataRoutingContext()
		if err == nil {
			t.Errorf("DATA would go out naming %v with none selected; it cannot "+
				"identify the traffic flow", rc.RoutingContexts())
		}
		if !errors.Is(err, ErrAmbiguousRoutingContext) {
			t.Errorf("error = %v, want ErrAmbiguousRoutingContext", err)
		}
	})

	t.Run("the selected context is the one sent", func(t *testing.T) {
		conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient, 7, 8, 9)
		if err := conn.SelectRoutingContext(8); err != nil {
			t.Fatalf("SelectRoutingContext: %v", err)
		}
		rc, err := conn.dataRoutingContext()
		if err != nil {
			t.Fatalf("dataRoutingContext: %v", err)
		}
		if got := rc.RoutingContexts(); len(got) != 1 || got[0] != 8 {
			t.Errorf("Routing Context = %v, want [8]", got)
		}
	})

	t.Run("selecting an unconfigured context is refused", func(t *testing.T) {
		conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient, 7, 8)
		if err := conn.SelectRoutingContext(11); err == nil {
			t.Error("selected a Routing Context that is not configured")
		}
	})
}

// TestDataOmitsAnEmptyRoutingContext covers the same section:
//
//	Where a Routing Key has not been coordinated between the SGP and
//	ASP, sending of Routing Context is not required.
//
// The parameter is Conditional, so with no Routing Keys coordinated it must be
// left out. Emitting a zero-length one instead puts a parameter on the wire
// that names no context at all.
func TestDataOmitsAnEmptyRoutingContext(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient)
	rc, err := conn.dataRoutingContext()
	if err != nil {
		t.Fatalf("dataRoutingContext: %v", err)
	}
	if rc != nil {
		t.Errorf("DATA would carry a Routing Context of length %d with none "+
			"configured; the parameter is Conditional and must be omitted", rc.Length)
	}

	// And the message built from it leaves the parameter out entirely.
	d := messages.NewData(nil, rc,
		params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")), nil)
	b, err := d.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	back, err := messages.Parse(b)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := back.(*messages.Data).RoutingContext; got != nil {
		t.Errorf("the encoded DATA still carried a Routing Context: %v", got)
	}
}

// TestDataWithAnUnconfiguredRoutingContextIsRejected covers RFC 4666 Section
// 3.8.1:
//
//	The "Invalid Routing Context" error is sent if a message is received
//	with an invalid or unconfigured routing context value.
//
// A DATA naming a context this association does not serve was delivered to the
// user as though it belonged to a flow we had agreed to carry.
func TestDataWithAnUnconfiguredRoutingContextIsRejected(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 7)

	conn.handleData(context.Background(), messages.NewData(
		nil,
		params.NewRoutingContext(4242),
		params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
		nil,
	))

	err := firstErr(conn)
	if err == nil {
		t.Fatal("a DATA naming an unconfigured Routing Context was accepted")
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
}

// A DATA whose Routing Context is one we serve is delivered as before.
func TestDataWithAConfiguredRoutingContextIsDelivered(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 7)

	conn.handleData(context.Background(), messages.NewData(
		nil,
		params.NewRoutingContext(7),
		params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
		nil,
	))
	if err := firstErr(conn); err != nil {
		t.Fatalf("a DATA on a configured Routing Context was rejected: %v", err)
	}
	if len(conn.dataChan) != 1 {
		t.Error("the payload was not delivered")
	}
}

// A DATA with no Routing Context at all stays acceptable: the parameter is
// Conditional, and "Where a Routing Key has not been coordinated between the
// SGP and ASP, sending of Routing Context is not required."
func TestDataWithoutARoutingContextIsDelivered(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 7)

	conn.handleData(context.Background(), messages.NewData(
		nil, nil,
		params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
		nil,
	))
	if err := firstErr(conn); err != nil {
		t.Fatalf("a DATA without a Routing Context was rejected: %v", err)
	}
	if len(conn.dataChan) != 1 {
		t.Error("the payload was not delivered")
	}
}

// RFC 4666 Section 3.8.1 requires an SGP to reject an ASP's unconfigured
// Network Appearance and to include that exact value in the Error. Accepting
// the DATA loses both the network identity and the only feedback that lets the
// ASP repair its configuration.
func TestDataWithAnUnconfiguredNetworkAppearanceIsRejected(t *testing.T) {
	for _, tt := range []struct {
		name       string
		configured *params.Param
		peer       uint32
	}{
		{"different from configured", params.NewNetworkAppearance(7), 8},
		{"reverse decoy", params.NewNetworkAppearance(8), 7},
		{"none configured", nil, 8},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, sent := newTestConnWithContexts(t, StateAspActive, modeServer, 7)
			conn.cfg.NetworkAppearance = tt.configured

			conn.handleData(context.Background(), messages.NewData(
				params.NewNetworkAppearance(tt.peer),
				params.NewRoutingContext(7),
				params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
				nil,
			))

			reported := firstErr(conn)
			if reported == nil {
				t.Fatal("a DATA naming an unconfigured Network Appearance was accepted")
			}
			if !errors.Is(reported, ErrInvalidNetworkAppearance) {
				t.Errorf("error = %v, want ErrInvalidNetworkAppearance", reported)
			}
			var appearanceError *NetworkAppearanceError
			if !errors.As(reported, &appearanceError) || appearanceError.Appearance != tt.peer {
				t.Errorf("error = %#v, want NetworkAppearanceError carrying %d", reported, tt.peer)
			}
			if len(conn.dataChan) != 0 {
				t.Error("the payload was delivered to the user anyway")
			}
			if err := conn.handleErrors(reported); err != nil {
				t.Fatal(err)
			}

			e := lastError(t, *sent)
			if e.ErrorCode == nil || e.ErrorCode.ErrorCode() != params.ErrInvalidNetworkAppearance {
				t.Fatalf("error code = %v, want Invalid Network Appearance", e.ErrorCode)
			}
			if e.NetworkAppearance == nil || e.NetworkAppearance.NetworkAppearance() != tt.peer {
				t.Errorf("Error Network Appearance = %v, want the offending value %d",
					e.NetworkAppearance, tt.peer)
			}
			if e.RoutingContext != nil {
				t.Errorf("Error invented Routing Context %v", e.RoutingContext.RoutingContexts())
			}
		})
	}
}

func TestDataPreservesNetworkAppearanceAndPresence(t *testing.T) {
	for _, tt := range []struct {
		name       string
		role       mode
		configured *params.Param
		peer       *params.Param
		want       uint32
		wantSet    bool
	}{
		{"matching at SGP", modeServer, params.NewNetworkAppearance(7), params.NewNetworkAppearance(7), 7, true},
		{"different value at ASP", modeClient, params.NewNetworkAppearance(7), params.NewNetworkAppearance(8), 8, true},
		{"explicit zero", modeServer, params.NewNetworkAppearance(0), params.NewNetworkAppearance(0), 0, true},
		{"omitted", modeServer, params.NewNetworkAppearance(7), nil, 0, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, _ := newTestConnWithContexts(t, StateAspActive, tt.role, 7)
			conn.cfg.NetworkAppearance = tt.configured
			conn.handleData(context.Background(), messages.NewData(
				tt.peer,
				params.NewRoutingContext(7),
				params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
				nil,
			))

			if err := firstErr(conn); err != nil {
				t.Fatalf("valid DATA was rejected: %v", err)
			}
			select {
			case got := <-conn.dataChan:
				if got.NetworkAppearance != tt.want || got.NetworkAppearanceSet != tt.wantSet {
					t.Errorf("Network Appearance = %d (set=%v), want %d (set=%v)",
						got.NetworkAppearance, got.NetworkAppearanceSet, tt.want, tt.wantSet)
				}
			default:
				t.Fatal("valid DATA was not delivered")
			}
		})
	}
}

func TestMalformedDataNetworkAppearanceIsAParameterFieldError(t *testing.T) {
	for _, size := range []int{0, 1, 3, 5, 8} {
		t.Run(string(rune('0'+size)), func(t *testing.T) {
			conn, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 7)
			conn.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
			conn.handleData(context.Background(), messages.NewData(
				params.NewParam(int(params.NetworkAppearance), make([]byte, size)),
				params.NewRoutingContext(7),
				params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
				nil,
			))

			reported := firstErr(conn)
			var parameterFault *ParameterFaultError
			if !errors.As(reported, &parameterFault) {
				t.Fatalf("error = %v (%T), want ParameterFaultError", reported, reported)
			}
			if parameterFault.Code != params.ErrParameterFieldError {
				t.Errorf("error code = %d, want Parameter Field Error", parameterFault.Code)
			}
			if len(conn.dataChan) != 0 {
				t.Error("the malformed DATA was delivered")
			}
		})
	}
}

func FuzzDataNetworkAppearance(f *testing.F) {
	for _, seed := range [][]byte{
		nil,
		{},
		{0, 0, 0},
		{0, 0, 0, 0},
		{0, 0, 0, 7},
		{0, 0, 0, 8},
		{0, 0, 0, 7, 0},
		make([]byte, 1024),
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, appearanceData []byte) {
		conn, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 7)
		conn.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
		conn.handleData(context.Background(), messages.NewData(
			params.NewParam(int(params.NetworkAppearance), appearanceData),
			params.NewRoutingContext(7),
			params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
			nil,
		))

		shouldDeliver := len(appearanceData) == 4 && binary.BigEndian.Uint32(appearanceData) == 7
		select {
		case <-conn.dataChan:
			if !shouldDeliver {
				t.Fatal("an invalid Network Appearance was delivered")
			}
			if err := firstErr(conn); err != nil {
				t.Fatalf("the DATA was both delivered and rejected: %v", err)
			}
		default:
			if shouldDeliver {
				t.Fatal("the configured Network Appearance was not delivered")
			}
			if err := firstErr(conn); err == nil {
				t.Fatal("the DATA was neither delivered nor reported")
			}
		}
	})
}

func TestNetworkAppearanceValidationAcrossAssociation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	client, server, err := setupConn(t, ctx, 3121)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = client.Close()
		_ = server.Close()
	}()

	data := func(appearance uint32, payload string) *messages.Data {
		return messages.NewData(
			params.NewNetworkAppearance(appearance),
			params.NewRoutingContext(1),
			params.NewProtocolData(
				0x11111111, 0x22222222, params.ServiceIndSCCP, 0, 0, 2,
				[]byte(payload),
			),
			nil,
		)
	}

	if _, err := client.WriteSignal(data(0, "configured-network")); err != nil {
		t.Fatalf("WriteSignal(valid DATA): %v", err)
	}
	delivered, err := readDataWithin(t, server, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !delivered.NetworkAppearanceSet || delivered.NetworkAppearance != 0 {
		t.Errorf("valid DATA Network Appearance = %d (set=%v), want explicit zero",
			delivered.NetworkAppearance, delivered.NetworkAppearanceSet)
	}

	rawInvalid, err := data(8, "unconfigured-network").MarshalBinary()
	if err != nil {
		t.Fatalf("marshal invalid DATA: %v", err)
	}
	sctpInfo := *client.sctpInfo
	sctpInfo.Stream = client.streamFor(2)
	if _, err := client.sctpConn.SCTPWrite(rawInvalid, &sctpInfo); err != nil {
		t.Fatalf("raw write invalid DATA: %v", err)
	}
	deadline := time.After(10 * time.Second)
	for {
		select {
		case indication := <-client.ManagementIndications():
			if indication.Kind != ManagementError {
				continue
			}
			if indication.ErrorCode != params.ErrInvalidNetworkAppearance {
				t.Fatalf("Error code = %d, want Invalid Network Appearance", indication.ErrorCode)
			}
			if !indication.NetworkAppearanceSet || indication.NetworkAppearance != 8 {
				t.Errorf("Error Network Appearance = %d (set=%v), want 8",
					indication.NetworkAppearance, indication.NetworkAppearanceSet)
			}
			if len(server.dataChan) != 0 {
				t.Error("the invalid DATA was delivered despite the Error")
			}
			return
		case <-deadline:
			t.Fatal("the SGP sent no Invalid Network Appearance Error")
		}
	}
}

// TestDataOnStreamZeroIsRejected covers RFC 4666 Section 1.4.7:
//
//  1. DATA messages MUST NOT be sent on stream 0.
//
// The receive side already refuses ASPSM messages that arrive on a non-zero
// stream, using the same recorded arrival stream; DATA was the direction of the
// rule left unchecked, so a peer breaking it was rewarded with delivery.
func TestDataOnStreamZeroIsRejected(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 7)
	conn.recvStream.Store(0)

	conn.handleData(context.Background(), messages.NewData(
		nil,
		params.NewRoutingContext(7),
		params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
		nil,
	))

	err := firstErr(conn)
	if err == nil {
		t.Fatal("a DATA that arrived on stream 0 was accepted")
	}
	var streamErr *InvalidSCTPStreamIDError
	if !errors.As(err, &streamErr) {
		t.Errorf("error = %v (%T), want an InvalidSCTPStreamIDError", err, err)
	}
	if len(conn.dataChan) != 0 {
		t.Error("the payload was delivered to the user anyway")
	}
}
