// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// Acceptance bullet 6: the typed DATA API is available to both ends whichever
// of them opened the SCTP association.
//
// RFC 4666 Section 1.4.8 only recommends an orientation — "The default
// orientation would be for the SGP to take on the role of server while the ASP
// is the client" — and Section 1.2 keeps the M3UA role a property of the node,
// not of the transport. A DATA API that worked only for the client would make
// the recommendation binding.
func TestTypedDataCrossesBothSCTPInitiationDirections(t *testing.T) {
	scope := ASKey{NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	for _, test := range []struct {
		name         string
		port         int
		acceptRole   Role
		dialRole     Role
		acceptConfig *AssociationConfig
		dialConfig   *AssociationConfig
	}{
		{
			name: "ASP dials SGP", port: 3341,
			acceptRole: RoleSGP, dialRole: RoleASP,
			acceptConfig: mcSGPConfig(), dialConfig: mcASPConfig(0x11111111),
		},
		{
			name: "SGP dials ASP", port: 3343,
			acceptRole: RoleASP, dialRole: RoleSGP,
			acceptConfig: mcASPConfig(0x11111111), dialConfig: mcSGPConfig(),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			acceptEndpoint, err := NewEndpoint(EndpointConfig{Role: test.acceptRole})
			if err != nil {
				t.Fatalf("NewEndpoint(%v): %v", test.acceptRole, err)
			}
			dialEndpoint, err := NewEndpoint(EndpointConfig{Role: test.dialRole})
			if err != nil {
				t.Fatalf("NewEndpoint(%v): %v", test.dialRole, err)
			}
			address := mcAddr(test.port, "127.0.0.2")
			listener, err := acceptEndpoint.Listen("m3ua", address, NewListenerConfig(test.acceptConfig))
			if err != nil {
				skipIfSCTPUnsupported(t, err)
				t.Fatalf("Listen: %v", err)
			}
			defer func() { _ = listener.Close() }()

			type acceptResult struct {
				association *Association
				err         error
			}
			accepted := make(chan acceptResult, 1)
			go func() {
				association, acceptErr := listener.Accept(ctx)
				accepted <- acceptResult{association: association, err: acceptErr}
			}()
			dialed, err := dialEndpoint.Dial(ctx, "m3ua", mcAddr(test.port+100, "127.0.0.1"), address, test.dialConfig)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			defer func() { _ = dialed.Close() }()

			result := <-accepted
			if result.err != nil {
				t.Fatalf("Accept: %v", result.err)
			}
			acceptedAssociation := result.association
			defer func() { _ = acceptedAssociation.Close() }()

			for _, direction := range []struct {
				name     string
				from, to *Association
				payload  string
			}{
				{name: "dialed to accepted", from: dialed, to: acceptedAssociation, payload: "from the client"},
				{name: "accepted to dialed", from: acceptedAssociation, to: dialed, payload: "from the server"},
			} {
				t.Run(direction.name, func(t *testing.T) {
					payload := simpleProtocolData(direction.payload)
					octets, err := direction.from.WriteData(DataRequest{AS: scope, ProtocolData: payload})
					if err != nil {
						t.Fatalf("WriteData: %v", err)
					}
					if octets != len(direction.payload) {
						t.Errorf("WriteData = %d, want %d", octets, len(direction.payload))
					}
					readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
					defer readCancel()
					message, err := direction.to.ReadData(readCtx)
					if err != nil {
						t.Fatalf("ReadData: %v", err)
					}
					if got := string(message.ProtocolData.Data); got != direction.payload {
						t.Errorf("payload = %q, want %q", got, direction.payload)
					}
					if message.Stream == 0 {
						t.Error("DATA arrived on stream 0, which RFC 4666 Section 1.4.7 rule 1 forbids")
					}
					if message.AS != scope {
						t.Errorf("resolved Application Server = %+v, want %+v", message.AS, scope)
					}
				})
			}
		})
	}
}

// Acceptance bullet 6, IPSP Double Exchange: the two traffic directions are
// independent. RFC 4666 Section 5.6.2 gives each direction its own ASP state
// and its own Routing Key, so an active direction must not lend its state to
// the other.
func TestIPSPDoubleExchangeDirectionsAreIndependentForTypedData(t *testing.T) {
	peerScope := ASKey{NetworkAppearance: 20, NetworkAppearanceSet: true, RoutingContext: 22, RoutingContextSet: true}

	t.Run("only the local direction is active", func(t *testing.T) {
		association, _ := newDoubleExchangeIPSPForTest(t)
		association.maxMessageStreamID = 4
		association.setIPSPState(IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPInactive})
		association.noteRoutingContextsAcked(params.NewRoutingContext(11))

		_, err := association.WriteData(DataRequest{AS: peerScope, ProtocolData: simpleProtocolData("outbound")})
		requireDataWriteError(t, err, DataNotSent, ErrNotEstablished)

		association.recvStream.Store(1)
		association.handleData(context.Background(), messages.NewData(
			params.NewNetworkAppearance(10),
			params.NewRoutingContext(11),
			params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("inbound")),
			nil,
		), nil)
		message, err := association.ReadData(context.Background())
		if err != nil {
			t.Fatalf("ReadData on the active local direction: %v", err)
		}
		if got := string(message.ProtocolData.Data); got != "inbound" {
			t.Errorf("payload = %q, want %q", got, "inbound")
		}
	})

	t.Run("with no peer direction configured there is nothing to send in", func(t *testing.T) {
		config := newDoubleExchangeAssociationConfigForTest()
		config.IPSP.TrafficToPeer = nil
		association, _ := newDoubleExchangeIPSPWithConfigForTest(t, config)
		association.maxMessageStreamID = 4
		// The peer-directed state says traffic may flow; the configuration says
		// there is no peer-directed Application Server for it to flow in.
		association.setIPSPState(IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
		association.noteRoutingContextsActive([]uint32{22})

		_, err := association.WriteData(DataRequest{
			AS:           peerScope,
			ProtocolData: simpleProtocolData("no direction"),
		})
		requireDataWriteError(t, err, DataNotSent, ErrUnknownApplicationServerScope)
	})

	t.Run("only the peer direction is active", func(t *testing.T) {
		association, _ := newDoubleExchangeIPSPForTest(t)
		association.maxMessageStreamID = 4
		association.setIPSPState(IPSPState{TrafficToLocal: StateASPInactive, TrafficToPeer: StateASPActive})
		association.noteRoutingContextsActive([]uint32{22})

		if _, err := association.WriteData(DataRequest{
			AS: peerScope, ProtocolData: simpleProtocolData("outbound"),
		}); err != nil {
			t.Fatalf("WriteData on the active peer direction: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if _, err := association.ReadData(ctx); !errors.Is(err, ErrNotEstablished) {
			t.Errorf("ReadData on the inactive local direction = %v, want ErrNotEstablished", err)
		}
	})
}

// Acceptance bullet 6, dynamic Routing Key Management: a dynamically registered
// scope becomes writable at its registration and not before. RFC 4666 Section
// 4.4.1 makes the REG RSP the moment the Routing Context exists for this
// association; until then naming it is naming a scope that does not exist.
func TestDynamicallyRegisteredScopeBecomesWritableAtRegistration(t *testing.T) {
	conn, capture := newDataWriteAssociation(t)
	scope := ASKey{RoutingContext: 5, RoutingContextSet: true}

	_, err := conn.WriteData(DataRequest{AS: scope, ProtocolData: simpleProtocolData("before registration")})
	requireDataWriteError(t, err, DataNotSent, ErrInvalidRoutingContext)
	if capture.submissions() != 0 {
		t.Fatal("an unregistered scope reached the transport")
	}

	conn.addDynamicASKey(scope, RoutingKey{}, false)
	conn.noteRoutingContextsAcked(params.NewRoutingContext(5))

	if _, err := conn.WriteData(DataRequest{
		AS: scope, ProtocolData: simpleProtocolData("after registration"),
	}); err != nil {
		t.Fatalf("WriteData on the registered scope: %v", err)
	}
	sent := capture.messages(t)
	if len(sent) != 1 {
		t.Fatalf("%d messages reached the transport, want 1", len(sent))
	}
	if got := sent[0].RoutingContext.RoutingContexts(); len(got) != 1 || got[0] != 5 {
		t.Errorf("DATA named Routing Context %v, want [5]", got)
	}
	if sent[0].NetworkAppearance != nil {
		t.Errorf("DATA carried Network Appearance %v, want none", sent[0].NetworkAppearance)
	}

	// A Routing Key registered without a Network Appearance serves all of
	// them. RFC 4666 Section 3.6.1: "If the Network Appearance is not specified
	// and the Routing Key applies to all Network Appearances, then this Routing
	// Key MUST be the only one registered for the association; that is, Routing
	// Context is implied, and DATA and SSNM messages are discriminated on
	// Network Appearance rather than on Routing Context." So a message may name
	// the appearance its own traffic belongs to, and it travels on the wire.
	if _, err := conn.WriteData(DataRequest{
		AS: ASKey{
			NetworkAppearance:    33,
			NetworkAppearanceSet: true,
			RoutingContext:       5,
			RoutingContextSet:    true,
		},
		ProtocolData: simpleProtocolData("explicit appearance"),
	}); err != nil {
		t.Fatalf("WriteData naming an appearance for an all-appearance key: %v", err)
	}
	sent = capture.messages(t)
	if len(sent) != 2 {
		t.Fatalf("%d messages reached the transport, want 2", len(sent))
	}
	if sent[1].NetworkAppearance == nil || sent[1].NetworkAppearance.NetworkAppearance() != 33 {
		t.Errorf("DATA carried Network Appearance %v, want 33", sent[1].NetworkAppearance)
	}
}

// Acceptance bullet 7: BEAT and compatibility policy belong to one peer, not to
// the Listener that accepted it. RFC 4666 Section 4.3.4.6 runs T(beat) per
// association, and a tolerance configured for one misbehaving peer must not
// excuse another.
func TestBeatAndCompatibilityPolicyStayPerAssociation(t *testing.T) {
	strict := NewAssociationConfig()
	strict.HeartbeatInfo = &HeartbeatInfo{Enabled: false}

	tolerant := NewAssociationConfig()
	tolerant.HeartbeatInfo = NewHeartbeatInfo(50*time.Millisecond, 200*time.Millisecond)
	tolerant.Compatibility = AcceptInvalidOptionalInfoString()

	listenerConfig := &ListenerConfig{
		DefaultAssociationConfig: strict,
		SelectAssociationConfig: func(info AcceptInfo) (*AssociationConfig, error) {
			if info.RemoteAddr != nil && info.RemoteAddr.Port == 2 {
				return tolerant, nil
			}
			return strict, nil
		},
	}

	strictConfig, err := listenerConfig.associationConfigForAccept(AcceptInfo{RemoteAddr: mcAddr(1, "127.0.0.1")})
	if err != nil {
		t.Fatalf("associationConfigForAccept: %v", err)
	}
	tolerantConfig, err := listenerConfig.associationConfigForAccept(AcceptInfo{RemoteAddr: mcAddr(2, "127.0.0.1")})
	if err != nil {
		t.Fatalf("associationConfigForAccept: %v", err)
	}

	strictAssociation := newAssociation(RoleSGP, strictConfig)
	tolerantAssociation := newAssociation(RoleSGP, tolerantConfig)

	if strictAssociation.hb.Enabled {
		t.Error("the strict peer inherited the tolerant peer's M3UA BEAT policy")
	}
	if !tolerantAssociation.hb.Enabled {
		t.Error("the tolerant peer lost its own M3UA BEAT policy")
	}
	if strictAssociation.cfg.Compatibility.Tolerator != nil {
		t.Error("the strict peer inherited the tolerant peer's compatibility policy")
	}
	if tolerantAssociation.cfg.Compatibility.Tolerator == nil {
		t.Error("the tolerant peer lost its own compatibility policy")
	}

	// The snapshot is the isolation: mutating the shared configuration
	// afterwards must not reach an association already built from it.
	tolerant.HeartbeatInfo.Enabled = false
	tolerant.Compatibility = CompatibilityPolicy{}
	if !tolerantAssociation.hb.Enabled || tolerantAssociation.cfg.Compatibility.Tolerator == nil {
		t.Error("a later mutation of the shared configuration reached an established association")
	}
}

// Acceptance bullet 7, second clause: the M3UA BEAT of RFC 4666 Section 3.5.5
// is an M3UA message with its own timer, and is not SCTP path management.
// Enabling it changes no SCTP setting, and the BEAT that goes out carries fresh
// Heartbeat Data every round so an Ack cannot be replayed.
func TestM3UABeatIsSeparateFromSCTPHeartbeat(t *testing.T) {
	config := NewAssociationConfig()
	config.SCTPConfig = &SCTPConfig{}
	config.EnableHeartbeat(50*time.Millisecond, 200*time.Millisecond)

	if config.SCTPConfig.SCTPSACKInfo != nil || config.SCTPConfig.SCTPNoDelayInfo != nil {
		t.Error("EnableHeartbeat configured SCTP transport options")
	}

	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	var mu sync.Mutex
	var beats [][]byte
	conn.signalWriter = func(message messages.M3UA) (int, error) {
		beat, ok := message.(*messages.Heartbeat)
		if !ok {
			return message.MarshalLen(), nil
		}
		mu.Lock()
		if beat.HeartbeatData != nil {
			beats = append(beats, append([]byte(nil), beat.HeartbeatData.Data...))
		} else {
			beats = append(beats, nil)
		}
		mu.Unlock()
		// Answer it so the loop moves on to the next round.
		select {
		case conn.beatAckChan <- struct{}{}:
		default:
		}
		return message.MarshalLen(), nil
	}
	conn.hb = HeartbeatInfo{Enabled: true, Interval: time.Millisecond, Timer: 5 * time.Second}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go conn.heartbeat(ctx)
	conn.allowHeartbeat()

	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		rounds := len(beats)
		mu.Unlock()
		if rounds >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d M3UA BEATs were written", rounds)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()

	mu.Lock()
	first, second := beats[0], beats[1]
	mu.Unlock()
	if len(first) == 0 {
		t.Fatal("the M3UA BEAT carried no Heartbeat Data")
	}
	if string(first) == string(second) {
		t.Error("two M3UA BEATs carried the same Heartbeat Data; an Ack could be replayed")
	}
}
