package m3ua

import (
	"reflect"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func recoveryIPSP(t *testing.T, model string, local, peer State) (*Association, *[]messages.M3UA) {
	t.Helper()
	exchange, aspsm := IPSPExchangeDouble, IPSPASPSMExchangeDouble
	if model == "SE" {
		exchange = IPSPExchangeSingle
	}
	if model == "shared DE" {
		aspsm = IPSPASPSMExchangeSingle
	}
	c, sent := newIPSPDownAssociationForTest(t, exchange, aspsm, StateASPDown)
	c.recvStream.Store(0)
	if _, err := c.beginASPSM(); err != nil {
		t.Fatal(err)
	}
	c.handleSignals(t.Context(), messages.NewAspUpAck(nil, nil))
	applyUpAckPublications(t, c)
	c.cfg.ASPProcedures = ipspInitiationPolicy(true, true)
	if model == "SE" {
		c.setState(local)
	} else {
		c.setIPSPState(IPSPState{TrafficToLocal: local, TrafficToPeer: peer})
	}
	if local == StateASPActive {
		c.noteRoutingContextsAcked(c.configuredLocalRoutingContextParam())
	} else {
		c.noteNoRoutingContextsAcked()
	}
	if peer == StateASPActive {
		c.noteRoutingContextsActive(c.configuredRoutingContexts())
	} else {
		c.noteRoutingContextsInactive(nil)
	}
	c.stateEntered = true
	c.appliedState = c.State()
	*sent = nil
	return c, sent
}

func recoveryIPSPKeys(model string) (ASKey, ASKey) {
	if model == "SE" {
		return routingContextASKey(1), routingContextASKey(1)
	}
	return ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 11, RoutingContextSet: true}, ASKey{NetworkAppearance: 20, NetworkAppearanceSet: true, RoutingContext: 22, RoutingContextSet: true}
}

func assertPreviousIPSPState(t *testing.T, ind *ManagementIndication, want IPSPState) {
	t.Helper()
	field := reflect.ValueOf(ind).Elem().FieldByName("PreviousIPSPState")
	if !field.IsValid() || field.Interface() != want {
		t.Errorf("indication does not preserve previous IPSP directions %+v", want)
	}
}

func TestIPSPUnowedUpAckReportsAffectedDirections(t *testing.T) {
	for _, model := range []string{"SE", "DE", "shared DE"} {
		t.Run(model, func(t *testing.T) {
			c, sent := recoveryIPSP(t, model, StateASPActive, StateASPActive)
			dispatchRecoveryAck(t, c, messages.NewAspUpAck(nil, nil))
			want := IPSPState{TrafficToLocal: StateASPInactive, TrafficToPeer: StateASPInactive}
			if model == "DE" {
				want.TrafficToPeer = StateASPActive
			}
			if c.IPSPState() != want {
				t.Errorf("unowed Up Ack directions %+v, want %+v", c.IPSPState(), want)
			}
			if got := errorCodes(*sent); !reflect.DeepEqual(got, []uint32{params.UnexpectedMessageError}) {
				t.Errorf("Up Ack Error frames %v", got)
			}
			if countType(*sent, "ASP Active") != 0 {
				t.Error("IPSP started automatic recovery Active")
			}

			localKey, peerKey := recoveryIPSPKeys(model)
			if c.routingContextAcked(localKey.RoutingContext) {
				t.Error("withdrawal left local traffic gate open")
			}
			if model != "DE" && c.activeForASKey(peerKey) {
				t.Error("withdrawal left shared peer traffic gate open")
			}
			local, peer := recoveryIPSPKeys(model)
			keys := []ASKey{local}
			if model == "shared DE" {
				keys = append(keys, peer)
			}
			ind := recoveryIndication(t, c, "ASP-WITHDRAWAL", keys, StateASPActive)
			assertPreviousIPSPState(t, ind, IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
		})
	}
}

func TestSharedASPSMUpAckReportsActivePeerWhenLocalInactive(t *testing.T) {
	c, sent := recoveryIPSP(t, "shared DE", StateASPInactive, StateASPActive)
	dispatchRecoveryAck(t, c, messages.NewAspUpAck(nil, nil))
	if c.IPSPState() != (IPSPState{TrafficToLocal: StateASPInactive, TrafficToPeer: StateASPInactive}) {
		t.Errorf("shared withdrawal did not close peer direction: %+v", c.IPSPState())
	}
	if countType(*sent, "Error") != 1 || countType(*sent, "ASP Active") != 0 {
		t.Errorf("shared withdrawal wrote %v", typeNames(*sent))
	}
	_, peer := recoveryIPSPKeys("shared DE")
	ind := recoveryIndication(t, c, "ASP-WITHDRAWAL", []ASKey{peer}, StateASPInactive)
	assertPreviousIPSPState(t, ind, IPSPState{TrafficToLocal: StateASPInactive, TrafficToPeer: StateASPActive})
}

func TestIndependentASPSMInactiveUpAckPreservesActivePeer(t *testing.T) {
	c, sent := recoveryIPSP(t, "DE", StateASPInactive, StateASPActive)
	dispatchRecoveryAck(t, c, messages.NewAspUpAck(nil, nil))
	if c.IPSPState() != (IPSPState{TrafficToLocal: StateASPInactive, TrafficToPeer: StateASPActive}) || len(*sent) != 0 || len(c.mgmtChan) != 0 {
		t.Fatalf("independent inactive Up Ack changed service: %+v %v", c.IPSPState(), typeNames(*sent))
	}
}

func TestIPSPUnsolicitedDownAckReportsWithoutRecovery(t *testing.T) {
	for _, model := range []string{"SE", "DE", "shared DE"} {
		t.Run(model, func(t *testing.T) {
			c, sent := recoveryIPSP(t, model, StateASPActive, StateASPActive)
			dispatchRecoveryAck(t, c, messages.NewAspDownAck(nil))
			want := IPSPState{TrafficToLocal: StateASPDown, TrafficToPeer: StateASPDown}
			if model == "DE" {
				want.TrafficToPeer = StateASPActive
			}
			if c.IPSPState() != want || len(*sent) != 0 {
				t.Errorf("Down Ack changed directions or started recovery: %+v %v", c.IPSPState(), typeNames(*sent))
			}

			localKey, peerKey := recoveryIPSPKeys(model)
			if c.routingContextAcked(localKey.RoutingContext) {
				t.Error("withdrawal left local traffic gate open")
			}
			if model == "DE" && !c.activeForASKey(peerKey) {
				t.Error("independent withdrawal closed the peer traffic gate")
			}
			local, peer := recoveryIPSPKeys(model)
			keys := []ASKey{local}
			if model == "shared DE" {
				keys = append(keys, peer)
			}
			ind := recoveryIndication(t, c, "ASP-WITHDRAWAL", keys, StateASPActive)
			assertPreviousIPSPState(t, ind, IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
		})
	}
}

func TestIPSPUnsolicitedActiveAckReportsLocalGrant(t *testing.T) {
	for _, model := range []string{"SE", "DE", "shared DE"} {
		t.Run(model, func(t *testing.T) {
			peerState := StateASPActive
			if model == "SE" {
				peerState = StateASPInactive
			}
			c, sent := recoveryIPSP(t, model, StateASPInactive, peerState)
			key, _ := recoveryIPSPKeys(model)
			dispatchRecoveryAck(t, c, messages.NewAspActiveAck(nil, params.NewRoutingContext(key.RoutingContext), nil))
			if c.IPSPState() != (IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive}) {
				t.Errorf("grant changed wrong direction: %+v", c.IPSPState())
			}
			if len(*sent) != 0 {
				t.Errorf("accepted Active Ack wrote %v", typeNames(*sent))
			}
			ind := recoveryIndication(t, c, "ASP-ACTIVATION", []ASKey{key}, StateASPInactive)
			assertPreviousIPSPState(t, ind, IPSPState{TrafficToLocal: StateASPInactive, TrafficToPeer: peerState})
		})
	}
}

func TestSharedWithdrawalKeepsSameRoutingContextInDifferentNetworks(t *testing.T) {
	c, _ := recoveryIPSP(t, "shared DE", StateASPActive, StateASPActive)
	local, peer := recoveryIPSPKeys("shared DE")
	peer.RoutingContext = local.RoutingContext
	c.cfg.IPSP.TrafficToPeer.ApplicationServers = []ASConfig{{ASKey: peer, TrafficMode: params.TrafficModeLoadshare}}
	c.noteRoutingContextsActive([]uint32{peer.RoutingContext})
	dispatchRecoveryAck(t, c, messages.NewAspDownAck(nil))
	recoveryIndication(t, c, "ASP-WITHDRAWAL", []ASKey{local, peer}, StateASPActive)
}
