package m3ua

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestOmittedInactiveAckCompletesSoleAS(testContext *testing.T) {
	association, _ := newTestConn(testContext, StateASPActive, RoleASP)
	setInventoryRoutingContexts(&association.cfg.ApplicationServers, params.NewRoutingContext(1))
	association.cfg.TAck = time.Hour
	association.noteRoutingContextsAcked(params.NewRoutingContext(1))
	request := association.startTAck(messages.NewAspInactive(params.NewRoutingContext(1), nil), requestAspInactive)
	rawAcknowledgement, err := messages.NewAspInactiveAck(nil, params.NewInfoString("inactive")).MarshalBinary()
	if err != nil {
		testContext.Fatal(err)
	}
	acknowledgement, err := messages.ParseAspInactiveAck(rawAcknowledgement)
	if err != nil {
		testContext.Fatal(err)
	}
	if err := association.handleAspInactiveAck(acknowledgement); err != nil {
		testContext.Fatalf("omitted Routing Context on sole-AS Inactive Ack: %v", err)
	}
	if association.pendingTAck() != 0 || association.routingContextAcked(1) {
		testContext.Fatal("acknowledged AS remains active or pending")
	}
	select {
	case err := <-request.result:
		if err != nil {
			testContext.Fatalf("procedure result: %v", err)
		}
	default:
		testContext.Fatal("procedure waiter was not completed")
	}
	if acknowledgement.RoutingContext != nil {
		testContext.Fatal("received acknowledgement was mutated")
	}
	retainedAcknowledgement, err := acknowledgement.MarshalBinary()
	if err != nil || !bytes.Equal(retainedAcknowledgement, rawAcknowledgement) {
		testContext.Fatalf("received acknowledgement metadata changed: %v", err)
	}
	association.noteRoutingContextsAcked(params.NewRoutingContext(1))
	if err := association.handleAspInactiveAck(acknowledgement); err != nil {
		testContext.Fatalf("repeated acknowledgement: %v", err)
	}
	if !association.routingContextAcked(1) {
		testContext.Fatal("repeated acknowledgement reversed later activation")
	}
	association.startTAck(messages.NewAspInactive(params.NewRoutingContext(1), nil), requestAspInactive)
	if err := association.handleAspInactiveAck(acknowledgement); err != nil {
		testContext.Fatalf("new procedure after repeated acknowledgement: %v", err)
	}
	if association.pendingTAck() != 0 || association.routingContextAcked(1) {
		testContext.Fatal("prior acknowledgement hid the new inactive procedure")
	}
}

func TestOmittedInactiveAckRejectsAmbiguousScopes(testContext *testing.T) {
	tests := []struct {
		name      string
		configure func(*Association)
	}{
		{"multiple ASs", func(association *Association) {
			setInventoryRoutingContexts(&association.cfg.ApplicationServers, params.NewRoutingContext(1, 2))
		}},
		{"same RC different network", func(association *Association) {
			other := association.cfg.ApplicationServers[0]
			other.ASKey.NetworkAppearanceSet = true
			other.ASKey.NetworkAppearance = 7
			association.cfg.ApplicationServers = append(association.cfg.ApplicationServers, other)
		}},
		{"contextless sibling", func(association *Association) {
			association.cfg.ApplicationServers = append(association.cfg.ApplicationServers, ASConfig{})
		}},
		{"dynamic sibling", func(association *Association) {
			association.dynamicPeerASKeys[2] = ASKey{RoutingContext: 2, RoutingContextSet: true}
		}},
		{"dynamic same RC different network", func(association *Association) {
			association.dynamicPeerASKeys[1] = ASKey{RoutingContext: 1, RoutingContextSet: true, NetworkAppearance: 7, NetworkAppearanceSet: true}
		}},
		{"contextless inventory", func(association *Association) {
			association.cfg.ApplicationServers = nil
		}},
		{"explicit contextless AS", func(association *Association) {
			association.cfg.ApplicationServers = []ASConfig{{}}
		}},
		{"implicit contextless AS beside dynamic AS", func(association *Association) {
			association.cfg.SetApplicationServers()
			association.dynamicPeerASKeys[1] = ASKey{RoutingContext: 1, RoutingContextSet: true}
		}},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			association, _ := newTestConn(testContext, StateASPActive, RoleASP)
			setInventoryRoutingContexts(&association.cfg.ApplicationServers, params.NewRoutingContext(1))
			association.cfg.TAck = time.Hour
			test.configure(association)
			association.noteRoutingContextsAcked(params.NewRoutingContext(1))
			association.startTAck(messages.NewAspInactive(params.NewRoutingContext(1), nil), requestAspInactive)
			if err := association.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil)); !errors.Is(err, ErrMissingRoutingContext) {
				testContext.Fatalf("ambiguous omission: %v, want ErrMissingRoutingContext", err)
			}
			if association.pendingTAck() != 1 || !association.routingContextAcked(1) {
				testContext.Fatal("ambiguous acknowledgement changed pending request or traffic state")
			}
		})
	}
}

func TestOmittedInactiveAckPreservesOtherPendingScopes(testContext *testing.T) {
	association, _ := newTestConn(testContext, StateASPActive, RoleASP)
	association.cfg.TAck = time.Hour
	association.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	association.startTAck(messages.NewAspInactive(params.NewRoutingContext(1), nil), requestAspInactive)
	association.startTAck(messages.NewAspInactive(params.NewRoutingContext(2), nil), requestAspInactive)
	if err := association.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil)); !errors.Is(err, ErrMissingRoutingContext) {
		testContext.Fatalf("multiple pending requests: %v", err)
	}
	if association.pendingTAck() != 2 || !association.routingContextAcked(1) || !association.routingContextAcked(2) {
		testContext.Fatal("ambiguous acknowledgement changed multiple pending scopes")
	}
	if err := association.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(1), nil)); err != nil {
		testContext.Fatal(err)
	}
	if err := association.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil)); !errors.Is(err, ErrMissingRoutingContext) {
		testContext.Fatalf("remaining request on multiple-AS association: %v", err)
	}
	if association.pendingTAck() != 1 || association.routingContextAcked(1) || !association.routingContextAcked(2) {
		testContext.Fatal("omission completed the remaining unidentified scope")
	}
}

func TestOmittedInactiveAckPreservesPartialAcknowledgement(testContext *testing.T) {
	association, _ := newTestConn(testContext, StateASPActive, RoleASP)
	association.cfg.TAck = time.Hour
	association.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	association.startTAck(messages.NewAspInactive(params.NewRoutingContext(1, 2), nil), requestAspInactive)
	if err := association.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(1), nil)); err != nil {
		testContext.Fatal(err)
	}
	if err := association.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil)); !errors.Is(err, ErrMissingRoutingContext) {
		testContext.Fatalf("partially acknowledged request: %v", err)
	}
	if association.pendingTAck() != 1 || !association.routingContextAcked(2) {
		testContext.Fatal("omission completed a partially acknowledged multi-AS request")
	}
}

func TestOmittedInactiveAckPreservesRestartBoundary(testContext *testing.T) {
	association, _ := newTestConn(testContext, StateASPActive, RoleASP)
	setInventoryRoutingContexts(&association.cfg.ApplicationServers, params.NewRoutingContext(1))
	association.cfg.TAck = time.Hour
	association.startTAck(messages.NewAspInactive(params.NewRoutingContext(1), nil), requestAspInactive)
	association.resetTAckEpoch()
	association.noteRoutingContextsAcked(params.NewRoutingContext(1))
	if err := association.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil)); err == nil {
		testContext.Fatal("stale acknowledgement accepted after restart")
	}
	if !association.routingContextAcked(1) {
		testContext.Fatal("stale acknowledgement changed traffic state")
	}
}

func TestOmittedActiveAckStillRequiresExplicitRoutingContext(testContext *testing.T) {
	association, _ := newTestConn(testContext, StateASPInactive, RoleASP)
	association.noteNoRoutingContextsAcked()
	setInventoryRoutingContexts(&association.cfg.ApplicationServers, params.NewRoutingContext(1))
	association.cfg.TAck = time.Hour
	association.startTAck(messages.NewAspActive(nil, params.NewRoutingContext(1), nil), requestAspActive)
	if err := association.handleAspActiveAck(messages.NewAspActiveAck(nil, nil, nil)); !errors.Is(err, ErrMissingRoutingContext) {
		testContext.Fatalf("Active Ack without required explicit RC: %v", err)
	}
	if association.pendingTAck() != 1 || association.routingContextAcked(1) {
		testContext.Fatal("invalid Active Ack completed activation")
	}
}

func TestOmittedInactiveAckRetainsUnscopedRequestSupport(testContext *testing.T) {
	association, _ := newTestConn(testContext, StateASPActive, RoleASP)
	association.cfg.TAck = time.Hour
	association.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	association.startTAck(messages.NewAspInactive(nil, nil), requestAspInactive)
	if err := association.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil)); err != nil {
		testContext.Fatal(err)
	}
	if association.pendingTAck() != 0 || association.routingContextAcked(1) || association.routingContextAcked(2) {
		testContext.Fatal("unscoped request was not completed")
	}
}

func TestOmittedInactiveAckUsesOnlyLocalDynamicInventory(testContext *testing.T) {
	association, _ := newDoubleExchangeIPSPForTest(testContext)
	association.cfg.TAck = time.Hour
	association.setIPSPState(IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
	association.noteRoutingContextsAcked(params.NewRoutingContext(11))
	association.dynamicLocalASKeys[33] = ASKey{RoutingContext: 33, RoutingContextSet: true}
	association.startTAck(messages.NewAspInactive(params.NewRoutingContext(11), nil), requestAspInactive)
	if err := association.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil)); !errors.Is(err, ErrMissingRoutingContext) {
		testContext.Fatalf("ambiguous local dynamic AS: %v", err)
	}
	delete(association.dynamicLocalASKeys, 33)
	association.dynamicPeerASKeys[44] = ASKey{RoutingContext: 44, RoutingContextSet: true}
	if err := association.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil)); err != nil {
		testContext.Fatalf("unrelated peer dynamic AS blocked local acknowledgement: %v", err)
	}
	if association.pendingTAck() != 0 || association.State() != StateASPActive {
		testContext.Fatal("local acknowledgement changed the peer direction")
	}
}

func FuzzInactiveAckRoutingContext(fuzzContext *testing.F) {
	fuzzContext.Add([]byte{}, false)
	fuzzContext.Add([]byte{}, true)
	fuzzContext.Add([]byte{0, 0, 0, 1}, true)
	fuzzContext.Add([]byte{0, 0, 0, 2}, true)
	fuzzContext.Add([]byte{0, 0, 1}, true)
	fuzzContext.Fuzz(func(testContext *testing.T, value []byte, present bool) {
		if len(value) > 64 {
			return
		}
		association, _ := newTestConn(testContext, StateASPActive, RoleASP)
		setInventoryRoutingContexts(&association.cfg.ApplicationServers, params.NewRoutingContext(1))
		association.cfg.TAck = time.Hour
		association.noteRoutingContextsAcked(params.NewRoutingContext(1))
		association.startTAck(messages.NewAspInactive(params.NewRoutingContext(1), nil), requestAspInactive)
		var routingContext *params.Param
		valid := !present
		if present {
			routingContext = params.NewRoutingContext(1)
			routingContext.Data = append([]byte(nil), value...)
			routingContext.Length = uint16(len(value) + 4)
			valid = len(value) > 0 && len(value)%4 == 0
			for offset := 0; valid && offset < len(value); offset += 4 {
				valid = value[offset] == 0 && value[offset+1] == 0 && value[offset+2] == 0 && value[offset+3] == 1
			}
		}
		err := association.handleAspInactiveAck(messages.NewAspInactiveAck(routingContext, nil))
		if valid {
			if err != nil || association.pendingTAck() != 0 || association.routingContextAcked(1) {
				testContext.Fatalf("valid acknowledgement: err=%v pending=%d", err, association.pendingTAck())
			}
		} else if err == nil || association.pendingTAck() != 1 || !association.routingContextAcked(1) {
			testContext.Fatalf("invalid acknowledgement changed procedure: err=%v pending=%d", err, association.pendingTAck())
		}
	})
}

func TestOmittedInactiveAckIPSPDirections(testContext *testing.T) {
	association, _ := newDoubleExchangeIPSPForTest(testContext)
	association.cfg.TAck = time.Hour
	association.setIPSPState(IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
	association.noteRoutingContextsAcked(params.NewRoutingContext(11))
	association.startTAck(messages.NewAspInactive(params.NewRoutingContext(11), nil), requestAspInactive)
	if err := association.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil)); err != nil {
		testContext.Fatalf("local-direction omitted ACK: %v", err)
	}
	if association.pendingTAck() != 0 || association.localIPSPStateValue() != StateASPInactive || association.State() != StateASPActive {
		testContext.Fatal("ACK did not deactivate only the local IPSP direction")
	}
}

func TestOmittedInactiveAckIPSPSingleExchange(testContext *testing.T) {
	association, _ := newSingleExchangeIPSPForTest(testContext, StateASPActive)
	association.cfg.TAck = time.Hour
	association.startTAck(messages.NewAspInactive(params.NewRoutingContext(1), nil), requestAspInactive)
	if err := association.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil)); err != nil {
		testContext.Fatal(err)
	}
	if association.pendingTAck() != 0 || association.State() != StateASPInactive {
		testContext.Fatal("single-exchange IPSP did not complete deactivation")
	}
}

func TestOmittedInactiveAckConcurrentDynamicInventory(testContext *testing.T) {
	association, _ := newTestConn(testContext, StateASPActive, RoleASP)
	setInventoryRoutingContexts(&association.cfg.ApplicationServers, params.NewRoutingContext(1))
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for iteration := 0; iteration < 1000; iteration++ {
			association.muDynamicASKeys.Lock()
			association.dynamicPeerASKeys[2] = ASKey{RoutingContext: 2, RoutingContextSet: true}
			association.muDynamicASKeys.Unlock()
			association.muDynamicASKeys.Lock()
			delete(association.dynamicPeerASKeys, 2)
			association.muDynamicASKeys.Unlock()
		}
	}()
	defer func() { <-finished }()
	for iteration := 0; iteration < 1000; iteration++ {
		if resolved := association.unambiguousInactiveAckRoutingContext(); resolved != nil {
			if contexts := resolved.RoutingContexts(); len(contexts) != 1 || contexts[0] != 1 {
				testContext.Fatalf("resolved unrelated scope: %v", contexts)
			}
		}
	}
}

func TestOmittedInactiveAckRepeatedAcrossDynamicMembership(testContext *testing.T) {
	for _, initiallyDynamic := range []bool{false, true} {
		testContext.Run(map[bool]string{false: "AS added", true: "AS removed"}[initiallyDynamic], func(testContext *testing.T) {
			association, _ := newTestConn(testContext, StateASPActive, RoleASP)
			setInventoryRoutingContexts(&association.cfg.ApplicationServers, params.NewRoutingContext(1))
			association.cfg.TAck = time.Hour
			dynamicKey := ASKey{RoutingContext: 2, RoutingContextSet: true}
			requestContext := params.NewRoutingContext(1)
			if initiallyDynamic {
				association.dynamicPeerASKeys[2] = dynamicKey
				requestContext = nil
			}
			association.startTAck(messages.NewAspInactive(requestContext, nil), requestAspInactive)
			if err := association.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil)); err != nil {
				testContext.Fatal(err)
			}
			if initiallyDynamic {
				delete(association.dynamicPeerASKeys, 2)
			} else {
				association.dynamicPeerASKeys[2] = dynamicKey
			}
			association.noteRoutingContextsAcked(params.NewRoutingContext(1))
			if err := association.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil)); err != nil {
				testContext.Fatalf("repeated ACK after membership change: %v", err)
			}
			if !association.routingContextAcked(1) {
				testContext.Fatal("repeated ACK reversed activation after membership change")
			}
		})
	}
}

func TestOmittedInactiveAckPendingRequestPrecedesAmbiguousReplay(testContext *testing.T) {
	association, _ := newTestConn(testContext, StateASPActive, RoleASP)
	setInventoryRoutingContexts(&association.cfg.ApplicationServers, params.NewRoutingContext(1))
	association.cfg.TAck = time.Hour
	association.startTAck(messages.NewAspInactive(params.NewRoutingContext(1), nil), requestAspInactive)
	if err := association.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil)); err != nil {
		testContext.Fatal(err)
	}
	association.dynamicPeerASKeys[2] = ASKey{RoutingContext: 2, RoutingContextSet: true}
	association.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	association.startTAck(messages.NewAspInactive(params.NewRoutingContext(1), nil), requestAspInactive)
	if err := association.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil)); !errors.Is(err, ErrMissingRoutingContext) {
		testContext.Fatalf("ambiguous acknowledgement with a new pending request: %v, want ErrMissingRoutingContext", err)
	}
	if association.pendingTAck() != 1 || !association.routingContextAcked(1) || !association.routingContextAcked(2) {
		testContext.Fatal("ambiguous acknowledgement changed pending request or traffic state")
	}
}

func TestOmittedInactiveAckDeduplicatesStaticAndDynamicAS(testContext *testing.T) {
	for _, doubleExchange := range []bool{false, true} {
		testContext.Run(map[bool]string{false: "ASP", true: "IPSP double exchange"}[doubleExchange], func(testContext *testing.T) {
			var association *Association
			if doubleExchange {
				association, _ = newDoubleExchangeIPSPForTest(testContext)
				association.setIPSPState(IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
			} else {
				association, _ = newTestConn(testContext, StateASPActive, RoleASP)
				setInventoryRoutingContexts(&association.cfg.ApplicationServers, params.NewRoutingContext(1))
			}
			association.cfg.TAck = time.Hour
			key := association.applicationServerInventory(true)[0].ASKey
			if doubleExchange {
				association.dynamicLocalASKeys[key.RoutingContext] = key
			} else {
				association.dynamicPeerASKeys[key.RoutingContext] = key
			}
			association.noteRoutingContextsAcked(params.NewRoutingContext(key.RoutingContext))
			association.startTAck(messages.NewAspInactive(params.NewRoutingContext(key.RoutingContext), nil), requestAspInactive)
			if err := association.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil)); err != nil {
				testContext.Fatalf("identical static and dynamic ASKey: %v", err)
			}
			if association.pendingTAck() != 0 || association.routingContextAcked(key.RoutingContext) {
				testContext.Fatal("sole AS was not deactivated")
			}
		})
	}
}
