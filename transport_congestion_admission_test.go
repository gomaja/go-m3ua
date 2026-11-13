package m3ua

import (
	"errors"
	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"sync"
	"testing"
	"time"
)

func TestIPSPCongestionRechecksWithdrawalAtWriter(t *testing.T) {
	for _, tc := range []struct{ sibling, library bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		sibling, library := tc.sibling, tc.library
		name := "direction"
		if sibling {
			name = "routing-context"
		}
		if library {
			name += "-library"
		}
		t.Run(name, func(t *testing.T) {
			cfg := newDoubleExchangeAssociationConfigForTest()
			if sibling {
				cfg.IPSP.TrafficToLocal.ApplicationServers = append(cfg.IPSP.TrafficToLocal.ApplicationServers, ASConfig{ASKey: *staticASKey(10, 12), TrafficMode: params.TrafficModeLoadshare})
			}
			c, _ := newDoubleExchangeIPSPWithConfigForTest(t, cfg)
			c.cfg.TAck = time.Hour
			c.setIPSPState(IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
			contexts := []uint32{11}
			if sibling {
				contexts = append(contexts, 12)
			}
			c.noteRoutingContextsAcked(params.NewRoutingContext(contexts...))
			c.noteRoutingContextsActive([]uint32{22})
			if err := c.DeactivateRoutingContexts(11); err != nil {
				t.Fatal(err)
			}
			transport := newFullSendBuffer()
			transport.firstErr = nil
			c.signalWriter = nil
			c.controlTransport = transport
			c.transportCloser = transport.close
			c.sendMu.Lock()
			var once sync.Once
			unlock := func() { once.Do(c.sendMu.Unlock) }
			defer unlock()
			written := make(chan error, 1)
			go func() {
				if library {
					_, err := c.writeControl(messages.NewSignallingCongestion(params.NewNetworkAppearance(10), params.NewRoutingContext(11), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(1), nil))
					written <- err
					return
				}
				written <- c.SignallingCongestion(SignallingCongestionRequest{Scope: WireScope{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContexts: []uint32{11}, RoutingContextSet: true}, Destinations: []PointCodeRange{{PointCode: 100}}, CongestionLevel: 1, CongestionLevelSet: true})
			}()
			if !waitFor(func() bool {
				if c.localIPSPSSNMDeliveryMu.TryLock() {
					c.localIPSPSSNMDeliveryMu.Unlock()
					return false
				}
				return true
			}, time.Second) {
				t.Fatal("typed SCON did not acquire its directional barrier")
			}
			withdrawn := make(chan error, 1)
			go func() {
				withdrawn <- c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(11), nil))
			}()
			if !waitFor(func() bool { return !c.routingContextAcked(11) }, time.Second) {
				t.Fatal("withdrawal did not commit scope before waiting for the barrier")
			}
			unlock()
			select {
			case err := <-written:
				want := ErrNotEstablished
				if sibling {
					want = ErrRoutingContextNotActive
				}
				if !errors.Is(err, want) {
					t.Errorf("typed SCON after withdrawal: %v want %v", err, want)
				}
			case <-time.After(time.Second):
				t.Fatal("typed writer did not finish")
			}
			select {
			case err := <-withdrawn:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("withdrawal did not finish")
			}
			if got := overflowTransportAttempts(transport); got != 0 {
				t.Fatalf("typed SCON submitted %d obsolete frames", got)
			}
		})
	}
}

func TestIPSPCongestionWriterDoesNotWaitForStateTransition(t *testing.T) {
	c, _ := newDoubleExchangeIPSPForTest(t)
	c.setIPSPState(IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
	c.noteRoutingContextsAcked(params.NewRoutingContext(11))
	c.noteRoutingContextsActive([]uint32{22})
	transport := newFullSendBuffer()
	transport.firstErr = nil
	c.signalWriter = nil
	c.controlTransport = transport
	c.transportCloser = transport.close
	c.sendMu.Lock()
	var once sync.Once
	unlock := func() { once.Do(c.sendMu.Unlock) }
	defer unlock()
	done := make(chan error, 1)
	go func() {
		done <- c.SignallingCongestion(SignallingCongestionRequest{Scope: WireScope{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContexts: []uint32{11}, RoutingContextSet: true}, Destinations: []PointCodeRange{{PointCode: 100}}})
	}()
	if !waitFor(func() bool {
		if c.localIPSPSSNMDeliveryMu.TryLock() {
			c.localIPSPSSNMDeliveryMu.Unlock()
			return false
		}
		return true
	}, time.Second) {
		t.Fatal("writer did not acquire barrier")
	}
	c.muState.Lock()
	defer c.muState.Unlock()
	unlock()
	select {
	case err := <-done:
		if !errors.Is(err, ErrNotEstablished) {
			t.Fatalf("busy state admission: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("congestion writer holds sendMu while waiting for state transition")
	}
	// A state entry holding muState must still be able to write its ASP Up.
	if _, err := c.writeControl(messages.NewAspUp(nil, nil)); err != nil {
		t.Fatal(err)
	}
}
