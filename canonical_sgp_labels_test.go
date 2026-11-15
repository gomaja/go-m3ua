package m3ua

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

const canonicalLabelPointCode = uint32(0x123456)

type canonicalLabelFixture struct {
	endpoint *Endpoint
	now      time.Time
	keys     [2]ASKey
	peers    [2]*Association
	data     [2]*mtpTransferCapture
	raw      [2]int
	controls [2][]messages.M3UA
}

func newCanonicalLabelFixture(t *testing.T, second ASKey, validity, auditing bool) *canonicalLabelFixture {
	t.Helper()
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionBroadcast)
	config.SignallingGateways[0].SGPs[1].ApplicationServers = append([]RemoteASConfig(nil), config.SignallingGateways[0].SGPs[1].ApplicationServers...)
	config.SignallingGateways[0].SGPs[1].ApplicationServers[0].ASKey = &second
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config,
		Congestion:          &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 3}},
		TransportCongestion: &TransportCongestionConfig{Disabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	r := &canonicalLabelFixture{endpoint: e, now: time.Now(), keys: [2]ASKey{*staticASKey(7, 1), second}}
	// Install deterministic policies before attaching sources, without starting
	// a real-clock worker. Every automation pass below is driven explicitly.
	e.ssnm.now = func() time.Time { return r.now }
	if validity {
		e.ssnm.validity = 10 * time.Second
	}
	if auditing {
		e.ssnm.auditing = &DestinationAuditConfig{Period: time.Second, MaxPending: 256, MaxMessagesPerSecond: 10}
	}
	for i, key := range r.keys {
		a, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, key.RoutingContext)
		a.cfg.ApplicationServers = []ASConfig{{ASKey: key, TrafficMode: params.TrafficModeLoadshare}}
		a.cfg.PeerSGP = &SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: config.SignallingGateways[0].SGPs[i].ID}
		a.noteRoutingContextsAcked(params.NewRoutingContext(key.RoutingContext))
		r.peers[i], r.data[i] = a, &mtpTransferCapture{}
		a.dataWriter = r.data[i].write
		a.signalWriter = func(message messages.M3UA) (int, error) {
			if _, data := message.(*messages.Data); data {
				r.raw[i]++
				frame, marshalErr := message.MarshalBinary()
				return len(frame), marshalErr
			}
			r.controls[i] = append(r.controls[i], message)
			return 0, nil
		}
		if !e.trackAssociation(a) {
			t.Fatal("attach alias", key)
		}
		t.Cleanup(func() { _ = a.Close() })
	}
	return r
}

func (r *canonicalLabelFixture) receive(t *testing.T, peer int, kind SSNMReportKind, level uint8) {
	t.Helper()
	key := r.keys[peer]
	var na *params.Param
	if key.NetworkAppearanceSet {
		na = params.NewNetworkAppearance(key.NetworkAppearance)
	}
	rc, apc := params.NewRoutingContext(key.RoutingContext), params.NewAffectedPointCode(canonicalLabelPointCode)
	var err error
	switch kind {
	case SSNMDestinationAvailableReport:
		err = r.peers[peer].handleDestinationAvailable(messages.NewDestinationAvailable(na, rc, apc, nil))
	case SSNMDestinationUnavailableReport:
		err = r.peers[peer].handleDestinationUnavailable(messages.NewDestinationUnavailable(na, rc, apc, nil))
	case SSNMSignallingCongestionReport:
		err = r.peers[peer].handleSignallingCongestion(messages.NewSignallingCongestion(na, rc, apc, nil, params.NewCongestionIndications(level), nil))
	default:
		t.Fatal("unexpected fixture report", kind)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func (r *canonicalLabelFixture) observed(t *testing.T, peer int) aspSelectionStatus {
	t.Helper()
	routes := r.endpoint.aspRoutes
	routes.mu.RLock()
	defer routes.mu.RUnlock()
	sgp := routes.config.signallingGateways[0].sgps[peer]
	value, found := routes.observedSGPCandidateLocked(sgp, SGPIdentity{"sg-a", sgp.id}, "sccp-a", canonicalLabelPointCode)
	if !found {
		t.Fatal("alias has no observed canonical candidate", peer)
	}
	return value.status
}

func (r *canonicalLabelFixture) send(t *testing.T, peer int, priority uint8, serialized bool) error {
	t.Helper()
	key := r.keys[peer]
	payload := transferProtocolData(canonicalLabelPointCode, 1, []byte("canonical labels"))
	payload.MessagePriority = priority
	if !serialized {
		_, err := r.peers[peer].WriteData(DataRequest{AS: key, ProtocolData: *payload})
		return err
	}
	var na *params.Param
	if key.NetworkAppearanceSet {
		na = params.NewNetworkAppearance(key.NetworkAppearance)
	}
	_, err := r.peers[peer].WriteSignal(messages.NewData(na, params.NewRoutingContext(key.RoutingContext), params.NewProtocolData(payload.OriginatingPointCode, payload.DestinationPointCode, payload.ServiceIndicator, payload.NetworkIndicator, payload.MessagePriority, payload.SignallingLinkSelection, payload.Data), nil))
	return err
}

func canonicalLabelCases(t *testing.T, run func(*testing.T, ASKey, bool, bool)) {
	t.Helper()
	for _, key := range []ASKey{*staticASKey(8, 2), *staticASKey(0, 2), {RoutingContext: 2, RoutingContextSet: true}} {
		for _, validity := range []bool{false, true} {
			for _, auditing := range []bool{false, true} {
				t.Run(fmt.Sprintf("NA=%d/set=%v/validity=%v/auditing=%v", key.NetworkAppearance, key.NetworkAppearanceSet, validity, auditing), func(t *testing.T) { run(t, key, validity, auditing) })
			}
		}
	}
}

func TestCanonicalSGPLabelsShareAvailabilityForTransfer(t *testing.T) {
	canonicalLabelCases(t, func(t *testing.T, key ASKey, validity, auditing bool) {
		r := newCanonicalLabelFixture(t, key, validity, auditing)
		for _, reporter := range []int{0, 1, 0} {
			r.receive(t, reporter, SSNMDestinationAvailableReport, 0)
			before := [2]int{r.data[0].count(), r.data[1].count()}
			result, err := r.endpoint.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(canonicalLabelPointCode, 1, nil)})
			if err != nil || len(result.SuccessfulPaths) != 2 {
				t.Fatalf("canonical availability lost an aliased SGP: paths=%d err=%v", len(result.SuccessfulPaths), err)
			}
			for peer := range r.peers {
				if r.data[peer].count() != before[peer]+1 {
					t.Fatal("canonical transfer omitted peer", peer)
				}
				message, _ := r.data[peer].lastData(t)
				if message.RoutingContext == nil || message.RoutingContext.RoutingContext() != r.keys[peer].RoutingContext || (message.NetworkAppearance != nil) != r.keys[peer].NetworkAppearanceSet || message.NetworkAppearance != nil && message.NetworkAppearance.NetworkAppearance() != r.keys[peer].NetworkAppearance {
					t.Fatal("canonical transfer rewrote peer-local labels", peer, message)
				}
			}
		}
		r.receive(t, 1, SSNMDestinationUnavailableReport, 0)
		before := [2]int{r.data[0].count(), r.data[1].count()}
		for peer := range r.peers {
			if status := r.observed(t, peer); !status.availabilitySet || status.availability != DestinationUnavailable {
				t.Fatal("canonical DUNA did not reach alias", peer, status)
			}
		}
		if _, err := r.endpoint.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(canonicalLabelPointCode, 1, nil)}); !errors.Is(err, ErrNoMTPRoute) {
			t.Fatal("canonical DUNA left a transfer path", err)
		}
		for peer := range r.peers {
			if r.data[peer].count() != before[peer] {
				t.Fatal("canonical DUNA allowed DATA submission", peer)
			}
		}
		r.receive(t, 0, SSNMDestinationAvailableReport, 0)
		result, err := r.endpoint.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(canonicalLabelPointCode, 1, nil)})
		if err != nil || len(result.SuccessfulPaths) != 2 {
			t.Fatal("newer canonical DAVA did not restore both aliases", result, err)
		}
		for peer := range r.peers {
			if r.data[peer].count() != before[peer]+1 {
				t.Fatal("restored alias omitted DATA submission", peer)
			}
		}
	})
}

func TestCanonicalSGPLabelsShareCongestionAndExpiry(t *testing.T) {
	canonicalLabelCases(t, func(t *testing.T, key ASKey, validity, auditing bool) {
		r := newCanonicalLabelFixture(t, key, validity, auditing)
		r.receive(t, 1, SSNMSignallingCongestionReport, 2)
		// A scoped diagnostic query stays filtered; the canonical consumers use
		// the configured SG/AS owner instead of this peer-local view.
		if scoped := r.endpoint.ssnm.destinationKnowledge(canonicalSSNMPartition("sg-a", "as-core"), canonicalLabelPointCode, r.keys[0]); scoped.congested || scoped.receivedUnknown {
			t.Fatal("explicit source-scope query broadened", scoped)
		}
		for peer := range r.peers {
			if status := r.observed(t, peer); !status.congested || status.congestionLevel != 2 || status.receivedUnknown {
				t.Fatal("canonical observation lost sibling congestion", peer, status)
			}
			for _, serialized := range []bool{false, true} {
				before := r.data[peer].count() + r.raw[peer]
				if err := r.send(t, peer, 1, serialized); !errors.Is(err, ErrNoMTPRoute) {
					t.Fatal("direct send bypassed aliased sibling congestion", peer, serialized, err)
				}
				if r.data[peer].count()+r.raw[peer] != before {
					t.Fatal("refused alias DATA reached writer")
				}
				if err := r.send(t, peer, 2, serialized); err != nil {
					t.Fatal("equal-priority alias DATA refused", peer, serialized, err)
				}
				if r.data[peer].count()+r.raw[peer] != before+1 {
					t.Fatal("equal-priority alias DATA did not reach writer", peer, serialized)
				}
			}
		}
		if auditing {
			r.now = r.now.Add(time.Second)
			r.endpoint.runCongestionAutomation(r.now)
			counts := [2]int{}
			for peer, signals := range r.controls {
				for _, message := range signals {
					if audit, ok := message.(*messages.DestinationStateAudit); ok {
						counts[peer]++
						if audit.RoutingContext == nil || audit.RoutingContext.RoutingContext() != r.keys[peer].RoutingContext || (audit.NetworkAppearance != nil) != r.keys[peer].NetworkAppearanceSet || audit.NetworkAppearance != nil && audit.NetworkAppearance.NetworkAppearance() != r.keys[peer].NetworkAppearance {
							t.Fatal("audit rewrote original reporter labels", audit)
						}
					}
				}
			}
			if counts != [2]int{0, 1} {
				t.Fatal("canonical view moved an audit to its sibling", counts)
			}
		}
		if validity {
			r.now = r.now.Add(10 * time.Second)
			r.endpoint.runCongestionAutomation(r.now)
			for peer := range r.peers {
				if status := r.observed(t, peer); status.congested || !status.receivedUnknown {
					t.Fatal("canonical observation hid sibling expiry", peer, status)
				}
				for _, serialized := range []bool{false, true} {
					before := r.data[peer].count() + r.raw[peer]
					if err := r.send(t, peer, 3, serialized); !errors.Is(err, ErrNoMTPRoute) {
						t.Fatal("direct send bypassed aliased expiry uncertainty", peer, serialized, err)
					}
					if r.data[peer].count()+r.raw[peer] != before {
						t.Fatal("expired alias DATA reached writer", peer, serialized)
					}
				}
			}
		}
		r.receive(t, 1, SSNMSignallingCongestionReport, 0)
		for peer := range r.peers {
			if status := r.observed(t, peer); status.congested || status.receivedUnknown {
				t.Fatal("canonical source clear did not reach alias", peer, status)
			}
			for _, serialized := range []bool{false, true} {
				before := r.data[peer].count() + r.raw[peer]
				if err := r.send(t, peer, 0, serialized); err != nil {
					t.Fatal("fresh canonical clear refused", peer, serialized, err)
				}
				if r.data[peer].count()+r.raw[peer] != before+1 {
					t.Fatal("fresh canonical clear did not reach writer", peer, serialized)
				}
			}
		}
		// Reverse the reporter and then clear only its sibling. Independent
		// histories keep the positive source; the disabled-policy scalar keeps
		// main's latest-statement behavior.
		r.receive(t, 0, SSNMSignallingCongestionReport, 2)
		for peer := range r.peers {
			if status := r.observed(t, peer); !status.congested || status.congestionLevel != 2 {
				t.Fatal("reverse alias reporter was not shared", peer, status)
			}
		}
		r.receive(t, 1, SSNMSignallingCongestionReport, 0)
		for peer := range r.peers {
			if status := r.observed(t, peer); status.congested != (validity || auditing) || status.receivedUnknown {
				t.Fatal("alias clear lost source-history policy", peer, status)
			}
			for _, serialized := range []bool{false, true} {
				before := r.data[peer].count() + r.raw[peer]
				err := r.send(t, peer, 1, serialized)
				if validity || auditing {
					if !errors.Is(err, ErrNoMTPRoute) || r.data[peer].count()+r.raw[peer] != before {
						t.Fatal("sibling clear bypassed retained alias pressure", peer, serialized, err)
					}
				} else if err != nil || r.data[peer].count()+r.raw[peer] != before+1 {
					t.Fatal("disabled-history scalar clear refused DATA", peer, serialized, err)
				}
			}
		}
	})
}

func TestCanonicalSGPLabelsKeepDistinctOwnersIndependent(t *testing.T) {
	config := ssnmPeerInventoryConfig()
	config.SignallingGateways[0].SGPs[1].ApplicationServers[0].ASKey = staticASKey(8, 2)
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config,
		Congestion:          &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 3}},
		TransportCongestion: &TransportCongestionConfig{Disabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	e.ssnm.validity = time.Hour
	first := attachSSNMAssociation(t, e, SGPIdentity{"sg-a", "sgp-a1"}, 7, 1, 3)
	alias := attachSSNMAssociation(t, e, SGPIdentity{"sg-a", "sgp-a2"}, 8, 2)
	otherGateway := attachSSNMAssociation(t, e, SGPIdentity{"sg-b", "sgp-b1"}, 7, 1)
	sendSCON(t, first, 7, 1, congestionLevel(2), canonicalLabelPointCode)
	for _, test := range []struct {
		name    string
		peer    *Association
		key     ASKey
		refused bool
	}{
		{"same owner through another label", alias, *staticASKey(8, 2), true},
		{"different AS on the same association", first, *staticASKey(7, 3), false},
		{"same AS name and labels on another SG", otherGateway, *staticASKey(7, 1), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			capture := &mtpTransferCapture{}
			test.peer.dataWriter = capture.write
			_, err := test.peer.WriteData(DataRequest{AS: test.key, ProtocolData: *transferProtocolData(canonicalLabelPointCode, 1, nil)})
			if test.refused {
				if !errors.Is(err, ErrNoMTPRoute) || capture.count() != 0 {
					t.Fatal("canonical congestion did not stay within its owner", err, capture.count())
				}
			} else if err != nil || capture.count() != 1 {
				t.Fatal("canonical congestion leaked to another owner", err, capture.count())
			}
		})
	}
}

func TestCanonicalSGPLabelsExpiryKeepsTransportPressure(t *testing.T) {
	r := newCanonicalLabelFixture(t, *staticASKey(8, 2), true, false)
	setAssociationTransport(t, r.endpoint, r.peers[0], 3)
	r.receive(t, 1, SSNMSignallingCongestionReport, 2)
	r.now = r.now.Add(10 * time.Second)
	r.endpoint.runCongestionAutomation(r.now)
	status := methodTransportStatus(r.peers[0], r.observed(t, 0))
	if !status.congested || status.congestionLevel != 3 || !status.receivedUnknown {
		t.Fatal("aliased expiry lost transport or uncertainty", status)
	}
	for _, serialized := range []bool{false, true} {
		if err := r.send(t, 0, 3, serialized); !errors.Is(err, ErrNoMTPRoute) {
			t.Fatal("transport masked aliased expiry admission", serialized, err)
		}
	}
	r.receive(t, 1, SSNMSignallingCongestionReport, 0)
	status = methodTransportStatus(r.peers[0], r.observed(t, 0))
	if !status.congested || status.congestionLevel != 3 || status.receivedUnknown || r.peers[0].TransportCongestion().Level != 3 {
		t.Fatal("aliased clear changed independent transport", status)
	}
	for _, serialized := range []bool{false, true} {
		if err := r.send(t, 0, 3, serialized); err != nil {
			t.Fatal("equal transport priority refused after current source clear", serialized, err)
		}
	}
}
