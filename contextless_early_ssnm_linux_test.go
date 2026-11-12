// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestContextlessActivationAppliesEarlyDUNAOverSCTP(t *testing.T) {
	exerciseContextlessEarlySSNMOverSCTP(t, false)
}

func TestContextlessActivationDeliversEarlySSNMIndicationsOverSCTP(t *testing.T) {
	exerciseContextlessEarlySSNMOverSCTP(t, true)
}

// RFC 4666 §3.4.1 permits omitted RC. Section 4.5.1 admits DUNA, DRST and
// SCON before Ack, and §4.6 admits restart DAVA. Section 4.5.2 keeps the
// per-SG reports and the aggregate MTP3-User indications distinct.
func exerciseContextlessEarlySSNMOverSCTP(t *testing.T, indications bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	noDelay := true
	listener, err := (&sctp.Config{NoDelay: &noDelay, InitMsg: sctp.InitMsg{OutStreams: 4, MaxInStreams: 4}}).Listen("sctp4", mcAddr(0, "127.0.0.1"))
	if err != nil {
		skipIfSCTPUnsupported(t, err)
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	type observers struct {
		association *Association
		reports     *SSNMSubscription
		mtp         <-chan *MTPIndication
	}
	subscribed := make(chan observers, 1)
	peerResult := make(chan error, 1)
	activated := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	go func() {
		peer, acceptErr := listener.AcceptSCTP()
		if acceptErr != nil {
			peerResult <- acceptErr
			return
		}
		defer func() { _ = peer.Close() }()
		peerResult <- func() error {
			if err := peer.SetDeadline(time.Now().Add(12 * time.Second)); err != nil {
				return err
			}
			send := func(message messages.M3UA) error {
				raw, err := message.MarshalBinary()
				if err != nil {
					return err
				}
				_, err = peer.SendMsg(raw, sctp.SendOptions{Info: &sctp.SndInfo{PPID: M3UAPPID}})
				return err
			}
			var observed observers
			var na *params.Param
			if indications {
				na = params.NewNetworkAppearance(7)
			}
			type reportStep struct {
				message messages.M3UA
				kind    SSNMReportKind
				pc      uint32
			}
			observe := func(step reportStep, early bool) error {
				if err := send(step.message); err != nil {
					return err
				}
				for {
					event, err := observed.reports.Next(ctx)
					if err != nil {
						return err
					}
					if event.Kind != SSNMReportEvent || !event.ReportSet {
						continue
					}
					report := event.Report
					if report.Kind != step.kind || report.Association != observed.association.ID() || report.Source != SSNMPeerReport || report.Scope.RoutingContextSet || len(report.Destinations) != 1 || report.Destinations[0].PointCode != step.pc {
						return fmt.Errorf("LM SSNM report: %+v", report)
					}
					if early {
						if observed.association.State() != StateASPInactive {
							return fmt.Errorf("early report delivered after activation")
						}
						for _, knowledge := range observed.association.endpoint.SSNMKnowledge().Partitions {
							if knowledge.TrafficAuthorized {
								return fmt.Errorf("early report authorized traffic")
							}
						}
						if indications {
							for draining := true; draining; {
								select {
								case indication := <-observed.mtp:
									if indication.Kind == MTPResumeIndication {
										return fmt.Errorf("MTP-RESUME before Ack: %+v", indication)
									}
								default:
									draining = false
								}
							}
						}
					}
					return nil
				}
			}
			for _, step := range []struct {
				class, kind uint8
				reply       messages.M3UA
			}{
				{messages.MsgClassASPSM, messages.MsgTypeAspUp, messages.NewAspUpAck(nil, nil)},
				{messages.MsgClassASPTM, messages.MsgTypeAspActive, messages.NewAspActiveAck(params.NewTrafficModeType(params.TrafficModeLoadshare), nil, nil)},
			} {
				raw, _, err := peer.ReadMsg(65536)
				if err != nil {
					return err
				}
				message, err := messages.Parse(raw)
				if err != nil {
					return err
				}
				if message.MessageClass() != step.class || message.MessageType() != step.kind {
					return fmt.Errorf("unexpected handshake: %s", message.MessageTypeName())
				}
				if active, ok := message.(*messages.AspActive); ok {
					if active.RoutingContext != nil {
						return fmt.Errorf("ASP Active carried RC")
					}
					observed = <-subscribed
					reports := []reportStep{{messages.NewDestinationUnavailable(na, nil, apc(100), nil), SSNMDestinationUnavailableReport, 100}}
					if indications {
						reports = append(reports,
							reportStep{messages.NewDestinationRestricted(na, nil, apc(101), nil), SSNMDestinationRestrictedReport, 101},
							reportStep{messages.NewSignallingCongestion(na, nil, apc(102), nil, params.NewCongestionIndications(2), nil), SSNMSignallingCongestionReport, 102},
							reportStep{messages.NewDestinationUnavailable(na, nil, apc(103), nil), SSNMDestinationUnavailableReport, 103},
							reportStep{messages.NewDestinationAvailable(na, nil, apc(103), nil), SSNMDestinationAvailableReport, 103})
					}
					for _, report := range reports {
						if err := observe(report, true); err != nil {
							return err
						}
					}
				}
				if err := send(step.reply); err != nil {
					return err
				}
			}
			if indications {
				select {
				case <-activated:
				case <-ctx.Done():
					return ctx.Err()
				}
				// Check the early reports before post-Ack updates can mask lost state.
				if retainedAvailability(observed.association, 100) != DestinationUnavailable || retainedAvailability(observed.association, 101) != DestinationRestricted || retainedAvailability(observed.association, 103) != DestinationAvailable || retainedDestinationState(observed.association, 102).Congestion.Level != 2 {
					return fmt.Errorf("activation lost early DUNA/DRST/SCON/DAVA state")
				}
				// These post-Ack transitions also prove PAUSE/RESUME/STATUS delivery;
				// pre-Ack reports alone must not resume traffic through an inactive AS.
				for _, step := range []reportStep{
					{messages.NewDestinationAvailable(na, nil, apc(100), nil), SSNMDestinationAvailableReport, 100},
					{messages.NewDestinationUnavailable(na, nil, apc(100), nil), SSNMDestinationUnavailableReport, 100},
					{messages.NewSignallingCongestion(na, nil, apc(102), nil, params.NewCongestionIndications(0), nil), SSNMSignallingCongestionReport, 102},
					{messages.NewSignallingCongestion(na, nil, apc(102), nil, params.NewCongestionIndications(2), nil), SSNMSignallingCongestionReport, 102},
					{messages.NewDestinationRestricted(na, nil, apc(103), nil), SSNMDestinationRestrictedReport, 103},
					{messages.NewDestinationAvailable(na, nil, apc(103), nil), SSNMDestinationAvailableReport, 103},
				} {
					if err := observe(step, false); err != nil {
						return err
					}
				}
			}
			// Same-stream BEAT/Ack is a barrier: a DUNA ERR would precede this Ack.
			if err := send(messages.NewHeartbeat(nil)); err != nil {
				return err
			}
			raw, _, err := peer.ReadMsg(65536)
			if err != nil {
				return err
			}
			message, err := messages.Parse(raw)
			if err != nil {
				return err
			}
			if _, ok := message.(*messages.HeartbeatAck); !ok {
				return fmt.Errorf("after activation: %s, want Heartbeat Ack without ERR", message.MessageTypeName())
			}
			return nil
		}()
		<-release
	}()
	var config *ASPConfig
	if indications {
		config = validASPConfig()
		useSignallingGateways(config, "sg-a")
		config.SignallingGateways[0].SGPs[0].ApplicationServers[0].ASKey = &ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true}
		config.Routing.MTPRoutes[0].DestinationPointCode, config.Routing.MTPRoutes[0].Mask = 100, 2
	}
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	cfg := NewAssociationConfig().SetApplicationServers(ASConfig{TrafficMode: params.TrafficModeLoadshare})
	if indications {
		setInventoryNetworkAppearance(&cfg.ApplicationServers, params.NewNetworkAppearance(7))
		cfg.PeerSGP = &SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	}
	cfg.HeartbeatInfo = &HeartbeatInfo{Enabled: false}
	cfg.TAck = time.Hour
	var states <-chan State
	var management <-chan *ManagementIndication
	cfg.Subscribe = func(a *Association) {
		states = a.StateChanges()
		management = a.ManagementIndications()
		subscribed <- observers{a, observeSSNM(t, a), endpoint.MTPIndications()}
	}
	a, err := endpoint.Dial(ctx, "m3ua", mcAddr(0, "127.0.0.1"), listener.Addr().(*sctp.Addr), cfg)
	if err != nil {
		select {
		case peerErr := <-peerResult:
			t.Fatalf("Dial: %v; peer: %v", err, peerErr)
		case <-time.After(time.Second):
			t.Fatal(err)
		}
	}
	close(activated)
	t.Cleanup(func() { _ = a.Close() })
	select {
	case err := <-peerResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if a.State() != StateASPActive || retainedAvailability(a, 100) != DestinationUnavailable {
		t.Fatalf("state=%v destination=%v", a.State(), retainedAvailability(a, 100))
	}
	if !indications {
		return
	}
	if retainedAvailability(a, 101) != DestinationRestricted || retainedAvailability(a, 103) != DestinationAvailable || retainedDestinationState(a, 102).Congestion.Level != 2 {
		t.Fatal("early DRST/SCON/DAVA state not retained")
	}
	seenStates := map[State]bool{}
	for !seenStates[StateASPInactive] || !seenStates[StateASPActive] {
		select {
		case state, open := <-states:
			if !open {
				t.Fatal("LM state confirms closed")
			}
			seenStates[state] = true
		case <-ctx.Done():
			t.Fatalf("LM state confirms: %v", seenStates)
		}
	}
	for draining := true; draining; {
		select {
		case indication := <-management:
			if indication.Kind == ManagementError {
				t.Fatalf("LM error: %+v", indication)
			}
		default:
			draining = false
		}
	}
	kinds := map[MTPIndicationKind]bool{}
	for draining := true; draining; {
		select {
		case indication := <-endpoint.MTPIndications():
			if indication.ResyncRequired {
				t.Fatal("MTP continuity lost")
			}
			kinds[indication.Kind] = true
		default:
			draining = false
		}
	}
	for _, kind := range []MTPIndicationKind{MTPPauseIndication, MTPResumeIndication, MTPStatusIndication} {
		if !kinds[kind] {
			t.Fatalf("missing %s delivery: %v", kind, kinds)
		}
	}
}
