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

func TestSCONFanoutProfileRefusalKeepsAssociationUsable(t *testing.T) {
	e, first, refused, second, accepted := multiAssociationDialedSGPFixture(t)
	first.cfg.CongestionProfiles = []NetworkCongestionProfile{{NetworkAppearance: 7, NetworkAppearanceSet: true,
		Profile: CongestionProfile{Method: CongestionNationalWithoutPriorities, MaxLevel: 1}}}
	first.notificationQueue = make(chan mandatoryControl, 8)
	second.notificationQueue = make(chan mandatoryControl, 8)
	err := e.SignallingCongestion(SignallingCongestionRequest{Scope: testWireScope(7, true, 1, 2),
		Destinations: []PointCodeRange{{PointCode: 100}}, CongestionLevel: 2, CongestionLevelSet: true})
	var delivery *SSNMDeliveryError
	if !errors.As(err, &delivery) || len(delivery.Failed) != 1 || delivery.Failed[0].Association != first.ID() || !errors.Is(delivery.Failed[0].Cause, ErrInvalidParameterValue) {
		t.Fatalf("per-target profile refusal = %+v, %v", delivery, err)
	}
	if len(ssnmMessages(refused.snapshot())) != 0 || len(ssnmMessages(accepted.snapshot())) != 1 {
		t.Fatal("fan-out did not preserve the admitted target and omit the refused target")
	}
	// A subsequent ordered control joins the writer after the refusal. Its
	// completion proves both transport lifetime and queue service survive.
	err = first.writeMandatoryControls([]messages.M3UA{messages.NewNotify(params.NewStatus(params.AsStateActive), nil, nil, nil)}, false, true)
	if err != nil {
		t.Fatalf("profile refusal ended a healthy control writer: %v", err)
	}
	select {
	case <-first.Done():
		t.Fatalf("profile refusal closed association: %v", first.Err())
	default:
	}
}

func TestLocalSCONRefusalIsNotAPeerProtocolError(t *testing.T) {
	c, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	c.noteRoutingContextsAcked(params.NewRoutingContext(1))
	c.endpoint = methodEndpointForTest(t, RoleASP, uint8(CongestionInternational), 0)
	_, err := c.writeControl(messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(2), nil))
	if !errors.Is(err, ErrInvalidParameterValue) {
		t.Fatalf("local profile refusal = %v", err)
	}
	if err := c.handleErrors(err); err != nil {
		t.Fatalf("local unsent control became a dispatch failure: %v", err)
	}
	if len(*sent) != 0 {
		t.Fatal("local admission refusal emitted a peer Error")
	}
}

func TestMethodAdmissionPreservesDistributionErrors(t *testing.T) {
	for _, invalid := range []string{"missing", "framing", "management", "stream"} {
		t.Run(invalid, func(t *testing.T) {
			listener, as, c, capture := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(cfg *distributionFixtureConfig) {
				cfg.Congestion = &CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}}
			})
			c.setState(StateASPActive)
			c.noteRoutingContextsActive([]uint32{1})
			as.setASPState(c, StateASPActive, time.Hour)
			message := distributionData(1, 1, "invalid")
			message.ProtocolData = params.NewProtocolData(1, 100, 3, 0, 255, 1, nil)
			var want error
			switch invalid {
			case "missing":
				message.ProtocolData = nil
				want = ErrMissingProtocolData
			case "framing":
				message.ProtocolData.Data = []byte{0}
				want = params.ErrInvalidLength
			case "management":
				message.ProtocolData = params.NewProtocolData(1, 100, 0, 0, 255, 1, nil)
				want = ErrMTPManagementData
			case "stream":
				c.maxMessageStreamID = 0
				want = ErrNoDataStream
			}
			_, err := listener.DistributeData(message)
			if !errors.Is(err, want) {
				t.Fatalf("existing distribution error = %v, want %v", err, want)
			}
			if capture.dataCount() != 0 {
				t.Fatal("already-invalid distributed DATA reached transport")
			}
		})
	}
}

func TestQueueRecoveryCannotPublishForReplacedBinding(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		for _, sameAppearance := range []bool{false, true} {
			t.Run(fmt.Sprintf("recovery_%t/same_appearance_%t", recovery, sameAppearance), func(t *testing.T) {
				c, sent := rkmRequesterAssociation(t, 1, 1)
				c.endpoint = methodEndpointForTest(t, RoleASP, uint8(CongestionNationalWithoutPriorities), 3)
				register := func(na uint32, remoteAS RemoteASID) ASKey {
					t.Helper()
					results, err := c.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{RemoteAS: remoteAS, RoutingKey: testRoutingKey(na, 0x222222, 3)})
					if err != nil || len(results) != 1 || results[0].Status != RegistrationSuccessfullyRegistered {
						t.Fatalf("register: %+v, %v", results, err)
					}
					c.setState(StateASPActive)
					c.noteRoutingContextsAcked(params.NewRoutingContext(1))
					return results[0].ASKey
				}
				old := register(7, "old-as")
				attachOverflowEndpointForTest(t, c)
				_ = c.ManagementIndications()
				c.recvStream.Store(1)
				c.dataChan = make(chan *DataMessage, 1)
				c.errChan = make(chan error, 8)
				// Drive observations explicitly: the recovery admission below is
				// synchronized by a management event and sendMu, never a timer.
				c.dataOverflowWorker = true
				for range 2 {
					deliver(t, c, 1, inboundData(1, "overflow"))
				}
				overflow := firstErr(c)
				if overflow == nil {
					t.Fatal("missing overflow")
				}
				if recovery {
					if err := c.handleErrors(overflow); err != nil {
						t.Fatal(err)
					}
					runOverflowPublicationsForTest(c)
					reports := 0
					for _, m := range *sent {
						if _, ok := m.(*messages.SignallingCongestion); ok {
							reports++
						}
					}
					if reports != 1 {
						t.Fatalf("submitted onset reports=%d", reports)
					}
				}
				if !nextQueueCongestion(t, c).Congested {
					t.Fatal("missing onset")
				}
				c.sendMu.Lock()
				var release sync.Once
				unlock := func() { release.Do(c.sendMu.Unlock) }
				defer unlock()
				finished := make(chan error, 1)
				if recovery {
					<-c.dataChan
					c.muDataOverflow.Lock()
					c.dataOverflowLowSince = time.Now().Add(-2 * dataOverflowDrainDwell)
					c.muDataOverflow.Unlock()
					go func() { c.observeDataQueue(); finished <- nil }()
					if nextQueueCongestion(t, c).Congested {
						t.Fatal("missing recovery admission barrier")
					}
				} else {
					go func() { finished <- c.handleErrors(overflow) }()
				}
				results, err := c.DeregisterApplicationServers(context.Background(), old)
				if err != nil || len(results) != 1 || results[0].Status != DeregistrationSuccessfullyDeregistered {
					t.Fatalf("deregister: %+v, %v", results, err)
				}
				na := uint32(9)
				if sameAppearance {
					na = 7
				}
				register(na, "replacement-as")
				unlock()
				select {
				case err := <-finished:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("recovery writer did not finish")
				}
				runOverflowPublicationsForTest(c)
				for _, message := range *sent {
					if scon, ok := message.(*messages.SignallingCongestion); ok && (!recovery || scon.CongestionIndications != nil && scon.CongestionIndications.CongestionLevel() == 0) {
						t.Fatalf("recovery published for retired binding: %+v", scon)
					}
				}
			})
		}
	}
}

func TestMTPTransferOversizePrecedesCongestionMethodAdmission(t *testing.T) {
	e, _, captures := newASPTransferFixtureReporting(t, oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare), true,
		&CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}})
	request := MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 1, make([]byte, 65520))}
	request.ProtocolData.MessagePriority = 1
	_, err := e.MTPTransfer(request)
	if !errors.Is(err, ErrProtocolDataTooLarge) {
		t.Fatalf("oversized local-user DATA error = %v, want ErrProtocolDataTooLarge", err)
	}
	for _, capture := range captures {
		if capture.count() != 0 {
			t.Fatal("already-invalid payload reached transport")
		}
	}
}

func TestDeferredSCONCannotOutliveRegisteredBinding(t *testing.T) {
	c, sent := rkmRequesterAssociation(t, 1, 1)
	c.endpoint = methodEndpointForTest(t, RoleASP, uint8(CongestionNationalWithoutPriorities), 3)
	register := func(id RemoteASID) ASKey {
		t.Helper()
		results, err := c.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{RemoteAS: id, RoutingKey: testRoutingKey(7, 100, 3)})
		if err != nil || len(results) != 1 {
			t.Fatalf("register: %+v, %v", results, err)
		}
		c.setState(StateASPActive)
		c.noteRoutingContextsAcked(params.NewRoutingContext(1))
		return results[0].ASKey
	}
	old := register("old-as")
	entered, released := make(chan struct{}), make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(released) })
	writer := c.signalWriter
	var block sync.Once
	c.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.Notify); ok {
			block.Do(func() { close(entered); <-released })
		}
		return writer(message)
	}
	if err := c.writeMandatoryControls([]messages.M3UA{messages.NewNotify(params.NewStatus(params.AsStateActive), nil, nil, nil)}, false, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("queue barrier not entered")
	}
	message := messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(2), nil)
	if err := c.writeMandatoryControls([]messages.M3UA{message}, true, false); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeregisterApplicationServers(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	register("replacement-as")
	release.Do(func() { close(released) })
	if err := c.writeMandatoryControls([]messages.M3UA{messages.NewNotify(params.NewStatus(params.AsStateActive), nil, nil, nil)}, false, true); err != nil {
		t.Fatalf("retired publication stopped queue service: %v", err)
	}
	for _, message := range *sent {
		if _, ok := message.(*messages.SignallingCongestion); ok {
			t.Fatal("queued SCON was submitted for a replacement binding")
		}
	}
}

func TestMTPMethodAdmissionPreservesInvalidStreamAndRouteErrors(t *testing.T) {
	for _, invalid := range []string{"stream", "inactive", "oversize-under-congestion", "method-mismatch-with-oversize", "route-method"} {
		t.Run(invalid, func(t *testing.T) {
			profile := CongestionProfile{Method: CongestionInternational}
			if invalid == "oversize-under-congestion" {
				profile = CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 3}
			}
			e, associations, captures := newASPTransferFixtureReporting(t, oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare), true, &CongestionConfig{Default: profile})
			data := transferProtocolData(0x123456, 1, nil)
			data.MessagePriority = 1
			want := ErrProtocolDataTooLarge
			for _, c := range associations {
				switch invalid {
				case "stream":
					c.maxMessageStreamID = 0
					want = ErrNoDataStream
				case "inactive":
					c.noteRoutingContextsInactive([]uint32{1})
					c.setState(StateASPInactive)
					want = ErrNoMTPRoute
				case "oversize-under-congestion":
					data.Data = make([]byte, 65520)
					applyASPSCON(t, c, 7, 1, 0x123456, 0, params.NewCongestionIndications(3))
				case "method-mismatch-with-oversize", "route-method":
					if invalid == "route-method" {
						want = ErrInvalidParameterValue
					} else {
						data.Data = make([]byte, 65520)
					}
					c.cfg.CongestionProfiles = []NetworkCongestionProfile{{NetworkAppearance: 7, NetworkAppearanceSet: true, Profile: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 3}}}
				}
			}
			_, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: data})
			if !errors.Is(err, want) {
				t.Fatalf("existing %s error = %v, want %v", invalid, err, want)
			}
			for _, capture := range captures {
				if capture.count() != 0 {
					t.Fatal("invalid request reached transport")
				}
			}
		})
	}
}

func TestMethodAdmissionPreservesTypedAndRawDataErrors(t *testing.T) {
	for _, role := range []Role{RoleASP, RoleIPSP, RoleSGP} {
		for _, raw := range []bool{false, true} {
			for _, invalid := range []string{"management", "stream", "scope", "inactive"} {
				t.Run(fmt.Sprintf("role_%s/raw_%t/%s", role, raw, invalid), func(t *testing.T) {
					c, capture := newDataWriteAssociation(t, 1)
					c.role = role
					c.endpoint = methodEndpointForTest(t, role, uint8(CongestionInternational), 0)
					c.noteRoutingContextsActive([]uint32{1})
					data := testProtocolData(nil)
					data.MessagePriority = 255
					key := writeScope(1)
					var want error
					switch invalid {
					case "management":
						data.ServiceIndicator = 0
						want = ErrMTPManagementData
					case "stream":
						c.maxMessageStreamID = 0
						want = ErrNoDataStream
					case "scope":
						key.RoutingContext = 999
						want = ErrInvalidRoutingContext
					case "inactive":
						c.setState(StateASPInactive)
						want = ErrNotEstablished
					}
					var err error
					if raw {
						_, err = c.WriteSignal(messages.NewData(params.NewNetworkAppearance(key.NetworkAppearance), params.NewRoutingContext(key.RoutingContext), params.NewProtocolData(data.OriginatingPointCode, data.DestinationPointCode, data.ServiceIndicator, data.NetworkIndicator, data.MessagePriority, data.SignallingLinkSelection, data.Data), nil))
					} else {
						_, err = c.WriteData(DataRequest{AS: key, ProtocolData: data})
					}
					if !errors.Is(err, want) {
						t.Fatalf("existing error = %v, want %v", err, want)
					}
					if capture.submissions() != 0 {
						t.Fatal("already-invalid DATA reached transport")
					}
				})
			}
		}
	}
}
