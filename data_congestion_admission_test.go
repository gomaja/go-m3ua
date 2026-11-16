package m3ua

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func admissionCachedSample(c *Association, now time.Time, charged, limit int) {
	c.transportSnapshot.Store(&TransportCongestionState{Association: c.ID(), Epoch: c.Epoch(), SampleValid: true, Known: true, SampledAt: now, LastValidAt: now, Charged: charged, Limit: limit})
}

func admissionFrozenClock(c *Association) time.Time {
	now := time.Unix(1800000000, 0)
	c.endpoint.transport.now = func() time.Time { return now }
	return now
}

func admissionData() *messages.Data {
	return messages.NewData(params.NewNetworkAppearance(7), params.NewRoutingContext(1),
		params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("gate")), nil)
}

func TestCongestionAdmissionCoversEveryDataAPI(t *testing.T) {
	for _, api := range []string{"typed", "typed-context", "raw", "raw-seam", "distributed", "distributed-association", "distributed-transport", "distributed-association-transport", "routed", "routed-context"} {
		t.Run(api, func(t *testing.T) {
			if api == "distributed" || api == "distributed-association" || api == "distributed-transport" || api == "distributed-association-transport" {
				listener, as, c, capture := distributionFixture(t, params.TrafficModeLoadshare)
				c.cfg.CongestionAdmission = &CongestionAdmissionConfig{}
				c.setState(StateASPActive)
				c.noteRoutingContextsActive([]uint32{1})
				as.setASPState(c, StateASPActive, time.Hour)
				frameCapture := &dataFrameCapture{}
				if api == "distributed-transport" || api == "distributed-association-transport" {
					c.signalWriter = nil
					c.dataWriter = frameCapture.write
				}
				now := admissionFrozenClock(c)
				admissionCachedSample(c, now, 90, 100)
				var err error
				if api == "distributed-association" || api == "distributed-association-transport" {
					_, err = c.DistributeData(distributionData(1, 1, "gate"))
				} else {
					_, err = listener.DistributeData(distributionData(1, 1, "gate"))
				}
				if !errors.Is(err, ErrDataCongestionRefused) {
					t.Fatalf("distributed DATA escaped cached refusal: %v", err)
				}
				var refused *DataWriteError
				if !errors.As(err, &refused) || refused.Outcome != DataNotSent {
					t.Fatalf("distributed refusal lost not-sent classification: %v", err)
				}
				if capture.dataCount() != 0 || frameCapture.submissions() != 0 {
					t.Fatal("distributed refused DATA reached transport")
				}
				return
			}
			if api == "routed" || api == "routed-context" {
				e, associations, captures := newASPTransferFixture(t, oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare))
				for _, c := range associations {
					c.cfg.CongestionAdmission = &CongestionAdmissionConfig{}
					now := admissionFrozenClock(c)
					admissionCachedSample(c, now, 90, 100)
				}
				request := MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 1, []byte("gate"))}
				var err error
				if api == "routed-context" {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					_, err = e.MTPTransferContext(ctx, request)
				} else {
					_, err = e.MTPTransfer(request)
				}
				if !errors.Is(err, ErrDataCongestionRefused) {
					t.Fatalf("routed DATA escaped cached refusal: %v", err)
				}
				for _, capture := range captures {
					if capture.count() != 0 {
						t.Fatal("routed refused DATA reached transport")
					}
				}
				return
			}
			c, capture := newDataWriteAssociation(t, 1)
			c.cfg.CongestionAdmission = &CongestionAdmissionConfig{}
			now := admissionFrozenClock(c)
			admissionCachedSample(c, now, 90, 100)
			var err error
			switch api {
			case "typed":
				_, err = c.WriteData(DataRequest{AS: writeScope(1), ProtocolData: simpleProtocolData("gate")})
			case "typed-context":
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				_, err = c.WriteDataContext(ctx, DataRequest{AS: writeScope(1), ProtocolData: simpleProtocolData("gate")})
			case "raw":
				c.signalWriter = nil
				_, err = c.WriteSignal(admissionData())
			case "raw-seam":
				c.signalWriter = func(message messages.M3UA) (int, error) {
					t.Error("refused DATA reached signal writer")
					return message.MarshalLen(), nil
				}
				_, err = c.WriteSignal(admissionData())
			}
			requireDataWriteError(t, err, DataNotSent, ErrDataCongestionRefused)
			if capture.submissions() != 0 {
				t.Fatal("refused DATA reached transport")
			}
		})
	}
}

func TestCongestionAdmissionSampleValidityAndBoundary(t *testing.T) {
	for _, test := range []struct {
		name    string
		change  func(*TransportCongestionState, time.Time)
		refused bool
	}{
		{"below", func(s *TransportCongestionState, _ time.Time) { s.Charged = 89 }, false},
		{"exact", func(s *TransportCongestionState, _ time.Time) { s.Charged = 90 }, true},
		{"above-limit", func(s *TransportCongestionState, _ time.Time) { s.Charged = 101 }, true},
		{"invalid", func(s *TransportCongestionState, _ time.Time) { s.SampleValid = false }, true},
		{"unknown", func(s *TransportCongestionState, _ time.Time) { s.Known = false }, true},
		{"no-valid", func(s *TransportCongestionState, _ time.Time) { s.LastValidAt = time.Time{} }, true},
		{"at-stale-boundary", func(s *TransportCongestionState, now time.Time) { s.LastValidAt = now.Add(-3 * time.Second) }, true},
		{"before-stale-boundary", func(s *TransportCongestionState, now time.Time) {
			s.LastValidAt = now.Add(-3*time.Second + time.Nanosecond)
		}, false},
		{"stale", func(s *TransportCongestionState, now time.Time) { s.LastValidAt = now.Add(-time.Hour) }, true},
		{"future", func(s *TransportCongestionState, now time.Time) { s.LastValidAt = now.Add(time.Second) }, true},
		{"negative-charge", func(s *TransportCongestionState, _ time.Time) { s.Charged = -1 }, true},
		{"zero-limit", func(s *TransportCongestionState, _ time.Time) { s.Limit = 0 }, true},
		{"old-epoch", func(s *TransportCongestionState, _ time.Time) { s.Epoch++ }, true},
		{"wrong-association", func(s *TransportCongestionState, _ time.Time) { s.Association++ }, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, capture := newDataWriteAssociation(t, 1)
			c.cfg.CongestionAdmission = &CongestionAdmissionConfig{}
			now := admissionFrozenClock(c)
			admissionCachedSample(c, now, 1, 100)
			sample := *c.transportSnapshot.Load()
			test.change(&sample, now)
			c.transportSnapshot.Store(&sample)
			_, err := c.WriteData(DataRequest{AS: writeScope(1), ProtocolData: simpleProtocolData("gate")})
			if test.refused {
				requireDataWriteError(t, err, DataNotSent, ErrDataCongestionRefused)
				if capture.submissions() != 0 {
					t.Fatal("invalid sample admitted DATA")
				}
			} else if err != nil || capture.submissions() != 1 {
				t.Fatalf("below-ratio DATA: %v, submissions=%d", err, capture.submissions())
			}
		})
	}
}

func TestCachedAdmissionDoesNotQuerySocketPerData(t *testing.T) {
	c, capture := newDataWriteAssociation(t, 1)
	c.cfg.CongestionAdmission = &CongestionAdmissionConfig{ChargedRatioLimit: .8}
	now := admissionFrozenClock(c)
	admissionCachedSample(c, now, 79, 100)
	var reads, statusReads atomic.Int64
	c.endpoint.transport.read = func(*Association) (*sctp.SendBacklog, error) { reads.Add(1); return nil, errors.New("must not read") }
	c.endpoint.transport.readStatus = func(*Association) (*sctp.Status, error) {
		statusReads.Add(1)
		return nil, errors.New("must not read status")
	}
	before := *c.transportSnapshot.Load()
	for range 20 {
		if _, err := c.WriteData(DataRequest{AS: writeScope(1), ProtocolData: simpleProtocolData("gate")}); err != nil {
			t.Fatal(err)
		}
	}
	if reads.Load() != 0 || statusReads.Load() != 0 || capture.submissions() != 20 {
		t.Fatalf("cached DATA backlog reads=%d status reads=%d submissions=%d", reads.Load(), statusReads.Load(), capture.submissions())
	}
	if *c.transportSnapshot.Load() != before {
		t.Fatal("DATA admission changed cached sample")
	}
	admissionCachedSample(c, now, 80, 100)
	before = *c.transportSnapshot.Load()
	_, err := c.WriteData(DataRequest{AS: writeScope(1), ProtocolData: simpleProtocolData("gate")})
	requireDataWriteError(t, err, DataNotSent, ErrDataCongestionRefused)
	if reads.Load() != 0 || statusReads.Load() != 0 || capture.submissions() != 20 {
		t.Fatal("refusal queried socket or submitted DATA")
	}
	if *c.transportSnapshot.Load() != before {
		t.Fatal("refusal changed cached sample")
	}
}

func TestCongestionAdmissionDefaultAndControlRemainIndependent(t *testing.T) {
	c, capture := newDataWriteAssociation(t, 1)
	if _, err := c.WriteData(DataRequest{AS: writeScope(1), ProtocolData: simpleProtocolData("default")}); err != nil {
		t.Fatal(err)
	}
	if capture.submissions() != 1 {
		t.Fatal("default blocked without sample")
	}
	c.cfg.CongestionAdmission = &CongestionAdmissionConfig{}
	var controls int
	c.signalWriter = func(message messages.M3UA) (int, error) { controls++; return message.MarshalLen(), nil }
	_, err := c.WriteData(DataRequest{AS: writeScope(1), ProtocolData: simpleProtocolData("no-sample")})
	requireDataWriteError(t, err, DataNotSent, ErrDataCongestionRefused)
	if controls != 0 {
		t.Fatal("DATA admission manufactured control traffic")
	}
	if _, err := c.WriteSignal(messages.NewHeartbeat(nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.writeControl(messages.NewHeartbeatAck(nil)); err != nil {
		t.Fatal(err)
	}
	if controls != 2 {
		t.Fatal("DATA resource refusal affected application or mandatory control")
	}
	if c.TransportCongestion().Samples != 0 || c.dataOverflow.Load() {
		t.Fatal("DATA admission manufactured congestion evidence")
	}
}

func TestCongestionAdmissionValidationAndSnapshot(t *testing.T) {
	e := methodEndpointForTest(t, RoleASP, uint8(CongestionNationalWithoutPriorities), 3)
	for _, ratio := range []float64{-1, 1, 1.1, math.Inf(1), math.Inf(-1), math.NaN()} {
		cfg := &AssociationConfig{CongestionAdmission: &CongestionAdmissionConfig{ChargedRatioLimit: ratio}}
		if err := validateAssociationConfigForRole(RoleASP, cfg); !errors.Is(err, ErrInvalidRoleConfiguration) {
			t.Errorf("ratio %v accepted: %v", ratio, err)
		}
	}
	cfg := &AssociationConfig{CongestionAdmission: &CongestionAdmissionConfig{}}
	if err := validateAssociationConfigForRole(RoleASP, cfg); err != nil {
		t.Fatal(err)
	}
	if err := e.validateAssociationConfig(cfg); err != nil {
		t.Fatal(err)
	}
	e.transport.config.Disabled = true
	if err := e.validateAssociationConfig(cfg); !errors.Is(err, ErrInvalidRoleConfiguration) {
		t.Errorf("disabled monitor accepted gate: %v", err)
	}
	snapshot := snapshotAssociationConfig(cfg)
	cfg.CongestionAdmission.ChargedRatioLimit = .4
	if snapshot.CongestionAdmission.ChargedRatioLimit != 0 {
		t.Fatal("snapshot borrowed mutable admission configuration")
	}
}

func TestCongestionAdmissionReadsCacheAfterSendSerialization(t *testing.T) {
	for _, rawSeam := range []bool{false, true} {
		t.Run(map[bool]string{false: "typed", true: "raw-seam"}[rawSeam], func(t *testing.T) {
			c, capture := newDataWriteAssociation(t, 1)
			c.cfg.CongestionAdmission = &CongestionAdmissionConfig{}
			now := admissionFrozenClock(c)
			admissionCachedSample(c, now, 1, 100)
			var seamCalls atomic.Int64
			if rawSeam {
				c.signalWriter = func(message messages.M3UA) (int, error) { seamCalls.Add(1); return message.MarshalLen(), nil }
			}
			c.sendMu.Lock()
			done := make(chan error, 1)
			started := make(chan struct{})
			go func() {
				close(started)
				if rawSeam {
					_, err := c.WriteSignal(admissionData())
					done <- err
					return
				}
				_, err := c.WriteData(DataRequest{AS: writeScope(1), ProtocolData: simpleProtocolData("serialized")})
				done <- err
			}()
			<-started
			select {
			case err := <-done:
				c.sendMu.Unlock()
				t.Fatalf("DATA bypassed serialization: %v", err)
			case <-time.After(10 * time.Millisecond):
			}
			admissionCachedSample(c, now, 90, 100)
			c.sendMu.Unlock()
			select {
			case err := <-done:
				requireDataWriteError(t, err, DataNotSent, ErrDataCongestionRefused)
			case <-time.After(time.Second):
				t.Fatal("serialized admission did not complete")
			}
			if capture.submissions() != 0 || seamCalls.Load() != 0 {
				t.Fatal("stale pre-lock sample admitted DATA")
			}
		})
	}
}

func TestCongestionAdmissionDoesNotReserveForMessageSize(t *testing.T) {
	c, capture := newDataWriteAssociation(t, 1)
	c.cfg.CongestionAdmission = &CongestionAdmissionConfig{}
	now := admissionFrozenClock(c)
	admissionCachedSample(c, now, 89, 100)
	payload := simpleProtocolData("large")
	payload.Data = make([]byte, 8192)
	if _, err := c.WriteData(DataRequest{AS: writeScope(1), ProtocolData: payload}); err != nil {
		t.Fatal(err)
	}
	if capture.submissions() != 1 {
		t.Fatal("cached admission became a message-size reservation")
	}
	admissionCachedSample(c, now, 101, 100)
	_, err := c.WriteData(DataRequest{AS: writeScope(1), ProtocolData: payload})
	requireDataWriteError(t, err, DataNotSent, ErrDataCongestionRefused)
	if capture.submissions() != 1 {
		t.Fatal("overshot sample did not refuse next DATA")
	}
}

func TestCongestionAdmissionRejectsDisabledMonitorBeforeListen(t *testing.T) {
	e, err := NewEndpoint(nationalCongestionEndpointConfig(EndpointConfig{Role: RoleASP, TransportCongestion: &TransportCongestionConfig{Disabled: true}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	cfg := &AssociationConfig{CongestionAdmission: &CongestionAdmissionConfig{}}
	// The invalid network stops any accidental socket call if validation is absent.
	if _, err := e.Listen("invalid-network", nil, NewListenerConfig(cfg)); !errors.Is(err, ErrInvalidRoleConfiguration) {
		t.Fatalf("disabled monitor reached network selection: %v", err)
	}
	listener := newListener(e, NewListenerConfig(cfg))
	if _, err := listener.resolveAcceptedAssociationConfig(RoleASP, AcceptInfo{}); !errors.Is(err, ErrInvalidRoleConfiguration) {
		t.Fatalf("accepted configuration allowed disabled monitor: %v", err)
	}
}
