package m3ua

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
)

func TestNewEndpointRequiresCongestionMethod(t *testing.T) {
	for _, role := range []Role{RoleASP, RoleIPSP, RoleSGP} {
		for i, config := range []*CongestionConfig{nil, {}, {Default: CongestionProfile{MaxLevel: 0}}} {
			t.Run(fmt.Sprintf("%s/config=%d", role, i), func(t *testing.T) {
				endpoint, err := NewEndpoint(EndpointConfig{Role: role, Congestion: config})
				if endpoint != nil {
					_ = endpoint.Close()
				}
				if endpoint != nil || !errors.Is(err, ErrInvalidRoleConfiguration) {
					t.Fatalf("missing method endpoint=%v err=%v", endpoint, err)
				}
			})
		}
	}
}

func TestZeroMethodOverrideRejected(t *testing.T) {
	for _, role := range []Role{RoleASP, RoleIPSP, RoleSGP} {
		t.Run(role.String(), func(t *testing.T) {
			t.Run("endpoint-network", func(t *testing.T) {
				config := EndpointConfig{Role: role, Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}, Networks: []NetworkCongestionProfile{{}}}}
				endpoint, err := NewEndpoint(config)
				if endpoint != nil {
					_ = endpoint.Close()
				}
				if !errors.Is(err, ErrInvalidRoleConfiguration) {
					t.Fatalf("zero network override: %v", err)
				}
			})
			t.Run("association-override", func(t *testing.T) {
				endpoint, err := NewEndpoint(EndpointConfig{Role: role, Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}}})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = endpoint.Close() }()
				association := NewAssociationConfig()
				if role == RoleIPSP {
					association.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
					association.ASPProcedures = ipspInitiationPolicy(true, true)
				}
				// An invalid network stops every control before it can open a socket.
				_, err = endpoint.Dial(context.Background(), "invalid-network", nil, nil, association)
				if err == nil || errors.Is(err, ErrInvalidRoleConfiguration) {
					t.Fatalf("valid method control failed before network check: %v", err)
				}
				association.CongestionProfiles = []NetworkCongestionProfile{{}}
				_, err = endpoint.Dial(context.Background(), "invalid-network", nil, nil, association)
				if !errors.Is(err, ErrInvalidRoleConfiguration) {
					t.Fatalf("zero association override was not refused before transport setup: %v", err)
				}
			})
		})
	}
}

func TestNoPermissiveMethodReachable(t *testing.T) {
	for _, method := range []CongestionMethod{CongestionInternational, CongestionNationalWithPriorities, CongestionNationalWithoutPriorities} {
		for _, raw := range []bool{false, true} {
			t.Run(fmt.Sprintf("method=%d/raw=%v", method, raw), func(t *testing.T) {
				c, capture := newDataWriteAssociation(t, 1)
				c.signalWriter = nil
				max := uint8(0)
				if method != CongestionInternational {
					max = 3
				}
				c.endpoint = methodEndpointForTest(t, RoleASP, uint8(method), max)
				payload := testProtocolData([]byte("invalid-priority"))
				payload.MessagePriority = 255
				request := DataRequest{AS: writeScope(1), ProtocolData: payload}
				var err error
				if raw {
					var message messages.M3UA
					message, err = messages.Parse(c.encodeDataFrame(&request))
					if err != nil {
						t.Fatal(err)
					}
					_, err = c.WriteSignal(message)
				} else {
					_, err = c.WriteData(request)
				}
				if !errors.Is(err, ErrInvalidParameterValue) || capture.submissions() != 0 {
					t.Fatalf("MP255 err=%v writes=%d", err, capture.submissions())
				}
			})
		}
	}
	// Invalid internal state must fail closed even if a future call path misses construction validation.
	for _, method := range []CongestionMethod{0, 4, 255} {
		if (CongestionProfile{Method: method}).allowsPriority(255) {
			t.Fatalf("invalid method %d has a permissive priority path", method)
		}
	}
}

func TestSGPTransitRelaysAndReportsMethodMismatch(t *testing.T) {
	for _, tc := range []struct {
		method  CongestionMethod
		max, mp uint8
	}{{CongestionInternational, 0, 1}, {CongestionNationalWithPriorities, 1, 3}, {CongestionNationalWithoutPriorities, 3, 1}} {
		for _, raw := range []bool{false, true} {
			t.Run(fmt.Sprintf("method=%d/raw=%v", tc.method, raw), func(t *testing.T) {
				c, capture := newDataWriteAssociation(t, 1)
				c.role = RoleSGP
				c.noteRoutingContextsActive([]uint32{1})
				c.signalWriter = nil
				c.endpoint = methodEndpointForTest(t, RoleSGP, uint8(tc.method), tc.max)
				indications := c.ManagementIndications()
				payload := testProtocolData([]byte("original-ss7"))
				payload.MessagePriority = tc.mp
				request := DataRequest{AS: writeScope(1), ProtocolData: payload}
				var err error
				if raw {
					var message messages.M3UA
					message, err = messages.Parse(c.encodeDataFrame(&request))
					if err != nil {
						t.Fatal(err)
					}
					_, err = c.WriteSignal(message)
				} else {
					_, err = c.WriteData(request)
				}
				if err != nil {
					t.Fatalf("SGP refused NIF transit: %v", err)
				}
				sent := capture.messages(t)
				if len(sent) != 1 {
					t.Fatalf("sent %d messages", len(sent))
				}
				got, err := sent[0].ProtocolData.ProtocolData()
				if err != nil || got.MessagePriority != tc.mp || string(got.Data) != "original-ss7" {
					t.Fatalf("changed transit: %+v, %v", got, err)
				}
				diagnostic := c.CongestionProfileDiagnostics()
				if diagnostic.ReceivedDataMismatches != 1 || diagnostic.LastDataMismatch == nil || diagnostic.LastDataMismatch.MessagePriority != tc.mp {
					t.Fatalf("missing transit diagnostic: %+v", diagnostic)
				}
				if len(indications) != 1 {
					t.Fatalf("diagnostic indications=%d", len(indications))
				}
			})
		}
	}
}

func TestSGPTransitMismatchFloodKeepsManagementQueueBounded(t *testing.T) {
	c, capture := newDataWriteAssociation(t, 1)
	c.role = RoleSGP
	c.noteRoutingContextsActive([]uint32{1})
	c.signalWriter = nil
	c.endpoint = methodEndpointForTest(t, RoleSGP, uint8(CongestionInternational), 0)
	indications := c.ManagementIndications()
	payload := testProtocolData([]byte("transit"))
	payload.MessagePriority = 1
	for range 2048 {
		if _, err := c.WriteData(DataRequest{AS: writeScope(1), ProtocolData: payload}); err != nil {
			t.Fatalf("transit flood closed/refused: %v", err)
		}
		if len(indications) > 1 {
			t.Fatalf("transit mismatches emitted %d primitives", len(indications))
		}
	}
	select {
	case <-c.done:
		t.Fatal("transit mismatch flood closed association")
	default:
	}
	if capture.submissions() != 2048 || len(indications) != 1 || c.CongestionProfileDiagnostics().ReceivedDataMismatches != 2048 {
		t.Fatalf("writes=%d indications=%d diagnostics=%+v", capture.submissions(), len(indications), c.CongestionProfileDiagnostics())
	}
}

func TestSGPTransitRefusesPriorityOutsideSupportedWidth(t *testing.T) {
	for _, mp := range []uint8{4, 255} {
		for _, raw := range []bool{false, true} {
			t.Run(fmt.Sprintf("mp=%d/raw=%v", mp, raw), func(t *testing.T) {
				c, capture := newDataWriteAssociation(t, 1)
				c.role = RoleSGP
				c.noteRoutingContextsActive([]uint32{1})
				c.signalWriter = nil
				c.endpoint = methodEndpointForTest(t, RoleSGP, uint8(CongestionInternational), 0)
				payload := testProtocolData(nil)
				payload.MessagePriority = mp
				request := DataRequest{AS: writeScope(1), ProtocolData: payload}
				var err error
				if raw {
					var m messages.M3UA
					m, err = messages.Parse(c.encodeDataFrame(&request))
					if err != nil {
						t.Fatal(err)
					}
					_, err = c.WriteSignal(m)
				} else {
					_, err = c.WriteData(request)
				}
				if !errors.Is(err, ErrInvalidParameterValue) || capture.submissions() != 0 {
					t.Fatalf("out-of-width MP: %v, writes=%d", err, capture.submissions())
				}
			})
		}
	}
}

func TestSGPTransitAndPeerDiagnosticShareLifetimeBound(t *testing.T) {
	c, capture := newDataWriteAssociation(t, 1)
	c.role = RoleSGP
	c.noteRoutingContextsActive([]uint32{1})
	c.signalWriter = nil
	c.endpoint = methodEndpointForTest(t, RoleSGP, uint8(CongestionInternational), 0)
	indications := c.ManagementIndications()
	payload := testProtocolData([]byte("mixed-input"))
	payload.MessagePriority = 1
	c.observeDataProfile(&DataMessage{AS: writeScope(1), Scope: testWireScope(7, true, 1), ProtocolData: &payload, Epoch: c.Epoch()})
	if _, err := c.WriteData(DataRequest{AS: writeScope(1), ProtocolData: payload}); err != nil {
		t.Fatal(err)
	}
	if capture.submissions() != 1 || c.CongestionProfileDiagnostics().ReceivedDataMismatches != 2 || len(indications) != 1 {
		t.Fatalf("mixed diagnostic count=%+v indications=%d writes=%d", c.CongestionProfileDiagnostics(), len(indications), capture.submissions())
	}
}
