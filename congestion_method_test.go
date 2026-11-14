package m3ua

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func setMethodForTest(config *EndpointConfig, method, max uint8) {
	config.Congestion = &CongestionConfig{Default: CongestionProfile{Method: CongestionMethod(method), MaxLevel: max}}
}

func methodEndpointForTest(t *testing.T, role Role, method, max uint8) *Endpoint {
	t.Helper()
	config := EndpointConfig{Role: role}
	setMethodForTest(&config, method, max)
	endpoint, err := NewEndpoint(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	return endpoint
}

func TestCongestionMethodStrictSendsShareTypedAndSerializedAdmission(t *testing.T) {
	for method := uint8(1); method <= 3; method++ {
		for _, max := range []uint8{1, 2, 3} {
			if method < 2 && max != 1 {
				continue
			}
			for _, mp := range []uint8{0, 1, 2, 3, 4, 255} {
				for _, raw := range []bool{false, true} {
					t.Run(fmt.Sprintf("method=%d/max=%d/mp=%d/raw=%v", method, max, mp, raw), func(t *testing.T) {
						c, capture := newDataWriteAssociation(t, 1)
						c.signalWriter = nil
						c.endpoint = methodEndpointForTest(t, RoleASP, method, func() uint8 {
							if method < 2 {
								return 0
							}
							return max
						}())
						payload := testProtocolData([]byte("method"))
						payload.MessagePriority = mp
						request := DataRequest{AS: writeScope(1), ProtocolData: payload}
						var err error
						if raw {
							_, err = c.WriteSignal(messages.New(1, messages.MsgClassTransfer, messages.MsgTypePayloadData, params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewProtocolData(payload.OriginatingPointCode, payload.DestinationPointCode, payload.ServiceIndicator, payload.NetworkIndicator, payload.MessagePriority, payload.SignallingLinkSelection, payload.Data)))
						} else {
							_, err = c.WriteData(request)
						}
						valid := method == 2 && mp <= max || method != 2 && mp == 0
						if valid {
							if err != nil {
								t.Fatal(err)
							}
						} else if !errors.Is(err, ErrInvalidParameterValue) {
							t.Fatalf("invalid MP admitted: %v", err)
						}
						if !valid && capture.submissions() != 0 {
							t.Fatal("invalid MP reached transport")
						}
					})
				}
			}
			for _, level := range []uint8{1, 2, 3} {
				for _, set := range []bool{false, true} {
					for _, raw := range []bool{false, true} {
						t.Run(fmt.Sprintf("SCON/method=%d/max=%d/level=%d/set=%v/raw=%v", method, max, level, set, raw), func(t *testing.T) {
							c, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
							c.noteRoutingContextsAcked(params.NewRoutingContext(1))
							c.endpoint = methodEndpointForTest(t, RoleASP, method, func() uint8 {
								if method < 2 {
									return 0
								}
								return max
							}())
							req := SignallingCongestionRequest{Scope: WireScope{RoutingContexts: []uint32{1}, RoutingContextSet: true}, Destinations: []PointCodeRange{{PointCode: 100}}, CongestionLevel: level, CongestionLevelSet: set}
							var err error
							if raw {
								var ci *params.Param
								if set {
									ci = params.NewCongestionIndications(level)
								}
								ps := []*params.Param{params.NewRoutingContext(1), params.NewAffectedPointCode(100)}
								if ci != nil {
									ps = append(ps, ci)
								}
								_, err = c.WriteSignal(messages.New(1, messages.MsgClassSSNM, messages.MsgTypeSignallingCongestion, ps...))
							} else {
								err = c.SignallingCongestion(req)
							}
							valid := !set || method >= 2 && level <= max
							if valid {
								if err != nil {
									t.Fatal(err)
								}
							} else if !errors.Is(err, ErrInvalidParameterValue) {
								t.Fatalf("invalid level admitted: %v", err)
							}
							if !valid && len(*sent) != 0 {
								t.Fatal("invalid SCON reached transport")
							}
						})
					}
				}
			}
		}
	}
}

func TestCongestionMethodMismatchedReceiveKeepsInformationAndReports(t *testing.T) {
	for _, method := range []uint8{1, 2, 3} {
		t.Run(fmt.Sprint(method), func(t *testing.T) {
			c, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
			c.noteRoutingContextsAcked(params.NewRoutingContext(1))
			c.endpoint = methodEndpointForTest(t, RoleASP, method, func() uint8 {
				if method == 1 {
					return 0
				}
				return 1
			}())
			if !c.endpoint.trackAssociation(c) {
				t.Fatal("track association")
			}
			observeSSNM(t, c)
			scon := messages.NewSignallingCongestion(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(3), nil)
			raw, err := scon.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			c.dispatchRaw(context.Background(), inbound{data: raw, stream: 0, ppid: M3UAPPID})
			report := nextSSNMReport(t, c)
			if report.CongestionLevel != 3 || !report.CongestionLevelSet || len(*sent) != 0 || firstErr(c) != nil {
				t.Fatal("mismatched SCON rejected or changed")
			}
			mismatches := reflect.ValueOf(report).FieldByName("ProfileMismatches")
			if !mismatches.IsValid() || mismatches.Len() != 1 {
				t.Fatal("SCON profile mismatch not reported to Layer Management")
			}
		})
	}
}

func TestCongestionMethodMismatchedDATAIsDeliveredAndBounded(t *testing.T) {
	c, _ := newDataWriteAssociation(t, 1)
	c.endpoint = methodEndpointForTest(t, RoleASP, 1, 0)
	if !c.endpoint.trackAssociation(c) {
		t.Fatal("track")
	}
	management := c.ManagementIndications()
	payload := testProtocolData([]byte("unchanged"))
	payload.MessagePriority = 255
	raw := c.encodeDataFrame(&DataRequest{AS: writeScope(1), ProtocolData: payload})
	const count = 1024
	validPayload := payload
	validPayload.MessagePriority = 0
	validRaw := c.encodeDataFrame(&DataRequest{AS: writeScope(1), ProtocolData: validPayload})
	for index := range count * 2 {
		incoming, expectedMP := raw, uint8(255)
		if index%2 == 0 {
			incoming, expectedMP = validRaw, 0
		}
		c.dispatchRaw(context.Background(), inbound{data: incoming, stream: 1, ppid: M3UAPPID})
		// This socket-free fixture has no FSM monitor; consume its publication token.
		select {
		case <-c.stateChan:
		default:
		}
		select {
		case <-c.done:
			t.Fatalf("mismatched traffic closed association at %d messages; management queue=%d", index+1, len(management))
		default:
		}
		select {
		case msg := <-c.dataChan:
			if msg.ProtocolData.MessagePriority != expectedMP || string(msg.ProtocolData.Data) != "unchanged" {
				t.Fatal("mismatch changed DATA")
			}
		default:
			t.Fatal("well-formed mismatched DATA was dropped")
		}
	}
	diagnostics := c.CongestionProfileDiagnostics()
	if diagnostics.ReceivedDataMismatches != count || diagnostics.LastDataMismatch == nil || diagnostics.LastDataMismatch.MessagePriority != 255 {
		t.Fatalf("diagnostics=%+v", diagnostics)
	}
	if len(management) != 1 {
		t.Fatalf("mismatch primitive count=%d, want exactly one", len(management))
	}
	ind := <-management
	if ind.Kind != ManagementCongestionProfileMismatch || ind.CongestionProfileMismatch == nil {
		t.Fatalf("indication=%+v", ind)
	}
	select {
	case <-c.done:
		t.Fatal("mismatches closed the association")
	default:
	}
	if err := firstErr(c); err != nil {
		t.Fatalf("mismatch sent Error: %v", err)
	}
	diagnostics.LastDataMismatch.Scope.RoutingContexts[0] = 999
	if c.CongestionProfileDiagnostics().LastDataMismatch.Scope.RoutingContexts[0] != 1 {
		t.Fatal("snapshot aliases diagnostic")
	}
}

func TestCongestionMethodLocalUserGateKeepsEqualPriorityAndCapsReceivedLevel(t *testing.T) {
	for _, method := range []uint8{1, 2, 3} {
		for _, max := range []uint8{1, 2, 3} {
			for _, levelSet := range []bool{false, true} {
				for mp := uint8(0); mp <= max; mp++ {
					t.Run(fmt.Sprintf("method=%d/max=%d/levelSet=%v/mp=%d", method, max, levelSet, mp), func(t *testing.T) {
						c, capture := newDataWriteAssociation(t, 1)
						c.signalWriter = nil
						profileMax := max
						if method < 2 {
							profileMax = 0
						}
						c.endpoint = methodEndpointForTest(t, RoleASP, method, profileMax)
						if !c.endpoint.trackAssociation(c) {
							t.Fatal("track")
						}
						var ci *params.Param
						if levelSet {
							ci = params.NewCongestionIndications(3)
						}
						applyASPSCON(t, c, 7, 1, 100, 0, ci)
						payload := testProtocolData([]byte("priority"))
						payload.DestinationPointCode = 100
						payload.MessagePriority = mp
						_, err := c.WriteData(DataRequest{AS: writeScope(1), ProtocolData: payload})
						validMP := method == 2 || mp == 0
						allowed := validMP && (method != 2 || levelSet && mp >= max)
						if allowed {
							if err != nil {
								t.Fatal(err)
							}
							if capture.submissions() != 1 {
								t.Fatal("allowed priority not submitted")
							}
						} else {
							var refusal *DataWriteError
							if !errors.As(err, &refusal) || refusal.Outcome != DataNotSent {
								t.Fatalf("expected known-not-sent refusal, got %v", err)
							}
							if capture.submissions() != 0 {
								t.Fatal("refused priority reached transport")
							}
						}
					})
				}
			}
		}
	}
}

func TestCongestionMethodStateAndScopeErrorsPrecedeProfileMismatch(t *testing.T) {
	for _, raw := range []bool{false, true} {
		for _, tc := range []struct {
			name string
			key  ASKey
			want error
		}{
			{"appearance", *staticASKey(99, 1), ErrInvalidNetworkAppearance}, {"context", *staticASKey(7, 99), ErrInvalidRoutingContext}, {"inactive", *staticASKey(7, 2), ErrRoutingContextNotActive},
		} {
			t.Run(fmt.Sprintf("%s/raw=%v", tc.name, raw), func(t *testing.T) {
				c, _ := newDataWriteAssociation(t, 1, 2)
				c.signalWriter = nil
				c.noteRoutingContextsUnacked(params.NewRoutingContext(2))
				c.endpoint = methodEndpointForTest(t, RoleASP, 1, 0)
				payload := testProtocolData([]byte("precedence"))
				payload.MessagePriority = 255
				var err error
				if raw {
					message, parseErr := messages.Parse(c.encodeDataFrame(&DataRequest{AS: tc.key, ProtocolData: payload}))
					if parseErr != nil {
						t.Fatal(parseErr)
					}
					_, err = c.WriteSignal(message)
				} else {
					_, err = c.WriteData(DataRequest{AS: tc.key, ProtocolData: payload})
				}
				if !errors.Is(err, tc.want) {
					t.Fatalf("scope precedence=%v want %v", err, tc.want)
				}
			})
		}
	}
}

func TestCongestionMethodSGPTransitDoesNotUseLocalUserGate(t *testing.T) {
	for _, raw := range []bool{false, true} {
		c, capture := newDataWriteAssociation(t, 1)
		c.role = RoleSGP
		c.noteRoutingContextsActive([]uint32{1})
		c.signalWriter = nil
		c.endpoint = methodEndpointForTest(t, RoleSGP, 2, 2)
		c.transportSnapshot.Store(&TransportCongestionState{Known: true, Level: 3, Epoch: c.Epoch()})
		payload := testProtocolData([]byte("transit"))
		payload.MessagePriority = 0
		var err error
		if raw {
			message, parseErr := messages.Parse(c.encodeDataFrame(&DataRequest{AS: writeScope(1), ProtocolData: payload}))
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			_, err = c.WriteSignal(message)
		} else {
			_, err = c.WriteData(DataRequest{AS: writeScope(1), ProtocolData: payload})
		}
		if err != nil || capture.submissions() != 1 {
			t.Fatalf("transit raw=%v err=%v submissions=%d", raw, err, capture.submissions())
		}
	}
}

func TestCongestionMethodRawSignalSeamCannotBypassProfile(t *testing.T) {
	c, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	c.noteRoutingContextsAcked(params.NewRoutingContext(1))
	c.endpoint = methodEndpointForTest(t, RoleASP, 1, 0)
	payload := testProtocolData([]byte("serialized"))
	payload.MessagePriority = 1
	frame := c.encodeDataFrame(&DataRequest{AS: ASKey{RoutingContext: 1, RoutingContextSet: true}, ProtocolData: payload})
	msg, err := messages.Parse(frame)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := c.WriteSignal(msg); n != 0 || !errors.Is(err, ErrInvalidParameterValue) || len(*sent) != 0 {
		t.Fatalf("raw signal bypass: n=%d err=%v sent=%d", n, err, len(*sent))
	}
}

func TestCongestionMethodTransportAndReceivedSCONStayIndependent(t *testing.T) {
	c, capture := newDataWriteAssociation(t, 1)
	c.signalWriter = nil
	c.endpoint = methodEndpointForTest(t, RoleASP, 2, 2)
	if !c.endpoint.trackAssociation(c) {
		t.Fatal("track")
	}
	payload := testProtocolData([]byte("independent"))
	payload.DestinationPointCode = 100
	payload.MessagePriority = 1
	received := func(level uint8) { applyASPSCON(t, c, 7, 1, 100, 0, params.NewCongestionIndications(level)) }
	transport := func(level uint8, known bool) {
		c.transportSnapshot.Store(&TransportCongestionState{Known: known, Level: level, Epoch: c.Epoch()})
	}
	refused := func() {
		t.Helper()
		_, err := c.WriteData(DataRequest{AS: writeScope(1), ProtocolData: payload})
		requireDataWriteError(t, err, DataNotSent, ErrNoMTPRoute)
	}
	received(2)
	transport(3, true)
	refused()
	transport(0, true)
	refused()
	received(0)
	transport(3, true)
	refused()
	transport(0, true)
	if _, err := c.WriteData(DataRequest{AS: writeScope(1), ProtocolData: payload}); err != nil {
		t.Fatal(err)
	}
	transport(3, false)
	refused()
	c.transportSnapshot.Store(nil)
	if _, err := c.WriteData(DataRequest{AS: writeScope(1), ProtocolData: payload}); err != nil {
		t.Fatal(err)
	}
	if capture.submissions() != 2 {
		t.Fatal("clearing one contribution changed another")
	}
}

func TestCongestionMethodInternationalNumericClearIsAcceptedAndReported(t *testing.T) {
	c, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	c.noteRoutingContextsAcked(params.NewRoutingContext(1))
	c.endpoint = methodEndpointForTest(t, RoleASP, 1, 0)
	if !c.endpoint.trackAssociation(c) {
		t.Fatal("track")
	}
	observeSSNM(t, c)
	message := messages.NewSignallingCongestion(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(0), nil)
	raw, err := message.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	c.dispatchRaw(context.Background(), inbound{data: raw, stream: 0, ppid: M3UAPPID})
	report := nextSSNMReport(t, c)
	if report.CongestionLevel != 0 || !report.CongestionLevelSet || len(report.ProfileMismatches) != 1 || len(*sent) != 0 {
		t.Fatalf("clear report=%+v sent=%d", report, len(*sent))
	}
	state, known := c.destinations.lookupRange(destinationKey{routingContext: 1, routingContextSet: true}, 100, 0)
	if !known || state.Congestion.Congested || !state.Congestion.LevelSet || state.Congestion.Level != 0 {
		t.Fatalf("retained clear=%+v known=%v", state, known)
	}
}

func TestCongestionMethodWriterRechecksPressureBeforeSubmission(t *testing.T) {
	c, capture := newDataWriteAssociation(t, 1)
	c.endpoint = methodEndpointForTest(t, RoleASP, 2, 2)
	payload := testProtocolData([]byte("recheck"))
	payload.MessagePriority = 1
	c.sendMu.Lock()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, err := c.WriteData(DataRequest{AS: writeScope(1), ProtocolData: payload})
		done <- err
	}()
	<-started
	c.transportSnapshot.Store(&TransportCongestionState{Known: true, Level: 3, Epoch: c.Epoch()})
	c.sendMu.Unlock()
	requireDataWriteError(t, <-done, DataNotSent, ErrNoMTPRoute)
	if capture.submissions() != 0 {
		t.Fatal("pressure changed while waiting but DATA was submitted")
	}
}

func TestCongestionMethodSCONScopeAndFramingPrecedeProfileMismatch(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	c.noteRoutingContextsAcked(params.NewRoutingContext(1))
	c.endpoint = methodEndpointForTest(t, RoleASP, 1, 0)
	message := messages.NewSignallingCongestion(params.NewNetworkAppearance(99), params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(3), nil)
	if _, err := c.WriteSignal(message); !errors.Is(err, ErrInvalidNetworkAppearance) {
		t.Fatalf("SCON scope precedence=%v", err)
	}
	raw, err := messages.NewSignallingCongestion(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(3), nil).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	raw[18], raw[19] = 0, 5 // APC framing, before method semantics.
	_, framingErr := messages.Parse(raw)
	if framingErr == nil {
		t.Fatal("fixture is well formed")
	}
	malformed := encodedDataSignal{Data: messages.NewData(nil, nil, params.NewProtocolData(1, 100, 3, 0, 0, 1, nil), nil), raw: raw}
	if n, err := c.WriteSignal(malformed); n != 0 || err == nil || err.Error() != framingErr.Error() {
		t.Fatalf("framing precedence n=%d err=%v want %v", n, err, framingErr)
	}
}

func TestCongestionMethodCallerSerializerCannotHideSCONOrMP(t *testing.T) {
	c, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	c.noteRoutingContextsAcked(params.NewRoutingContext(1))
	c.endpoint = methodEndpointForTest(t, RoleASP, 1, 0)
	scon := messages.NewSignallingCongestion(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(3), nil)
	raw, err := scon.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	claimed := messages.NewData(nil, params.NewRoutingContext(1), params.NewProtocolData(1, 100, 3, 0, 0, 1, nil), nil)
	if n, err := c.WriteSignal(encodedDataSignal{Data: claimed, raw: raw}); n != 0 || !errors.Is(err, ErrInvalidParameterValue) {
		t.Fatalf("hidden SCON n=%d err=%v", n, err)
	}
	payload := testProtocolData(nil)
	payload.MessagePriority = 255
	raw = c.encodeDataFrame(&DataRequest{AS: ASKey{RoutingContext: 1, RoutingContextSet: true}, ProtocolData: payload})
	if n, err := c.WriteSignal(encodedDataSignal{Data: claimed, raw: raw}); n != 0 || !errors.Is(err, ErrInvalidParameterValue) {
		t.Fatalf("hidden MP n=%d err=%v", n, err)
	}
	if len(*sent) != 0 {
		t.Fatal("caller serializer bypassed admission")
	}
}
