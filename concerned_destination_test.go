package m3ua

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestConcernedSCONSendRequiresSingleUnmaskedDestination(t *testing.T) {
	for _, tc := range []struct {
		name         string
		destinations []PointCodeRange
		valid        bool
	}{
		{"single", []PointCodeRange{{PointCode: 0x123456}}, true},
		{"zero", []PointCodeRange{{PointCode: 0}}, true},
		{"maximum", []PointCodeRange{{PointCode: 0xffffff}}, true},
		{"list", []PointCodeRange{{PointCode: 0x123456}, {PointCode: 0x234567}}, false},
		{"duplicates", []PointCodeRange{{PointCode: 0x123456}, {PointCode: 0x123456}}, false},
		{"range", []PointCodeRange{{PointCode: 0x123400, Mask: 8}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, cd := range []bool{false, true} {
				t.Run(fmt.Sprintf("CD=%t", cd), func(t *testing.T) {
					for _, path := range []string{"operation", "typed-message", "generic-message"} {
						t.Run(path, func(t *testing.T) {
							c, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
							setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
							c.noteRoutingContextsAcked(params.NewRoutingContext(1))
							request := SignallingCongestionRequest{Scope: testWireScope(7, true, 1), Destinations: tc.destinations, ConcernedDestination: 0, ConcernedDestinationSet: cd, CongestionLevel: 2, CongestionLevelSet: true}
							var err error
							var n int
							if path == "operation" {
								err = c.SignallingCongestion(request)
							} else {
								msg := concernedSCONMessage(t, request, path == "generic-message")
								n, err = c.WriteSignal(msg)
							}
							if tc.valid || !cd {
								if err != nil || len(*sent) != 1 {
									t.Fatalf("valid send err=%v sent=%d", err, len(*sent))
								}
							} else {
								if !errors.Is(err, ErrInvalidParameterValue) || len(*sent) != 0 || n != 0 {
									t.Fatalf("ambiguous CD admission n=%d err=%v sent=%d; want known pre-write refusal", n, err, len(*sent))
								}
							}
						})
					}
				})
			}
		})
	}
}

func concernedSCONMessage(t *testing.T, request SignallingCongestionRequest, generic bool) messages.M3UA {
	t.Helper()
	p, err := buildSSNMParameters(request.Scope, request.Destinations, request.Info)
	if err != nil {
		t.Fatal(err)
	}
	var cd *params.Param
	if request.ConcernedDestinationSet {
		cd = params.NewConcernedDestination(request.ConcernedDestination)
	}
	var level *params.Param
	if request.CongestionLevelSet {
		level = params.NewCongestionIndications(request.CongestionLevel)
	}
	if generic {
		var present []*params.Param
		for _, parameter := range []*params.Param{p.networkAppearance, p.routingContext, p.affectedPointCode, cd, level, p.info} {
			if parameter != nil {
				present = append(present, parameter)
			}
		}
		return messages.New(1, messages.MsgClassSSNM, messages.MsgTypeSignallingCongestion, present...)
	}
	return messages.NewSignallingCongestion(p.networkAppearance, p.routingContext, p.affectedPointCode, cd, level, p.info)
}

func TestConcernedSCONRawSendKeepsASPToSGPDirection(t *testing.T) {
	for _, role := range []Role{RoleSGP, RoleIPSP} {
		t.Run(fmt.Sprint(role), func(t *testing.T) {
			for _, path := range []string{"typed-message", "generic-message"} {
				c, sent := newTestConnWithContexts(t, StateASPActive, role, 1)
				c.noteRoutingContextsActive([]uint32{1})
				msg := messages.NewSignallingCongestion(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(100), params.NewConcernedDestination(200), nil, nil)
				var signal messages.M3UA = msg
				if path == "generic-message" {
					signal = messages.New(1, messages.MsgClassSSNM, messages.MsgTypeSignallingCongestion, msg.RoutingContext, msg.AffectedPointCode, msg.ConcernedDestination)
				}
				n, err := c.WriteSignal(signal)
				if n != 0 || !errors.Is(err, ErrInvalidParameterValue) || len(*sent) != 0 {
					t.Fatalf("role=%v path=%s n=%d err=%v sent=%d", role, path, n, err, len(*sent))
				}
			}
		})
	}
}

func TestConcernedSCONReceivePreservesAmbiguousCongestion(t *testing.T) {
	for _, destinations := range [][]PointCodeRange{
		{{PointCode: 0x123456}, {PointCode: 0x234567}},
		{{PointCode: 0x123400, Mask: 8}},
		{{PointCode: 0x123456}, {PointCode: 0x123456}},
	} {
		for _, cd := range []uint32{0, 0x654321} {
			for _, level := range []*params.Param{nil, params.NewCongestionIndications(0), params.NewCongestionIndications(3)} {
				t.Run(fmt.Sprintf("%v/CD=%x/level=%v", destinations, cd, level != nil), func(t *testing.T) {
					c, sent := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
					setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
					p, err := buildAffectedPointCodes(destinations)
					if err != nil {
						t.Fatal(err)
					}
					msg := messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(1), p, params.NewConcernedDestination(cd), level.Copy(), nil)
					raw, err := msg.MarshalBinary()
					if err != nil {
						t.Fatal(err)
					}
					c.dispatchRaw(context.Background(), inbound{data: raw, stream: 0, ppid: M3UAPPID})
					if err := firstErr(c); err != nil {
						t.Fatalf("accepted congestion produced protocol error: %v", err)
					}
					if len(*sent) != 0 {
						t.Fatalf("accepted congestion sent %v", typeNames(*sent))
					}
					report := nextSSNMReport(t, c)
					wantLevel := uint32(0)
					if level != nil {
						wantLevel = level.CongestionLevel()
					}
					if !report.PeerReported || !reflect.DeepEqual(report.Destinations, destinations) || report.CongestionLevelSet != (level != nil) || uint32(report.CongestionLevel) != wantLevel {
						t.Fatalf("lost valid congestion information: %+v", report)
					}
					if report.ConcernedDestinationSet || report.ConcernedDestination != 0 {
						t.Fatalf("ambiguous CD exposed as TFC trigger: %+v", report)
					}
					if report.UnmappedConcernedDestination == nil || *report.UnmappedConcernedDestination != cd {
						t.Fatalf("raw CD diagnostic is missing: %+v", report)
					}
					select {
					case ind := <-c.ManagementIndications():
						t.Fatalf("accepted mapping diagnostic must stay on SubscribeSSNM, got %+v", ind)
					default:
					}

				})
			}
		}
	}
}

func TestConcernedSCONFramingErrorPrecedesMapping(t *testing.T) {
	c, sent := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
	// Mandatory APC has one octet instead of complete 32-bit entries.
	raw := []byte{1, 0, 2, messages.MsgTypeSignallingCongestion, 0, 0, 0, 28, 0, 6, 0, 8, 0, 0, 0, 1, 0, 0x12, 0, 5, 0, 0, 0, 0, 2, 6, 0, 4}
	binary.BigEndian.PutUint32(raw[4:8], uint32(len(raw)))
	c.dispatchRaw(context.Background(), inbound{data: raw, stream: 0, ppid: M3UAPPID})
	reported := firstErr(c)
	var fault *ParameterFaultError
	if !errors.As(reported, &fault) || fault.Code != params.ErrParameterFieldError {
		t.Fatalf("malformed APC error=%v, want the earlier parameter framing error", reported)
	}
	if len(*sent) != 0 {
		t.Fatal("test recorder unexpectedly handled queued error")
	}
	assertNoSSNMReport(t, c)
	select {
	case ind := <-c.ManagementIndications():
		t.Fatalf("mapping diagnostic preceded malformed framing: %+v", ind)
	default:
	}
}

func TestConcernedSCONDiagnosticIsOwnedByEachSubscriber(t *testing.T) {
	c, _ := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
	_, second, err := c.endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close() }()
	if err := c.handleSignallingCongestion(messages.NewSignallingCongestion(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(100, 200), params.NewConcernedDestination(300), nil, nil)); err != nil {
		t.Fatal(err)
	}
	firstReport := nextSSNMReport(t, c)
	event, err := second.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if firstReport.UnmappedConcernedDestination == nil || event.Report.UnmappedConcernedDestination == nil {
		t.Fatal("missing unmapped diagnostic")
	}
	*firstReport.UnmappedConcernedDestination = 999
	if *event.Report.UnmappedConcernedDestination != 300 {
		t.Fatal("subscriber mutation changed another subscriber's diagnostic")
	}

}

func TestConcernedSCONDiagnosticDoesNotFillManagementQueue(t *testing.T) {
	c, _ := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
	c.ManagementIndications()
	for range 65 {
		if err := c.handleSignallingCongestion(messages.NewSignallingCongestion(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(100, 200), params.NewConcernedDestination(300), nil, nil)); err != nil {
			t.Fatal(err)
		}
		report := nextSSNMReport(t, c)
		if !reflect.DeepEqual(report.Destinations, []PointCodeRange{{PointCode: 100}, {PointCode: 200}}) {
			t.Fatalf("lost accepted congestion: %+v", report)
		}
	}
	if queued := len(c.ManagementIndications()); queued != 0 {
		t.Fatalf("accepted mapping warnings filled the management primitive queue: %d", queued)
	}
	select {
	case <-c.done:
		t.Fatal("accepted mapping diagnostics closed the association")
	default:
	}
}

func TestConcernedSCONCodecPreservesAmbiguousMapping(t *testing.T) {
	for _, destinations := range [][]PointCodeRange{{{PointCode: 100}, {PointCode: 200}}, {{PointCode: 0x123400, Mask: 8}}} {
		msg := concernedSCONMessage(t, SignallingCongestionRequest{Destinations: destinations, ConcernedDestination: 300, ConcernedDestinationSet: true}, true)
		raw, err := msg.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := messages.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := decoded.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(encoded, raw) {
			t.Fatal("serializer changed a captured CD/APC combination")
		}
	}
}

func TestConcernedSCONSendPreservesScopeErrors(t *testing.T) {
	for _, tc := range []struct {
		name               string
		scope              WireScope
		inactive           bool
		typedWant, rawWant error
	}{
		{"appearance", testWireScope(8, true, 1), false, ErrInvalidNetworkAppearance, ErrInvalidNetworkAppearance},
		{"unknown-context", testWireScope(7, true, 3), false, ErrInvalidRoutingContext, ErrInvalidRoutingContext},
		{"inactive-context", testWireScope(7, true, 1), true, ErrInvalidState, ErrRoutingContextNotActive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, path := range []string{"operation", "typed-message", "generic-message"} {
				t.Run(path, func(t *testing.T) {
					c, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
					setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
					if tc.inactive {
						c.noteRoutingContextsActive([]uint32{2})
						c.noteRoutingContextsAcked(params.NewRoutingContext(2))
					} else {
						c.noteRoutingContextsActive([]uint32{1, 2})
						c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
					}
					request := SignallingCongestionRequest{Scope: tc.scope, Destinations: []PointCodeRange{{PointCode: 100}, {PointCode: 200}}, ConcernedDestination: 300, ConcernedDestinationSet: true}
					var err error
					var n int
					want := tc.rawWant
					if path == "operation" {
						err = c.SignallingCongestion(request)
						want = tc.typedWant
					} else {
						n, err = c.WriteSignal(concernedSCONMessage(t, request, path == "generic-message"))
					}
					if !errors.Is(err, want) || n != 0 || len(*sent) != 0 {
						t.Fatalf("combined-invalid send n=%d err=%v sent=%d, want original scope error %v", n, err, len(*sent), want)
					}
				})
			}
		})
	}
}
