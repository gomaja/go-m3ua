// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// managementData constructs a synthetic received DATA with the requested SI.
func managementData(t *testing.T, si uint8) (*messages.Data, []byte) {
	t.Helper()
	d := messages.NewData(nil, params.NewRoutingContext(1), params.NewProtocolData(200, 100, 3, 0, 0, 1, []byte{0x14, 0, 0}), nil)
	raw, err := d.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	// Common header + Routing Context + Protocol Data header + OPC/DPC.
	raw[28] = si
	decoded, err := messages.ParseData(raw)
	if err != nil {
		t.Fatal(err)
	}
	return decoded, raw
}

// encodedDataSignal deliberately bypasses Data's encoder to exercise the raw
// WriteSignal admission boundary used by custom M3UA implementations.
type encodedDataSignal struct {
	*messages.Data
	raw []byte
}

func (d encodedDataSignal) MarshalTo(b []byte) error { copy(b, d.raw); return nil }
func (d encodedDataSignal) MarshalLen() int          { return len(d.raw) }

func TestOmittedServiceIndicatorExcludesMTPManagement(t *testing.T) {
	key, err := canonicalizeRoutingKey(testRoutingKey(10, 100))
	if err != nil {
		t.Fatal(err)
	}
	for si := 0; si < 256; si++ {
		if got := key.matchesTraffic(10, true, 200, 100, uint8(si)); got != (si != 0) {
			t.Errorf("SI %d matched=%v, want %v", si, got, si != 0)
		}
	}
}

func TestDataRefusesMTPManagementServiceIndicator(t *testing.T) {
	for _, path := range []string{"WriteData", "WriteDataContext", "WriteSignal", "generic WriteSignal", "raw WriteSignal"} {
		t.Run(path, func(t *testing.T) {
			c, capture := newDataWriteAssociation(t, 1)
			signals := 0
			c.signalWriter = func(m messages.M3UA) (int, error) { signals++; return m.MarshalLen(), nil }
			request := DataRequest{AS: writeScope(1), Stream: 1, ProtocolData: params.ProtocolDataPayload{OriginatingPointCode: 200, DestinationPointCode: 100, Data: []byte{0x14, 0, 0}}}
			var n int
			var err error
			switch path {
			case "WriteData":
				n, err = c.WriteData(request)
			case "WriteDataContext":
				n, err = c.WriteDataContext(context.Background(), request)
			default:
				d, raw := managementData(t, 0)
				switch path {
				case "WriteSignal":
					n, err = c.WriteSignal(d)
				case "generic WriteSignal":
					n, err = c.WriteSignal(messages.New(1, messages.MsgClassTransfer, messages.MsgTypePayloadData, d.RoutingContext, d.ProtocolData))
				default:
					n, err = c.WriteSignal(encodedDataSignal{d, raw})
				}
			}
			requireManagementDataNotSent(t, err)
			if !errors.Is(err, ErrMTPManagementData) || n != 0 || capture.submissions() != 0 || signals != 0 {
				t.Fatalf("n=%d err=%v submissions=%d", n, err, capture.submissions())
			}
		})
	}
}

func TestMTPTransferRefusesManagementBeforeRouteSelection(t *testing.T) {
	for _, cancellable := range []bool{false, true} {
		t.Run(fmt.Sprint(cancellable), func(t *testing.T) {
			e, _, captures := newASPTransferFixture(t, validASPConfig())
			pd := transferProtocolData(0x123456, 1, []byte("management"))
			pd.ServiceIndicator = 0
			req := MTPTransferRequest{MTPRoute: "missing", ProtocolData: pd}
			var err error
			if cancellable {
				_, err = e.MTPTransferContext(context.Background(), req)
			} else {
				_, err = e.MTPTransfer(req)
			}
			if err == nil {
				t.Fatal("management accepted")
			}
			requireManagementDataNotSent(t, err)
			// A routing failure would hide the missing admission check.
			if !errors.Is(err, ErrMTPManagementData) || !errors.Is(err, ErrInvalidMTPTransfer) {
				t.Fatalf("route selected before SI validation: %v", err)
			}
			for name, capture := range captures {
				if capture.count() != 0 {
					t.Fatalf("%s received management DATA", name)
				}
			}
		})
	}
}

func TestDistributionRefusesMTPManagement(t *testing.T) {
	for _, path := range []string{"Listener", "Association"} {
		t.Run(path, func(t *testing.T) {
			l, as, c, sent := distributionFixture(t, params.TrafficModeLoadshare)
			as.setASPState(c, StateASPActive, time.Hour)
			d := distributionData(999, 1, "management")
			d.ProtocolData.Data[8] = 0
			var err error
			if path == "Listener" {
				_, err = l.DistributeData(d)
			} else {
				_, err = c.DistributeData(d)
			}
			requireManagementDataNotSent(t, err)
			if !errors.Is(err, ErrMTPManagementData) || len(sent.data()) != 0 {
				t.Fatalf("err=%v writes=%d", err, len(sent.data()))
			}
		})
	}
}

func TestInboundMTPManagementIsRejected(t *testing.T) {
	for _, role := range []Role{RoleASP, RoleSGP, RoleIPSP} {
		t.Run(fmt.Sprint(role), func(t *testing.T) {
			c, sent := newTestConnWithContexts(t, StateASPActive, role, 1)
			c.noteRoutingContextsAcked(params.NewRoutingContext(1))
			c.noteRoutingContextsActive([]uint32{1})
			d, raw := managementData(t, 0)
			indications := c.ManagementIndications()
			wantRaw := bytes.Clone(raw)
			c.handleData(context.Background(), d, raw)
			raw[len(raw)-1] ^= 0xff
			select {
			case <-c.dataChan:
				t.Fatal("management reached M3UA user")
			default:
			}
			err := firstErr(c)
			if !errors.Is(err, ErrMTPManagementData) {
				t.Fatalf("management error = %v", err)
			}
			if err := c.handleErrors(err); err != nil {
				t.Fatal(err)
			}
			select {
			case ind := <-indications:
				if ind.Kind != ManagementError || ind.ErrorCode != params.ErrInvalidParameterValue || !errors.Is(ind.Cause, ErrMTPManagementData) {
					t.Fatalf("indication=%+v", ind)
				}
				if len(ind.RoutingContexts) != 1 || ind.RoutingContexts[0] != 1 || len(ind.ASKeys) != 1 || ind.ASKeys[0] != c.receivedASKey(d) || ind.NetworkAppearanceSet {
					t.Fatalf("indication scope=%+v", ind)
				}
			default:
				t.Fatal("no Layer Management indication")
			}
			if codes := errorCodes(*sent); len(codes) != 1 || codes[0] != params.ErrInvalidParameterValue {
				t.Fatalf("ERR codes=%v", codes)
			}
			for _, m := range *sent {
				if e, ok := m.(*messages.Error); ok {
					if e.DiagnosticInformation == nil || !bytes.Equal(e.DiagnosticInformation.DiagnosticInformation(), wantRaw) {
						t.Fatalf("diagnostic=%v want %x", e.DiagnosticInformation, wantRaw)
					}
				}
			}
		})
	}
}

func TestNonManagementServiceIndicatorsRemainData(t *testing.T) {
	for _, si := range []uint8{1, 2, 3} {
		t.Run(fmt.Sprint(si), func(t *testing.T) {
			c, capture := newDataWriteAssociation(t, 1)
			if _, err := c.WriteData(DataRequest{AS: writeScope(1), ProtocolData: params.ProtocolDataPayload{ServiceIndicator: si, Data: []byte("user")}}); err != nil {
				t.Fatal(err)
			}
			if capture.submissions() != 1 {
				t.Fatal("DATA not submitted")
			}
			signal, raw := managementData(t, si)
			if _, err := c.WriteSignal(signal); err != nil {
				t.Fatalf("typed signal SI %d: %v", si, err)
			}
			if _, err := c.WriteSignal(encodedDataSignal{signal, raw}); err != nil {
				t.Fatalf("raw signal SI %d: %v", si, err)
			}
			config := validASPConfig()
			config.Routing.MTPRoutes[0].ServiceIndicators = nil
			endpoint, _, _ := newASPTransferFixture(t, config)
			pd := transferProtocolData(0x123456, 1, []byte("user"))
			pd.ServiceIndicator = si
			if _, err := endpoint.MTPTransfer(MTPTransferRequest{ProtocolData: pd}); err != nil {
				t.Fatalf("Endpoint SI %d: %v", si, err)
			}
			l, as, asp, sent := distributionFixture(t, params.TrafficModeLoadshare)
			as.setASPState(asp, StateASPActive, time.Hour)
			d := distributionData(1, 1, "user")
			d.ProtocolData.Data[8] = si
			if _, err := l.DistributeData(d); err != nil {
				t.Fatalf("distribution SI %d: %v", si, err)
			}
			if len(sent.data()) != 1 {
				t.Fatal("distribution dropped user traffic")
			}

			receiver, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
			receiver.noteRoutingContextsAcked(params.NewRoutingContext(1))
			d, raw = managementData(t, si)
			receiver.handleData(context.Background(), d, raw)
			select {
			case got := <-receiver.dataChan:
				if got.ProtocolData.ServiceIndicator != si {
					t.Fatal("SI changed")
				}
			default:
				t.Fatal("DATA not delivered")
			}
		})
	}
}

func TestRegisteredRoutingKeyOmittedSIExcludesManagement(t *testing.T) {
	rk := testRoutingKey(10, 100)
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus { return RegistrationSuccessfullyRegistered }, ProvisionedRoutingKeys: []ProvisionedRoutingKey{{RoutingContext: 1, RoutingKey: rk}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, si := range []uint8{0, 1, 2, 3} {
		matches, configured := registry.matchingASKeys(10, true, 200, 100, si)
		want := 1
		if si == 0 {
			want = 0
		}
		if !configured || len(matches) != want {
			t.Fatalf("SI %d matches=%v configured=%v", si, matches, configured)
		}
		d := messages.NewData(params.NewNetworkAppearance(10), nil, params.NewProtocolData(200, 100, si, 0, 0, 1, nil), nil)
		pd, err := d.ProtocolData.ProtocolData()
		if err != nil {
			t.Fatal(err)
		}
		key, err := resolveDistributionRoutingContext(newApplicationServers(time.Hour), registry, d, pd)
		if si == 0 {
			if !errors.Is(err, ErrNoMatchingRoutingKey) {
				t.Fatalf("SI 0 distribution selected %+v, err=%v", key, err)
			}
		} else if err != nil || key.RoutingContext != 1 {
			t.Fatalf("SI %d selected %+v err=%v", si, key, err)
		}
	}
}

func FuzzRoutingKeyServiceIndicator(f *testing.F) {
	for _, si := range []uint8{0, 1, 2, 3, 255} {
		f.Add(si, false)
		f.Add(si, true)
	}
	f.Fuzz(func(t *testing.T, si uint8, explicit bool) {
		rk := testRoutingKey(10, 100)
		if explicit {
			rk.Groups[0].ServiceIndicators = []uint8{si}
		}
		key, err := canonicalizeRoutingKey(rk)
		if explicit && si == 0 {
			if err == nil {
				t.Fatal("explicit SI 0 accepted")
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if got := key.matchesTraffic(10, true, 200, 100, si); got != (si != 0) {
			t.Fatalf("SI %d match=%v", si, got)
		}
	})
}

func requireManagementDataNotSent(t *testing.T, err error) {
	t.Helper()
	var failure *DataWriteError
	if !errors.As(err, &failure) || failure.Outcome != DataNotSent || !errors.Is(err, ErrMTPManagementData) {
		t.Fatalf("error=%T %v, want DataWriteError/DataNotSent/ErrMTPManagementData", err, err)
	}
}

func TestDiagnosticInformationCopiesOnlyRetainedBytes(t *testing.T) {
	for _, size := range []int{maxDiagnosticInformationLen, maxDiagnosticInformationLen + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			raw := bytes.Repeat([]byte{0x7a}, size)
			stored := diagnosticInformation(raw)
			if len(stored) != maxDiagnosticInformationLen || cap(stored) != maxDiagnosticInformationLen {
				t.Fatalf("stored len=%d cap=%d want %d", len(stored), cap(stored), maxDiagnosticInformationLen)
			}
			raw[0] = 0
			if stored[0] != 0x7a {
				t.Fatal("diagnostic aliases original input")
			}
		})
	}
}

func TestMTPManagementDataRetainsEarlierReceiveErrors(t *testing.T) {
	for _, role := range []Role{RoleASP, RoleSGP, RoleIPSP} {
		for _, earlier := range []string{"state", "stream", "network appearance", "routing context"} {
			t.Run(role.String()+"/"+earlier, func(t *testing.T) {
				state := StateASPActive
				if earlier == "state" {
					state = StateASPInactive
				}
				c, sent := newTestConnWithContexts(t, state, role, 1)
				setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
				c.noteRoutingContextsAcked(params.NewRoutingContext(1))
				c.noteRoutingContextsActive([]uint32{1})
				data, _ := managementData(t, 0)
				want := uint32(params.UnexpectedMessageError)
				switch earlier {
				case "stream":
					c.recvStream.Store(0)
					want = params.ErrInvalidStreamIdentifier
				case "network appearance":
					data.NetworkAppearance = params.NewNetworkAppearance(8)
					want = params.ErrInvalidNetworkAppearance
					// An ASP accepts unknown NA values under the existing
					// policy; a malformed NA still has an earlier shape error.
					if role == RoleASP {
						data.NetworkAppearance = &params.Param{Tag: params.NetworkAppearance, Data: []byte{1}}
						want = params.ErrParameterFieldError
					}
				case "routing context":
					data.RoutingContext = params.NewRoutingContext(999)
					want = params.ErrInvalidRoutingContext
				}
				c.handleData(context.Background(), data, nil)
				failure := firstErr(c)
				if failure == nil || errors.Is(failure, ErrMTPManagementData) {
					t.Fatalf("earlier %s error=%v", earlier, failure)
				}
				switch earlier {
				case "state":
					var typed *UnexpectedMessageError
					if !errors.As(failure, &typed) {
						t.Fatalf("error=%T want UnexpectedMessageError", failure)
					}
				case "stream":
					var typed *InvalidSCTPStreamIDError
					if !errors.As(failure, &typed) {
						t.Fatalf("error=%T want InvalidSCTPStreamIDError", failure)
					}
				case "network appearance":
					if role == RoleASP {
						var typed *ParameterFaultError
						if !errors.As(failure, &typed) || typed.Code != params.ErrParameterFieldError {
							t.Fatal(failure)
						}
					} else if !errors.Is(failure, ErrInvalidNetworkAppearance) {
						t.Fatal(failure)
					}
				case "routing context":
					if !errors.Is(failure, ErrInvalidRoutingContext) {
						t.Fatal(failure)
					}
				}
				if err := c.handleErrors(failure); err != nil {
					t.Fatal(err)
				}
				codes := errorCodes(*sent)
				if len(codes) != 1 || codes[0] != want {
					t.Fatalf("codes=%v want [%d]", codes, want)
				}
				select {
				case <-c.dataChan:
					t.Fatal("management DATA delivered")
				default:
				}
			})
		}
	}
}

func TestRepeatedMTPManagementDataReceivesOneErrorEach(t *testing.T) {
	for _, role := range []Role{RoleASP, RoleSGP, RoleIPSP} {
		t.Run(role.String(), func(t *testing.T) {
			c, sent := newTestConnWithContexts(t, StateASPActive, role, 1)
			c.noteRoutingContextsAcked(params.NewRoutingContext(1))
			c.noteRoutingContextsActive([]uint32{1})
			indications := c.ManagementIndications()
			for i := 0; i < 3; i++ {
				data, raw := managementData(t, 0)
				c.handleData(context.Background(), data, raw)
				failure := firstErr(c)
				if !errors.Is(failure, ErrMTPManagementData) {
					t.Fatalf("message %d error=%v", i, failure)
				}
				if err := c.handleErrors(failure); err != nil {
					t.Fatal(err)
				}
				if firstErr(c) != nil {
					t.Fatal("more than one error per DATA")
				}
				select {
				case ind := <-indications:
					if ind.Kind != ManagementError || ind.ErrorCode != params.ErrInvalidParameterValue {
						t.Fatalf("indication=%+v", ind)
					}
				default:
					t.Fatal("missing management indication")
				}
				codes := errorCodes(*sent)
				if len(codes) != i+1 {
					t.Fatalf("message %d: ERR codes=%v", i, codes)
				}
				for _, code := range codes {
					if code != params.ErrInvalidParameterValue {
						t.Fatalf("ERR=%d", code)
					}
				}
			}
			select {
			case <-c.dataChan:
				t.Fatal("management DATA delivered")
			default:
			}
		})
	}
}
