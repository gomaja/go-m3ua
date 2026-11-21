package m3ua

import (
	"errors"
	"fmt"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// The methods advertise BEAT; only MarshalTo reveals the procedure header.
type encodedProcedureSignal struct {
	messages.M3UA
	raw []byte
}

func (m encodedProcedureSignal) MarshalLen() int          { return len(m.raw) }
func (m encodedProcedureSignal) MarshalTo(b []byte) error { copy(b, m.raw); return nil }

func procedureSignals(t *testing.T) []messages.M3UA {
	t.Helper()
	return []messages.M3UA{
		messages.NewAspUp(nil, nil), messages.NewAspDown(nil),
		messages.NewAspActive(nil, nil, nil), messages.NewAspInactive(nil, nil),
		messages.NewAspUpAck(nil, nil), messages.NewAspDownAck(nil),
		messages.NewAspActiveAck(nil, nil, nil), messages.NewAspInactiveAck(nil, nil),
		registrationRequestMessage(t), messages.NewRegistrationResponse(params.NewRegistrationResult(params.NewRegistrationResultPayload(params.NewLocalRoutingKeyIdentifier(1), params.NewRegistrationStatus(params.SuccessfullyRegistered), params.NewRoutingContext(1)))),
		messages.NewDeregistrationRequest(params.NewRoutingContext(1)), messages.NewDeregistrationResponse(params.NewDeregistrationResult(params.NewDeregResultPayload(params.NewRoutingContext(1), params.NewDeregistrationStatus(params.SuccessfullyDeregistered)))),
	}
}

func TestWriteSignalRefusesProcedureMessages(t *testing.T) {
	for _, role := range []Role{RoleASP, RoleSGP, RoleIPSP} {
		for _, model := range []IPSPExchangeModel{IPSPExchangeSingle, IPSPExchangeDouble} {
			if role != RoleIPSP && model == IPSPExchangeDouble {
				continue
			}
			for _, state := range []State{StateASPDown, StateASPInactive, StateASPActive} {
				for _, terminating := range []bool{false, true} {
					for _, message := range procedureSignals(t) {
						for _, encoding := range []string{"typed", "generic", "custom"} {
							t.Run(fmt.Sprintf("%s/%v/%s/shutdown=%v/%s/%s", role, model, state, terminating, message.MessageTypeName(), encoding), func(t *testing.T) {
								c, sent := newTestConn(t, state, role)
								c.cfg.IPSP = &IPSPConfig{ExchangeModel: model}
								c.terminating.Store(terminating)
								if role == RoleIPSP && model == IPSPExchangeDouble {
									c.setIPSPState(IPSPState{TrafficToLocal: state, TrafficToPeer: state})
								}
								signal := message
								switch encoding {
								case "generic":
									signal = messages.New(1, message.MessageClass(), message.MessageType())
								case "custom":
									raw, err := message.MarshalBinary()
									if err != nil {
										t.Fatal(err)
									}
									signal = encodedProcedureSignal{M3UA: messages.NewHeartbeat(nil), raw: raw}
								}
								n, err := c.WriteSignal(signal)
								if n != 0 || !errors.Is(err, ErrProcedureMessage) {
									t.Errorf("WriteSignal = %d, %v; want refusal", n, err)
								}
								if len(*sent) != 0 || c.pendingTAck() != 0 || c.State() != state || c.Err() != nil {
									t.Fatalf("refusal changed association: sent=%v pending=%d state=%v err=%v", typeNames(*sent), c.pendingTAck(), c.State(), c.Err())
								}
								select {
								case report := <-c.ManagementIndications():
									t.Errorf("unexpected indication: %+v", report)
								default:
								}
								before := len(*sent)
								if _, err := c.WriteSignal(messages.NewHeartbeat(nil)); err != nil || len(*sent) != before+1 {
									t.Fatalf("association unusable after refusal: %v", err)
								}
							})
						}
					}
				}
			}
		}
	}
}

func TestWriteSignalAllowsNonProcedureMessages(t *testing.T) {
	for _, message := range []messages.M3UA{
		messages.NewHeartbeat(nil), messages.NewHeartbeatAck(nil),
		messages.NewError(params.NewErrorCode(params.UnexpectedMessageError), nil, nil, nil, nil),
		messages.NewNotify(params.NewStatus(params.AsStateActive), nil, nil, nil),
		messages.NewDestinationStateAudit(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil),
		messages.NewData(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewProtocolData(1, 2, 3, 0, 0, 0, []byte("allowed")), nil),
	} {
		t.Run(message.MessageTypeName(), func(t *testing.T) {
			c, capture := newDataWriteAssociation(t, 1)
			c.signalWriter = func(m messages.M3UA) (int, error) {
				raw, err := m.MarshalBinary()
				if err != nil {
					return 0, err
				}
				return capture.write(raw, c.sctpInfo)
			}
			n, err := c.WriteSignal(message)
			if err != nil || n != message.MarshalLen() || capture.submissions() != 1 {
				t.Fatalf("WriteSignal = %d,%v; submissions=%d", n, err, capture.submissions())
			}
		})
	}
}

func TestWriteSignalRefusalPrecedesSendLock(t *testing.T) {
	c, _ := newTestConn(t, StateASPActive, RoleASP)
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	result := make(chan error, 1)
	go func() { _, err := c.WriteSignal(messages.NewAspUp(nil, nil)); result <- err }()
	if err := receiveASPTest(t, result, "raw refusal while sendMu is held"); !errors.Is(err, ErrProcedureMessage) {
		t.Fatal(err)
	}
}

func TestWriteSignalRefusalNeverSubmitsToTransport(t *testing.T) {
	for _, message := range procedureSignals(t) {
		t.Run(message.MessageTypeName(), func(t *testing.T) {
			c, _ := newTestConn(t, StateASPActive, RoleASP)
			transport := newFullSendBuffer()
			transport.firstErr = nil
			c.signalWriter = nil
			c.controlTransport = transport
			n, err := c.WriteSignal(message)
			if n != 0 || !errors.Is(err, ErrProcedureMessage) || len(transport.attempted) != 0 {
				t.Fatalf("refusal = %d,%v; submissions=%d", n, err, len(transport.attempted))
			}
			if _, err := c.WriteSignal(messages.NewHeartbeat(nil)); err != nil || len(transport.attempted) != 1 || c.Err() != nil {
				t.Fatalf("association unusable: %v", err)
			}
		})
	}
}
