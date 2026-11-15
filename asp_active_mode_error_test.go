package m3ua

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestUndefinedASPActiveModeDrawsUnsupportedMode(t *testing.T) {
	for _, mode := range []uint32{0, 4, 0xffffffff} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			raw, err := messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(1), nil).MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			binary.BigEndian.PutUint32(raw[12:16], mode)
			c, sent := newTestConn(t, StateASPInactive, RoleSGP)
			c.dispatchRaw(context.Background(), inbound{data: raw, ppid: M3UAPPID})
			select {
			case e := <-c.errChan:
				if err := c.handleErrors(e); err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatal("no rejection")
			}
			if codes := errorCodes(*sent); len(codes) != 1 || codes[0] != params.ErrUnsupportedTrafficModeType {
				t.Fatalf("error codes = %v, want [5]", codes)
			}
			if len(*sent) != 1 || c.State() != StateASPInactive || len(c.activeRCs) != 0 || c.contextlessASActive {
				t.Fatalf("activation or extra reply: state %v messages %v", c.State(), typeNames(*sent))
			}
		})
	}
}

func TestASPActiveModeErrorKeepsParameterPrecedence(t *testing.T) {
	mode := []byte{0, byte(params.TrafficModeType), 0, 8, 0, 0, 0, 4}
	invalidInfo := []byte{0, byte(params.InfoString), 0, 5, 0xff, 0, 0, 0}
	for _, tc := range []struct {
		name       string
		class, typ byte
		payload    []byte
		code       uint32
	}{
		{"invalid INFO before mode", messages.MsgClassASPTM, messages.MsgTypeAspActive, append(append([]byte{}, invalidInfo...), mode...), params.ErrInvalidParameterValue},
		{"invalid INFO after mode", messages.MsgClassASPTM, messages.MsgTypeAspActive, append(append([]byte{}, mode...), invalidInfo...), params.ErrUnsupportedTrafficModeType},
		{"short mode", messages.MsgClassASPTM, messages.MsgTypeAspActive, []byte{0, byte(params.TrafficModeType), 0, 7, 0, 0, 4, 0}, params.ErrParameterFieldError},
		{"overrun mode", messages.MsgClassASPTM, messages.MsgTypeAspActive, []byte{0, byte(params.TrafficModeType), 0, 12, 0, 0, 0, 4}, params.ErrParameterFieldError},
		{"nested Routing Key mode stays strict", messages.MsgClassASPTM, messages.MsgTypeAspActive, append([]byte{0x02, 0x07, 0, 12}, mode...), params.ErrInvalidParameterValue},
		{"ACK stays strict", messages.MsgClassASPTM, messages.MsgTypeAspActiveAck, mode, params.ErrInvalidParameterValue},
		{"other message stays strict", messages.MsgClassASPSM, messages.MsgTypeAspUp, mode, params.ErrInvalidParameterValue},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := append([]byte{1, 0, tc.class, tc.typ, 0, 0, 0, 0}, tc.payload...)
			binary.BigEndian.PutUint32(raw[4:8], uint32(len(raw)))
			c, sent := newTestConn(t, StateASPInactive, RoleSGP)
			c.dispatchRaw(context.Background(), inbound{data: raw, ppid: M3UAPPID})
			select {
			case err := <-c.errChan:
				if err := c.handleErrors(err); err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatal("no rejection")
			}
			if codes := errorCodes(*sent); len(codes) != 1 || codes[0] != tc.code {
				t.Fatalf("codes = %v, want [%d]", codes, tc.code)
			}
			if len(*sent) != 1 || c.State() != StateASPInactive {
				t.Fatal("unexpected reply or activation")
			}
		})
	}
}
