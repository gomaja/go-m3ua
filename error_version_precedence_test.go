package m3ua

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestUnsupportedVersionErrorIsNeverAnswered(t *testing.T) {
	for _, version := range []byte{0, 1, 2, 255} {
		for _, malformed := range []bool{false, true} {
			c, sent := newTestConn(t, StateASPActive, RoleSGP)
			raw, err := messages.NewError(params.NewErrorCode(params.ErrProtocolError), nil, nil, nil, nil).MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			raw[0] = version
			if malformed {
				raw[10], raw[11] = 255, 255
			}
			c.dispatchRaw(context.Background(), inbound{data: raw, ppid: M3UAPPID})
			for len(c.errChan) > 0 {
				if err := c.handleErrors(<-c.errChan); err != nil {
					t.Fatal(err)
				}
			}
			if got := errorCodes(*sent); len(got) != 0 {
				t.Fatalf("version %d malformed %v: answered Error with %v", version, malformed, got)
			}
		}
	}
}

func TestErrorInTransportFailureStateIsNeverAnswered(t *testing.T) {
	for _, state := range []State{StateSCTPCDI, StateSCTPRI} {
		c, sent := newTestConn(t, state, RoleSGP)
		raw, err := messages.NewError(params.NewErrorCode(params.ErrProtocolError), nil, nil, nil, nil).MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		c.dispatchRaw(context.Background(), inbound{data: raw, ppid: M3UAPPID})
		for len(c.errChan) > 0 {
			if err := c.handleErrors(<-c.errChan); err != nil {
				t.Fatal(err)
			}
		}
		if got := errorCodes(*sent); len(got) != 0 {
			t.Fatalf("state %v answered Error with codes %v", state, got)
		}
	}
}

func TestInvalidVersionPrecedesBodyDiagnostics(t *testing.T) {
	for _, body := range []struct {
		name        string
		class, kind byte
		payload     []byte
	}{
		{"invalid INFO", messages.MsgClassASPSM, messages.MsgTypeAspUp, []byte{0, byte(params.InfoString), 0, 5, 255, 0, 0, 0}},
		{"bad TLV length", messages.MsgClassASPSM, messages.MsgTypeAspUp, []byte{0, byte(params.InfoString), 255, 255}},
		{"missing parameter", messages.MsgClassTransfer, messages.MsgTypePayloadData, nil},
		{"unsupported class", 255, 1, nil},
		{"unsupported type", messages.MsgClassASPSM, 255, nil},
		{"disabled RKM", messages.MsgClassRKM, messages.MsgTypeRegistrationRequest, nil},
	} {
		t.Run(body.name, func(t *testing.T) {
			c, sent := newTestConn(t, StateASPDown, RoleSGP)
			raw := append([]byte{2, 0, body.class, body.kind, 0, 0, 0, 0}, body.payload...)
			binary.BigEndian.PutUint32(raw[4:8], uint32(len(raw)))
			c.dispatchRaw(context.Background(), inbound{data: raw, ppid: M3UAPPID})
			select {
			case fault := <-c.errChan:
				if err := c.handleErrors(fault); err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatal("no Invalid Version response")
			}
			if got := errorCodes(*sent); len(got) != 1 || got[0] != params.InvalidVersionError {
				t.Fatalf("reply codes %v, want Invalid Version", got)
			}
			if (*sent)[0].Version() != 1 {
				t.Fatal("reply did not advertise supported version 1")
			}
			select {
			case indication := <-c.mgmtChan:
				if indication.Kind != ManagementError || indication.ErrorCode != params.InvalidVersionError {
					t.Fatalf("management indication %+v", indication)
				}
			default:
				t.Fatal("no Invalid Version management indication")
			}
			if c.State() != StateASPDown {
				t.Fatal("version rejection changed state")
			}
		})
	}
}

func TestInvalidVersionStillRequiresCommonHeaderFraming(t *testing.T) {
	for _, raw := range [][]byte{
		{2, 0, 3, 1},
		{2, 0, 3, 1, 0, 0, 0, 7},
		{2, 0, 3, 1, 0, 0, 0, 12},
		{2, 0, 3, 1, 0, 0, 0, 8, 0, 0, 0, 0},
		{2, 0, 255, 1, 0, 0, 0, 12},
		{2, 0, 3, 255, 0, 0, 0, 12},
	} {
		c, sent := newTestConn(t, StateASPDown, RoleSGP)
		c.dispatchRaw(context.Background(), inbound{data: raw, ppid: M3UAPPID})
		for len(c.errChan) > 0 {
			if err := c.handleErrors(<-c.errChan); err != nil {
				t.Fatal(err)
			}
		}
		want := 1
		if len(raw) < 8 {
			want = 0
		}
		if got := errorCodes(*sent); len(got) != want || (want != 0 && got[0] != params.ErrProtocolError) {
			t.Fatalf("raw %x: reply codes %v", raw, got)
		}
		if c.lastRecv.Load() != 0 {
			t.Fatal("bad framing counted as activity")
		}
	}
	c, sent := newTestConn(t, StateASPDown, RoleSGP)
	c.dispatchRaw(context.Background(), inbound{data: []byte{2, 0, 3, 1, 0, 0, 0, 8}, ppid: 123})
	if len(*sent) != 0 || len(c.errChan) != 0 || c.lastRecv.Load() != 0 {
		t.Fatal("foreign PPID reached M3UA")
	}
}
