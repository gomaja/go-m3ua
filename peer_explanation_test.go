package m3ua

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestPeerManagementExplanationsAreOwned(t *testing.T) {
	for _, kind := range []string{"ERR", "NTFY"} {
		for _, value := range [][]byte{nil, {}, {0, 0x61, 0x7f}} {
			name := "absent"
			if value != nil {
				name = "present"
				if len(value) == 0 {
					name = "empty"
				}
			}
			t.Run(kind+"/"+name, func(t *testing.T) {
				c, sent := newTestConn(t, StateASPInactive, RoleASP)
				var p *params.Param
				field := "DiagnosticInformation"
				if value != nil {
					p = &params.Param{Tag: params.DiagnosticInformation, Data: bytes.Clone(value)}
				}
				var msg messages.M3UA = messages.NewError(params.NewErrorCode(params.ErrProtocolError), nil, nil, nil, p)
				description := errorCodeName(params.ErrProtocolError)
				if kind == "NTFY" {
					field = "InfoString"
					if p != nil {
						p.Tag = params.InfoString
					}
					msg = messages.NewNotify(params.NewStatus(params.AsStatePending), nil, nil, p)
					description = notifyStatusName(params.AsStatePending)
				}
				raw, err := msg.MarshalBinary()
				if err != nil {
					t.Fatal(err)
				}
				parsed, err := messages.Parse(raw)
				if err != nil {
					t.Fatal(err)
				}
				c.handleSignals(context.Background(), parsed)
				var ind *ManagementIndication
				select {
				case ind = <-c.mgmtChan:
				default:
					t.Fatal("no management indication")
				}
				// Reflection lets this regression fail on missing behaviour, before the API exists.
				got := reflect.ValueOf(ind).Elem().FieldByName(field)
				if !got.IsValid() {
					t.Fatalf("management indication drops peer %s", field)
				}
				if got.Kind() != reflect.Slice || got.Type().Elem().Kind() != reflect.Uint8 {
					t.Fatalf("%s is not []byte", field)
				}
				if got.IsNil() != (value == nil) || !bytes.Equal(got.Bytes(), value) {
					t.Fatalf("%s = %v (nil %v), want %v (nil %v)", field, got.Bytes(), got.IsNil(), value, value == nil)
				}
				if ind.Description != description {
					t.Fatalf("description = %q, want %q", ind.Description, description)
				}
				// Parsed parameters alias raw input. Publication must detach from both.
				for i := range raw {
					raw[i] = 0xff
				}
				if !bytes.Equal(got.Bytes(), value) {
					t.Fatal("delivered explanation aliases parsed message")
				}
				if len(*sent) != 0 {
					t.Fatalf("received management message elicited %v", typeNames(*sent))
				}
				other := "InfoString"
				if kind == "NTFY" {
					other = "DiagnosticInformation"
				}
				if !reflect.ValueOf(ind).Elem().FieldByName(other).IsNil() {
					t.Fatalf("unrelated %s set", other)
				}
			})
		}
	}
}

func TestManagementPublicationCopiesPeerExplanations(t *testing.T) {
	c, _ := newTestConn(t, StateASPInactive, RoleASP)
	input := &ManagementIndication{DiagnosticInformation: []byte{0xff, 0}, InfoString: []byte("peer")}
	c.notifyManagement(input)
	input.DiagnosticInformation[0] = 0
	input.InfoString[0] = 'x'
	got := <-c.mgmtChan
	if !bytes.Equal(got.DiagnosticInformation, []byte{0xff, 0}) || string(got.InfoString) != "peer" {
		t.Fatal("publication aliases input explanations")
	}
}
