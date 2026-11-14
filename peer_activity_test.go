package m3ua

import (
	"context"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestAcceptedUnsupportedRKMRefreshesPeerActivity(t *testing.T) {
	key, err := routingKeyParameter(RoutingKeyRegistrationRequest{
		LocalRoutingKeyIdentifier: 77,
		RoutingKey:                testRoutingKey(10, 100, params.ServiceIndSCCP),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range []Role{RoleSGP, RoleIPSP} {
		for _, message := range []messages.M3UA{
			messages.NewRegistrationRequest(key),
			messages.NewDeregistrationRequest(params.NewRoutingContext(1)),
			// The disabled capability boundary accepts these for an Error
			// without interpreting their body, so they are activity too.
			messages.New(1, messages.MsgClassRKM, messages.MsgTypeRegistrationRequest),
		} {
			c, _ := newTestConn(t, StateASPActive, role)
			raw, err := message.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if !c.rkmResponderUnsupported(raw) {
				t.Fatal("fixture is not RKM-disabled")
			}
			c.lastRecv.Store(1)
			c.dispatchRaw(context.Background(), inbound{data: raw, ppid: M3UAPPID})
			if c.lastRecv.Load() <= 1 {
				t.Errorf("role %v type %d: accepted RKM did not refresh activity", role, message.MessageType())
			}
			if firstErr(c) == nil {
				t.Fatal("accepted unsupported RKM produced no Error")
			}
		}
	}
}
