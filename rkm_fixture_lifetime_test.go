package m3ua

import (
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// Directly constructed procedure peers are not owned by an Endpoint.
func newRoutingKeyTestAssociation(t *testing.T, role Role, config *AssociationConfig) *Association {
	t.Helper()
	association := newAssociation(role, config)
	t.Cleanup(func() { _ = association.Close() })
	return association
}

func TestRoutingKeyFixtureRetiresNotificationWriter(t *testing.T) {
	var peer *Association
	t.Cleanup(func() {
		if peer != nil {
			_ = peer.Close()
		}
	})
	t.Run("notification", func(t *testing.T) {
		peer = newRoutingKeyTestAssociation(t, RoleSGP, NewAssociationConfig())
		peer.signalWriter = func(message messages.M3UA) (int, error) {
			return message.MarshalLen(), nil
		}
		message := messages.NewNotify(params.NewStatus(params.AsStateActive), nil, nil, nil)
		if err := peer.writeMandatoryControls([]messages.M3UA{message}, false, true); err != nil {
			t.Fatal(err)
		}
	})
	select {
	case <-peer.Done():
	default:
		t.Fatal("routing-key fixture left its notification writer live")
	}
}
