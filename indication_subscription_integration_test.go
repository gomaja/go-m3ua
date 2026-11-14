package m3ua

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestIndicationSubscriptionsKeepNotifyTrafficAndDataAlive(t *testing.T) {
	for index, subscribed := range []bool{false, true, false} {
		name := "unsubscribed"
		if subscribed {
			name = "subscribed and draining"
		}
		if index == 2 {
			name = "callback without accessors"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var subscribe func(*Association)
			var states <-chan State
			var management <-chan *ManagementIndication
			var calls atomic.Int32
			if subscribed {
				subscribe = func(association *Association) {
					states = association.StateChanges()
					management = association.ManagementIndications()
				}
			} else if index == 2 {
				subscribe = func(*Association) { calls.Add(1) }
			}
			asp, sgp, err := setupConnHB(t, ctx, 27971+index,
				&HeartbeatInfo{Enabled: false}, &HeartbeatInfo{Enabled: false}, subscribe)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = asp.Close(); _ = sgp.Close() })
			if subscribed {
				assertHandshakeStates(t, handshakeSubscription{states: states})
				// Each of the two ASes reports INACTIVE and ACTIVE. Consume all
				// four before the test traffic: setup can return before these
				// queued notifications have been written by the SGP.
				for range 4 {
					receiveNotifyIndication(t, ctx, management)
				}
			}
			// RFC 4666 Section 3.8.2: AS-ACTIVE is status type 1, info 3.
			for number := range 140 {
				if _, err := sgp.WriteSignal(messages.NewNotify(
					params.NewStatus(params.AsStateActive), nil, params.NewRoutingContext(1), nil,
				)); err != nil {
					t.Fatalf("Notify %d: %v (ASP cause %v)", number+1, err, asp.Err())
				}
				if subscribed {
					// A nonblocking publisher can fill its queue before a reader
					// goroutine runs. Confirm consumption before sending again.
					indication := receiveNotifyIndication(t, ctx, management)
					if indication.StatusType != 1 || indication.StatusInfo != 3 ||
						len(indication.RoutingContexts) != 1 || indication.RoutingContexts[0] != 1 {
						t.Fatalf("Notify %d indication = %+v, want AS-ACTIVE for Routing Context 1", number+1, indication)
					}
				}
			}
			// BEAT shares ordered stream 0 with NTFY (RFC 4666 Section 1.4.7).
			// Its matching Ack proves all earlier notifications were dispatched
			// before checking DATA on a different stream.
			waitIndicationBarrier(t, ctx, asp, sgp, "after 140 notifications")
			select {
			case <-asp.Done():
				t.Fatalf("ASP ended after Notify traffic: %v", asp.Err())
			default:
			}
			if index == 2 && calls.Load() != 1 {
				t.Fatalf("Subscribe calls = %d, want 1", calls.Load())
			}
			if !subscribed && (len(asp.stateEventChan) != 0 || len(asp.mgmtChan) != 0) {
				t.Fatal("unsubscribed streams queued indications")
			}
			if got := asp.State(); got != StateASPActive {
				t.Fatalf("ASP state after Notify traffic = %v, want ASP-ACTIVE", got)
			}
			payload := []byte("DATA after 140 notifications")
			if _, err := sgp.WriteData(DataRequest{
				AS:           ASKey{NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true},
				ProtocolData: testProtocolData(payload),
			}); err != nil {
				t.Fatalf("write DATA: %v", err)
			}
			data, err := asp.ReadData(ctx)
			if err != nil {
				t.Fatalf("read DATA: %v", err)
			}
			if string(data.ProtocolData.Data) != string(payload) {
				t.Errorf("DATA payload = %q, want %q", data.ProtocolData.Data, payload)
			}
		})
	}
}

func waitIndicationBarrier(t *testing.T, ctx context.Context, asp, sgp *Association, label string) {
	t.Helper()
	barrier := []byte(label)
	sgp.setBeatData(barrier)
	if _, err := sgp.WriteSignal(messages.NewHeartbeat(params.NewHeartbeatData(barrier))); err != nil {
		t.Fatalf("BEAT barrier: %v (ASP cause %v)", err, asp.Err())
	}
	select {
	case <-sgp.beatAckChan:
	case <-ctx.Done():
		t.Fatalf("BEAT barrier was not acknowledged: %v (ASP cause %v)", ctx.Err(), asp.Err())
	}
}

func receiveNotifyIndication(t *testing.T, ctx context.Context, management <-chan *ManagementIndication) *ManagementIndication {
	t.Helper()
	for {
		select {
		case indication, ok := <-management:
			if !ok {
				t.Fatal("management subscription closed before M-NOTIFY")
			}
			if indication.Kind == ManagementTransportCongestion || indication.Kind == ManagementDataQueueCongestion {
				continue
			}
			if indication.Kind != ManagementNotify {
				t.Fatalf("management indication = %+v, want M-NOTIFY", indication)
			}
			return indication
		case <-ctx.Done():
			t.Fatalf("M-NOTIFY was not delivered: %v", ctx.Err())
			return nil
		}
	}
}
