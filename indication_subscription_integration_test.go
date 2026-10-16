package m3ua

import (
	"context"
	"sync"
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
			var calls atomic.Int32
			if index == 2 {
				subscribe = func(*Association) { calls.Add(1) }
			}
			asp, sgp, err := setupConnHB(t, ctx, 27971+index,
				&HeartbeatInfo{Enabled: false}, &HeartbeatInfo{Enabled: false}, subscribe)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = asp.Close(); _ = sgp.Close() })
			if subscribed {
				states, management := asp.StateChanges(), asp.ManagementIndications()
				var readers sync.WaitGroup
				readers.Go(func() {
					for range states {
					}
				})
				readers.Go(func() {
					for range management {
					}
				})
				t.Cleanup(func() { _ = asp.Close(); readers.Wait() })
			}
			// RFC 4666 Section 3.8.2: AS-ACTIVE is status type 1, info 3.
			for number := range 140 {
				if _, err := sgp.WriteSignal(messages.NewNotify(
					params.NewStatus(params.AsStateActive), nil, params.NewRoutingContext(1), nil,
				)); err != nil {
					t.Fatalf("Notify %d: %v (ASP cause %v)", number+1, err, asp.Err())
				}
			}
			// BEAT shares ordered stream 0 with NTFY (RFC 4666 Section 1.4.7).
			// Its matching Ack proves all earlier notifications were dispatched
			// before checking DATA on a different stream.
			barrier := []byte("after 140 notifications")
			sgp.setBeatData(barrier)
			if _, err := sgp.WriteSignal(messages.NewHeartbeat(params.NewHeartbeatData(barrier))); err != nil {
				t.Fatalf("BEAT barrier: %v (ASP cause %v)", err, asp.Err())
			}
			select {
			case <-sgp.beatAckChan:
			case <-ctx.Done():
				t.Fatalf("BEAT barrier was not acknowledged: %v (ASP cause %v)", ctx.Err(), asp.Err())
			}
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
