package m3ua

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-sctp"
)

func TestReadDataPreservesTheSocketReceiveTime(t *testing.T) {
	conn, _ := newDataWriteAssociation(t, 1)
	// Include a monotonic reading and zero as well as an old wall-clock time.
	// Copying the exact value must preserve all three without manufacturing one.
	for _, received := range []time.Time{time.Now().Add(-time.Hour), {}, time.Now()} {
		frame, err := inboundData(1, "timestamp").MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		conn.dispatchInbound(context.Background(), newInboundMessage(frame,
			sctp.RcvInfo{Stream: 1, PPID: M3UAPPID}, received))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		message, err := conn.ReadData(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if message.Received != received {
			t.Fatalf("Received = %v, want exact reader time %v", message.Received, received)
		}
	}
}

func TestReadDataReportsReceiveTimeBeforeTheApplicationWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const port = 3098
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	peers := mcConnect(t, ctx, ln, mcAddr(port, "127.0.0.1"), []string{"127.0.0.2"}, port)
	from, to := peers[0].asp, peers[0].sgp
	const count = 3
	beforeSend := time.Now()
	for i := range count {
		writeRetrying(t, from, []byte(fmt.Sprintf("received-%d", i)), 1)
	}
	afterSend := time.Now()
	// Allow one second for receive/dispatch under -race on a loaded host.
	// Waiting for the queue first makes every message spend the full two
	// seconds in it, independent of the scheduler during the send burst.
	arrivalLimit := afterSend.Add(time.Second)
	for to.DataQueueStats().Queued != count {
		if time.Now().After(arrivalLimit) {
			t.Fatal("DATA did not reach the receive queue within one second of sending")
		}
		time.Sleep(time.Millisecond)
	}
	const applicationWait = 2 * time.Second
	time.Sleep(applicationWait)
	for i := range count {
		message, err := to.ReadData(ctx)
		if err != nil {
			t.Fatal(err)
		}
		returned := time.Now()
		if got, want := string(message.ProtocolData.Data), fmt.Sprintf("received-%d", i); got != want {
			t.Fatalf("payload = %q, want %q", got, want)
		}
		if message.Received.Before(beforeSend) || message.Received.After(arrivalLimit) {
			t.Fatalf("Received = %v, want between %v and %v", message.Received, beforeSend, arrivalLimit)
		}
		if elapsed := returned.Sub(message.Received); elapsed < applicationWait {
			t.Fatalf("ReadData returned %v after Received, want at least %v", elapsed, applicationWait)
		}
		if message.Received == message.Received.Round(0) { //nolint:staticcheck // QF1009: Equal ignores the monotonic reading being tested.
			t.Fatal("Received lost its monotonic clock reading")
		}
		t.Logf("message %d: receive after first send %v; ReadData after receive %v", i,
			message.Received.Sub(beforeSend), returned.Sub(message.Received))
	}
}
