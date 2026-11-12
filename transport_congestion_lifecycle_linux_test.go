package m3ua

import (
	"context"
	"github.com/gomaja/go-sctp"
	"testing"
	"time"
)

func TestPrivateReconnectOwnerStopsTransportSampler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	peer := newRawPeer(t, 0, handshakeOnly)
	cfg := mcASPConfig(1)
	cfg.HeartbeatInfo = &HeartbeatInfo{Enabled: false}
	c, err := dialASP(ctx, "m3ua", mcAddr(0, "127.0.0.1"), peer.ln.Addr().(*sctp.Addr), cfg)
	if err != nil {
		t.Fatal(err)
	}
	e := c.endpoint
	t.Cleanup(func() { _ = e.Close() })
	if !waitFor(func() bool { return c.TransportCongestion().Samples > 0 }, 2*time.Second) {
		t.Fatal("sampler never started")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-e.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("private reconnect owner left the transport sampler running")
	}
	e.transport.workers.Wait()
}
