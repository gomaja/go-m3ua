package m3ua

import (
	"context"
	"github.com/gomaja/go-sctp"
	"sync"
	"sync/atomic"
)

func rkmReleaseCall(c *Association, ctx context.Context, registration bool) error {
	if registration {
		_, err := c.RegisterRoutingKeys(ctx, RoutingKeyRegistration{RoutingKey: testRoutingKey(10, 100, 3)})
		return err
	}
	_, err := c.DeregisterApplicationServers(ctx, ASKey{RoutingContext: 1, RoutingContextSet: true})
	return err
}

type heldRKMReleaseSend struct {
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
	err     error
}

func (w *heldRKMReleaseSend) SendMsg(_ []byte, _ sctp.SendOptions) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.resume
	return 0, w.err
}
func (w *heldRKMReleaseSend) SendMsgContext(_ context.Context, b []byte, opts sctp.SendOptions) (int, error) {
	return w.SendMsg(b, opts)
}

type postGateRKMContext struct {
	context.Context
	observed chan struct{}
	entered  chan struct{}
	resume   chan struct{}
	once     sync.Once
	calls    atomic.Int32
}

func (ctx *postGateRKMContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.observed) })
	return ctx.Context.Done()
}
func (ctx *postGateRKMContext) Err() error {
	if ctx.calls.Add(1) == 2 {
		close(ctx.entered)
		<-ctx.resume
	}
	return ctx.Context.Err()
}
