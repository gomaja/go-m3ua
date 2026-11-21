package m3ua

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

type closeOnReleaseContext struct {
	context.Context
	association *Association
	calls       atomic.Int32
	closeAt     int32
}

func (ctx *closeOnReleaseContext) Err() error {
	if ctx.calls.Add(1) == ctx.closeAt {
		if err := ctx.association.Close(); err != nil {
			return err
		}
	}
	return ctx.Context.Err()
}

func TestShutdownContextErrCanReenterClose(t *testing.T) {
	for _, closeAt := range []int32{1, 2} {
		t.Run(fmt.Sprint(closeAt), func(t *testing.T) {
			c, _ := newTestConn(t, StateASPDown, RoleASP)
			ctx := &closeOnReleaseContext{Context: context.Background(), association: c, closeAt: closeAt}
			err := c.ShutdownContext(ctx)
			if closeAt == 1 {
				if !errors.Is(err, ErrAssociationClosed) {
					t.Errorf("shutdown after reentrant Close=%v", err)
				}
			} else if err != nil {
				t.Errorf("shutdown unexpectedly rechecked context at release: %v", err)
			}
			if ctx.calls.Load() != 1 {
				t.Errorf("Err calls=%d, want baseline initial check only", ctx.calls.Load())
			}
			if !errors.Is(c.Err(), ErrAssociationClosed) || errors.Is(c.Err(), ErrOrderlyShutdown) {
				t.Errorf("Err after release=%v", c.Err())
			}
		})
	}
}
