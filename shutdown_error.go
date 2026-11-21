package m3ua

import (
	"context"
	"errors"
	"fmt"
)

// aspRequestContext observes initial requests without changing their contexts.
// Only the public caller goroutine writes or reads requests. owner shares the
// observation with ASP Down's background-context write, as on the baseline.
// Outcome flags are also written/read only by that caller goroutine.
type aspRequestContext struct {
	context.Context
	requests           []*pendingRequest
	owner              *aspRequestContext
	releaseObserved    bool
	callerContextError bool
}

// labelShutdownError preserves the selected baseline error chain (RFC 4666
// §4.2), adding the orderly-withdrawal cause only at the public boundary (§4.9).
// Caller-context outcomes were recorded at selection; this helper calls no
// context method and runs after procedure locks/gates are released.
func (c *Association) labelShutdownError(err error, tracking *aspRequestContext) error {
	if err == nil || c == nil {
		return err
	}
	if tracking != nil && tracking.callerContextError {
		return err
	}
	if errors.Is(err, errASPProcedureShutdown) {
		return err
	}
	orderly := tracking != nil && tracking.releaseObserved && c.orderlyRelease.Load()
	var requests []*pendingRequest
	if tracking != nil {
		requests = tracking.requests
	}
	for _, request := range requests {
		if request.shutdownCancelled.Load() {
			orderly = true
			break
		}
	}
	if !orderly {
		return err
	}
	return fmt.Errorf("%w: %w", errASPProcedureShutdown, err)
}

// procedureObservation only inspects our concrete wrapper; it never calls a
// caller-supplied context method, including when used under a procedure lock.
func procedureObservation(ctx context.Context) *aspRequestContext {
	tracking, _ := ctx.(*aspRequestContext)
	if tracking != nil && tracking.owner != nil {
		tracking = tracking.owner
	}
	return tracking
}

// observeRelease is called only where a release-sensitive outcome is selected,
// never at the public boundary. The winner and closeErr are published before
// Done, so they also cover release in progress. The winner is already immutable.
func (tracking *aspRequestContext) observeRelease(c *Association) {
	if tracking == nil {
		return
	}
	if tracking.owner != nil {
		tracking = tracking.owner
	}
	if c.orderlyRelease.Load() || c.closeErr.Load() != nil {
		tracking.releaseObserved = true
		return
	}
	select {
	case <-c.done:
		tracking.releaseObserved = true
	default:
	}
}
