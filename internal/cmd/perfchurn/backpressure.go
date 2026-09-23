package main

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"time"
)

var errBackpressureTimeout = errors.New("transport kept refusing the message")

// backpressureRetry is the pause before a refused message is offered again.
const backpressureRetry = time.Millisecond

// transportRefused reports a DATA send the SCTP stack refused for a full send
// buffer. go-sctp sends with MSG_DONTWAIT and reports EAGAIN, and
// sctp_sendmsg queues a message in full or not at all (go-sctp v1.0.6
// sctp_linux.go, sendmsg), so nothing of a refused message is queued and
// offering it again cannot duplicate it. go-m3ua reports the refusal as
// DataNotSent from #115 on and as DataSendIndeterminate before it; both wrap
// the transport's EAGAIN, which is what is tested here, so the fixture reads
// both revisions the same way. Every other failure, including a not-sent
// admission refusal, is a real write failure and is never retried.
func transportRefused(err error) bool {
	return err != nil && (errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK))
}

// writeWithBackpressure offers one message until the transport accepts it,
// pausing between refusals, for at most maxWait. It is the application-level
// backpressure a sender needs without a socket write deadline, which would
// also bound the library's own control writes. It returns how many times the
// message was refused.
func writeWithBackpressure(ctx context.Context, maxWait time.Duration, write func() error) (int, error) {
	deadline := time.Now().Add(maxWait)
	refusals := 0
	for {
		err := write()
		if !transportRefused(err) {
			return refusals, err
		}
		refusals++
		if !time.Now().Before(deadline) {
			return refusals, fmt.Errorf("%w for %s: %w", errBackpressureTimeout, maxWait, err)
		}
		if err := sleepContext(ctx, backpressureRetry); err != nil {
			return refusals, err
		}
	}
}
