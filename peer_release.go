// Copyright 2026 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
)

// peerRelease keeps a failed send from consuming the SCTP end indication.
// SCTP_COMM_LOST carries the peer's ABORT cause (RFC 6458 §6.1.1;
// RFC 9260 §§9.1, 11.2.5), while a write can report only an errno.
// mu is never held across I/O, teardown or a blocking channel operation.
type peerRelease struct {
	mu       sync.Mutex
	started  bool
	drain    chan struct{}
	writeErr error
	readErr  error
}

func (p *peerRelease) startReader() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.drain == nil {
		p.drain = make(chan struct{})
	}
	p.started = true
	return p.drain
}

func (p *peerRelease) drainSignal() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.drain
}

func (p *peerRelease) failedWrite(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.started {
		return
	}
	if p.writeErr == nil {
		p.writeErr = err
		close(p.drain)
	}
}

func (p *peerRelease) writeFailure() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.writeErr
}

// finishedRead publishes before monitor's handoff. A writer either finds
// this cause, or asks the still-running reader to finish the teardown.
func (p *peerRelease) finishedRead(err error) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.readErr = err
	return p.writeErr != nil
}

func (p *peerRelease) endCause(cause error) (error, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.writeErr == nil {
		return cause, false
	}
	return p.readErr, p.readErr == nil
}

func sctpAssociationFailure(err error) bool {
	return errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.ENOTCONN) ||
		errors.Is(err, syscall.ETIMEDOUT) || errors.Is(err, syscall.ESHUTDOWN)
}

func peerReadCause(err error) error {
	if !errors.Is(err, ErrSCTPNotAlive) && sctpAssociationFailure(err) {
		// A kernel that supplies no association-change cause still reports
		// transport loss; retain its errno as well as the public loss category.
		return fmt.Errorf("%w: %w", ErrSCTPNotAlive, err)
	}
	return err
}

func localEndCause(cause error) bool {
	return errors.Is(cause, ErrAssociationClosed) || errors.Is(cause, context.Canceled) ||
		errors.Is(cause, context.DeadlineExceeded)
}
