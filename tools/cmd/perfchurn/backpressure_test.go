package main

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

// A full send buffer is reported as not sent from #115 on and as
// indeterminate before it; both wrap the transport's EAGAIN.
func refusal(outcome m3ua.DataSendOutcome) error {
	return &m3ua.DataWriteError{Outcome: outcome, Err: fmt.Errorf("failed to write M3UA: %w", syscall.EAGAIN)}
}

func TestTransportRefusalIsRecognisedUnderEitherOutcome(t *testing.T) {
	for _, err := range []error{refusal(m3ua.DataNotSent), refusal(m3ua.DataSendIndeterminate), syscall.EWOULDBLOCK} {
		if !transportRefused(err) {
			t.Errorf("%v not recognised as a refusal", err)
		}
	}
	for _, err := range []error{
		nil,
		&m3ua.DataWriteError{Outcome: m3ua.DataNotSent, Err: m3ua.ErrNotEstablished},
		&m3ua.DataWriteError{Outcome: m3ua.DataSendIndeterminate, Err: syscall.EPIPE},
		m3ua.ErrAssociationClosed,
	} {
		if transportRefused(err) {
			t.Errorf("%v treated as a refusal to retry", err)
		}
	}
}

func TestWriteWithBackpressureResendsOnlyRefusedMessages(t *testing.T) {
	attempts := 0
	refusals, err := writeWithBackpressure(context.Background(), time.Second, func() error {
		attempts++
		if attempts <= 3 {
			return refusal(m3ua.DataNotSent)
		}
		return nil
	})
	if err != nil || refusals != 3 || attempts != 4 {
		t.Fatalf("refusals %d attempts %d err %v", refusals, attempts, err)
	}

	attempts = 0
	genuine := &m3ua.DataWriteError{Outcome: m3ua.DataSendIndeterminate, Err: syscall.ECONNRESET}
	refusals, err = writeWithBackpressure(context.Background(), time.Second, func() error {
		attempts++
		return genuine
	})
	if !errors.Is(err, syscall.ECONNRESET) || refusals != 0 || attempts != 1 {
		t.Fatalf("an indeterminate send was retried: refusals %d attempts %d err %v", refusals, attempts, err)
	}

	started := time.Now()
	refusals, err = writeWithBackpressure(context.Background(), 50*time.Millisecond, func() error {
		return refusal(m3ua.DataSendIndeterminate)
	})
	if err == nil || !errors.Is(err, errBackpressureTimeout) || refusals == 0 || time.Since(started) > time.Second {
		t.Fatalf("a refusal that never clears: refusals %d err %v after %s", refusals, err, time.Since(started))
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := writeWithBackpressure(ctx, time.Second, func() error { return refusal(m3ua.DataNotSent) }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled wait: %v", err)
	}
}
