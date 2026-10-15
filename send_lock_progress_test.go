// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-sctp"
)

// Keep plain lockers queued long enough to enter starvation handoff on one P.
// A context waiter must join that handoff, rather than compete with TryLock
// only during the gaps between owners (issue #176). No SCTP socket is opened.
func startPlainLockTraffic(t *testing.T, acquire func() error) *atomic.Int64 {
	t.Helper()
	previous := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	stop := make(chan struct{})
	var group sync.WaitGroup
	var completed atomic.Int64
	for range 16 {
		group.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := acquire(); err != nil {
					t.Errorf("plain acquisition: %v", err)
					return
				}
				completed.Add(1)
			}
		})
	}
	t.Cleanup(func() { close(stop); group.Wait() })
	deadline := time.Now().Add(5 * time.Second)
	for completed.Load() < 32 {
		if time.Now().After(deadline) {
			t.Fatal("plain traffic did not warm up")
		}
		time.Sleep(time.Millisecond)
	}
	return &completed
}

func TestWriteDataContextProgressUnderPlainSendTraffic(t *testing.T) {
	c := newSendAllocationAssociation(t)
	c.dataWriter = func(data []byte, _ *sctp.SndInfo) (int, error) {
		time.Sleep(2 * time.Millisecond)
		return len(data), nil
	}
	request := allocationRequest(128)
	completed := startPlainLockTraffic(t, func() error {
		_, err := c.WriteData(request)
		return err
	})
	before := completed.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	n, err := c.WriteDataContext(ctx, request)
	if err != nil || n != len(request.ProtocolData.Data) {
		t.Fatalf("context DATA: octets=%d err=%v; plain sends completed while waiting=%d",
			n, err, completed.Load()-before)
	}
}

func TestMTPTransferContextProgressUnderPlainMutationTraffic(t *testing.T) {
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	e, associations, captures := newASPTransferFixture(t, config)
	c := associations["sg-a/sgp-a1"]
	completed := startPlainLockTraffic(t, func() error {
		release := c.lockASPTransferMutation()
		time.Sleep(2 * time.Millisecond)
		release()
		return nil
	})
	before := completed.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	result, err := e.MTPTransferContext(ctx, MTPTransferRequest{
		ProtocolData: transferProtocolData(0x123456, 1, []byte("progress")),
	})
	if err != nil || result.UserDataOctets != len("progress") || len(result.SuccessfulPaths) != 1 {
		t.Fatalf("context transfer: result=%#v err=%v; plain mutations completed while waiting=%d",
			result, err, completed.Load()-before)
	}
	if captures["sg-a/sgp-a1"].count() != 1 || captures["sg-b/sgp-b1"].count() != 0 {
		t.Fatal("context transfer did not use its selected path exactly once")
	}
}
