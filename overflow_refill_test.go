// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestOverflowRefillCrossingSurvivesConcurrentDequeue(t *testing.T) {
	c, _ := overflowAssociation(t)
	for episode := 0; episode < 2; episode++ {
		if !t.Run(fmt.Sprintf("episode_%d", episode), func(t *testing.T) { assertRefillSurvivesDequeue(t, c) }) {
			return
		}
		if episode == 0 {
			if _, err := c.ReadData(context.Background()); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				deliver(t, c, 1, inboundData(1, "next episode"))
			}
		}
	}
}

func assertRefillSurvivesDequeue(t *testing.T, c *Association) {
	t.Helper()
	deliver(t, c, 1, inboundData(1, "discarded"))
	_ = firstErr(c)
	if _, err := c.ReadData(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.dataChan) != 1 {
		t.Fatal("expected one queued message before refill")
	}

	// Prime a 950 ms low-occupancy dwell under the tracker lock. The queue
	// transitions below use the real handler and a concurrent public reader;
	// only the clock is primed, so scheduler delays cannot expire setup.
	c.muDataOverflow.Lock()
	lowStart := time.Now().Add(-950 * time.Millisecond)
	c.dataOverflowLowSince = lowStart
	queued := make(chan struct{})
	go func() { deliver(t, c, 1, inboundData(1, "refill")); close(queued) }()
	sent := waitFor(func() bool {
		if len(c.dataChan) != 2 {
			return false
		}
		for _, stack := range goroutinesBlockedIn("observeDataQueue") {
			if strings.Contains(stack, "handleData") && strings.Contains(stack, "lockSlow") {
				return true
			}
		}
		return false
	}, time.Second)
	if !sent {
		c.muDataOverflow.Unlock()
		<-queued
		t.Fatal("refill did not reach actual occupancy two before observation")
	}
	read := make(chan error, 1)
	go func() { _, err := c.ReadData(context.Background()); read <- err }()
	drained := waitFor(func() bool { return len(c.dataChan) == 1 }, time.Second)
	c.muDataOverflow.Unlock()
	<-queued
	if err := <-read; err != nil {
		t.Fatal(err)
	}
	if !drained {
		t.Fatal("concurrent reader did not restore actual occupancy one")
	}

	// Both delayed observations sample one. At the former dwell deadline only
	// about 50 ms of the new low interval can have elapsed, so ending is early.
	c.muDataOverflow.Lock()
	c.observeDataQueueLocked(lowStart.Add(dataOverflowDrainDwell), len(c.dataChan))
	stillActive := c.dataOverflow.Load()
	c.muDataOverflow.Unlock()
	if !stillActive {
		t.Fatal("1 -> 2 -> 1 refill was hidden: episode ended at the old drain deadline")
	}
	if !waitFor(func() bool { return !c.DataQueueStats().Congested }, 2*time.Second) {
		t.Fatal("episode did not end after a fresh sustained drain")
	}
}
