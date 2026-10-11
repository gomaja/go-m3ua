// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

func TestContextMutexCannotLoseAnUnlock(t *testing.T) {
	var m contextMutex
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for range 2000 {
		m.Lock()
		done := make(chan error, 1)
		go func() {
			err := m.LockContext(ctx)
			if err == nil {
				m.Unlock()
			}
			done <- err
		}()
		// Race registration against release, including the interval between
		// TryLock failing and the waiter subscribing to the unlock notification.
		m.Unlock()
		if err := <-done; err != nil {
			t.Fatalf("lost unlock: %v", err)
		}
	}
	if m.waiters.count.Load() != 0 {
		t.Fatal("lock retained waiters")
	}
}

func TestContextMutexWakesRegisteredWaiter(t *testing.T) {
	var m contextMutex
	m.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		err := m.LockContext(ctx)
		if err == nil {
			m.Unlock()
		}
		done <- err
	}()
	for m.waiters.count.Load() == 0 && ctx.Err() == nil {
		runtime.Gosched()
	}
	m.Unlock()
	if err := <-done; err != nil {
		t.Fatalf("registered waiter missed unlock: %v", err)
	}
}

func TestContextMutexRefusesAlreadyCancelledContext(t *testing.T) {
	var m contextMutex
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.LockContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !m.TryLock() {
		t.Fatal("cancelled acquisition took the lock")
	}
	m.Unlock()
}

func TestContextRWMutexCancelsReadAdmission(t *testing.T) {
	var m contextRWMutex
	m.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		err := m.RLockContext(ctx)
		if err == nil {
			m.RUnlock()
		}
		done <- err
	}()
	err, returned := contextResult(done)
	m.Unlock()
	if !returned {
		<-done
		t.Fatal("read admission ignored cancellation")
	}
	if !errors.Is(err, context.DeadlineExceeded) || m.waiters.count.Load() != 0 {
		t.Fatalf("cancelled read admission: %v", err)
	}
	if err := m.RLockContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.RUnlock()
}
