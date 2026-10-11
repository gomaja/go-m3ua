// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"sync"
	"sync/atomic"
)

// lockWaiters adds cancellation to a lock without a goroutine per waiter.
// Uncontended acquisitions and releases neither allocate nor take mu.
// A waiter registers before retrying the lock under mu, so an unlock cannot
// be lost between that retry and subscribing to changed.
type lockWaiters struct {
	mu      sync.Mutex
	count   atomic.Int32
	changed chan struct{}
}

func (w *lockWaiters) acquire(ctx context.Context, try func() bool) error {
	w.mu.Lock()
	w.count.Add(1)
	defer func() {
		w.count.Add(-1)
		w.mu.Unlock()
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if try() {
			return nil
		}
		if w.changed == nil {
			w.changed = make(chan struct{})
		}
		changed := w.changed
		w.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		w.mu.Lock()
	}
}

func (w *lockWaiters) wake() {
	if w.count.Load() == 0 {
		return
	}
	w.mu.Lock()
	if w.changed != nil {
		close(w.changed)
		w.changed = nil
	}
	w.mu.Unlock()
}

// contextMutex retains sync.Mutex's ordinary Lock/Unlock contract and adds
// an abandonable acquisition. Its zero value is ready for use.
type contextMutex struct {
	mu      sync.Mutex
	waiters lockWaiters
}

func (m *contextMutex) Lock()         { m.mu.Lock() }
func (m *contextMutex) TryLock() bool { return m.mu.TryLock() }
func (m *contextMutex) Unlock() {
	m.mu.Unlock()
	m.waiters.wake()
}

func (m *contextMutex) LockContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ctx.Done() == nil {
		m.Lock()
		return nil
	}
	if m.mu.TryLock() {
		return nil
	}
	return m.waiters.acquire(ctx, m.mu.TryLock)
}

// contextRWMutex adds abandonable read admission to the transfer barrier.
// Writers keep sync.RWMutex's preference over new readers.
type contextRWMutex struct {
	mu      sync.RWMutex
	waiters lockWaiters
}

func (m *contextRWMutex) Lock()  { m.mu.Lock() }
func (m *contextRWMutex) RLock() { m.mu.RLock() }
func (m *contextRWMutex) Unlock() {
	m.mu.Unlock()
	m.waiters.wake()
}
func (m *contextRWMutex) RUnlock() {
	m.mu.RUnlock()
	m.waiters.wake()
}

func (m *contextRWMutex) RLockContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ctx.Done() == nil {
		m.RLock()
		return nil
	}
	if m.mu.TryRLock() {
		return nil
	}
	return m.waiters.acquire(ctx, m.mu.TryRLock)
}
