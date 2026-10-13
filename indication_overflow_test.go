// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"testing"
	"time"
)

func TestStateChangesOverflowClosesWithAnExplicitCause(t *testing.T) {
	connection := newIndicationAssociation(t)
	states := connection.StateChanges()

	for range cap(states) {
		connection.notifyStateChange(StateASPInactive)
	}
	if connection.indicationOverflow.Load() || len(states) != cap(states) {
		t.Fatal("state queue overflowed before its last slot was filled")
	}
	connection.notifyStateChange(StateASPActive)

	select {
	case <-connection.Done():
	case <-time.After(time.Second):
		t.Fatal("state indication overflow did not close the association")
	}
	if err := connection.Err(); !errors.Is(err, ErrIndicationQueueFull) {
		t.Fatalf("state indication overflow error = %v, want %v", err, ErrIndicationQueueFull)
	}
}

func TestManagementIndicationsOverflowClosesWithAnExplicitCause(t *testing.T) {
	connection := newIndicationAssociation(t)
	management := connection.ManagementIndications()

	for range cap(management) {
		connection.notifyManagement(&ManagementIndication{Kind: ManagementNotify})
	}
	if connection.indicationOverflow.Load() || len(management) != cap(management) {
		t.Fatal("management queue overflowed before its last slot was filled")
	}
	connection.notifyManagement(&ManagementIndication{Kind: ManagementError})

	select {
	case <-connection.Done():
	case <-time.After(time.Second):
		t.Fatal("management indication overflow did not close the association")
	}
	if err := connection.Err(); !errors.Is(err, ErrIndicationQueueFull) {
		t.Fatalf("management indication overflow error = %v, want %v", err, ErrIndicationQueueFull)
	}
}
