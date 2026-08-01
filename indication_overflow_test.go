// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"testing"
	"time"
)

func TestSignallingStatusOverflowRequiresResynchronization(t *testing.T) {
	connection, _ := newTestConn(t, StateAspActive, modeClient)
	connection.statusChan = make(chan *DestinationStatus, 1)

	connection.notifyStatus(&DestinationStatus{PointCode: 1, State: DestinationUnavailable})
	connection.notifyStatus(&DestinationStatus{PointCode: 2, State: DestinationAvailable})

	status := <-connection.SignallingStatus()
	if status == nil || !status.ResyncRequired {
		t.Fatalf("overflow status = %#v, want ResyncRequired marker", status)
	}
}

func TestStateChangesOverflowClosesWithAnExplicitCause(t *testing.T) {
	connection, _ := newTestConn(t, StateAspActive, modeClient)
	connection.stateEventChan = make(chan State, 1)

	connection.notifyStateChange(StateAspInactive)
	connection.notifyStateChange(StateAspActive)

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
	connection, _ := newTestConn(t, StateAspActive, modeClient)
	connection.mgmtChan = make(chan *ManagementIndication, 1)

	connection.notifyManagement(&ManagementIndication{Kind: ManagementNotify})
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
