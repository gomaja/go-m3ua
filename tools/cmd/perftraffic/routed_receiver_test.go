package main

import (
	"context"
	"errors"
	"testing"
)

// A requested stop closes the SGP endpoints too, so both are ready when the
// receiver looks: the stop must win every time, and endpoints closing on their
// own must still be a fault.
func TestRoutedReceiverEndTellsAStopFromLostEndpoints(testContext *testing.T) {
	closed := make(chan struct{})
	close(closed)
	stopped, stop := context.WithCancel(context.Background())
	stop()
	for attempt := 0; attempt < 200; attempt++ {
		if err := routedReceiverEnd(stopped, nil, nil, closed); err != nil {
			testContext.Fatalf("attempt %d: a requested stop that also closed the endpoints reported %v", attempt, err)
		}
	}
	if err := routedReceiverEnd(context.Background(), nil, nil, closed); err == nil || err.Error() != "routed SGP endpoints closed" {
		testContext.Fatalf("endpoints closing without a stop = %v, want the fault", err)
	}
	fatal := make(chan error, 1)
	fatal <- errors.New("association 3 lost")
	if err := routedReceiverEnd(context.Background(), fatal, nil, nil); err == nil || err.Error() != "association 3 lost" {
		testContext.Fatalf("a fixture fault = %v", err)
	}
	failure := make(chan error, 1)
	failure <- errors.New("listener closed")
	if err := routedReceiverEnd(context.Background(), nil, failure, nil); err == nil || err.Error() != "HTTP control server: listener closed" {
		testContext.Fatalf("an HTTP failure = %v", err)
	}
}

// The routed receiver ends with the first fault its control recorded: a
// sender's stop is recorded before it closes the SGP endpoints, so the run
// reports the stop, not the endpoints closing that followed it.
func TestRoutedReceiverReportsTheFirstRecordedFault(testContext *testing.T) {
	closed := errors.New("routed SGP endpoints closed")
	if err := routedReceiverCause(&receiverControl{}, nil); err != nil {
		testContext.Fatalf("a requested stop = %v, want nil", err)
	}
	control := &receiverControl{}
	if err := routedReceiverCause(control, closed); err == nil || err.Error() != closed.Error() || control.fatalError() != closed.Error() {
		testContext.Fatalf("endpoints closing on their own = %v (recorded %q), want that fault", err, control.fatalError())
	}
	stopped := &receiverControl{}
	stopped.setFatal("routed peer topology was stopped by the sender before measurement completed")
	if err := routedReceiverCause(stopped, closed); err == nil || err.Error() != stopped.fatalError() {
		testContext.Fatalf("endpoints closing after the sender's stop = %v, want the stop reason", err)
	}
}
