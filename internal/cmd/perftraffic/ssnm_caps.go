package main

import (
	"fmt"

	"github.com/gomaja/go-m3ua"
)

// Accounted bytes of one queued SSNM event, from the documented contract of
// SSNMStateConfig.SubscriptionQueueBytes: "Accounting charges 512 bytes per
// event, 8 per report destination, 256 per updated destination, 4 per
// Routing Context in the report and both dimensions of each update, and the
// byte lengths of Reason and the event/report partition identity strings."
// The fixture recomputes them from delivered events as an independent
// observer; the continuity-loss marker is outside the accounting.
const (
	ssnmEventBaseBytes           = 512
	ssnmEventDestinationBytes    = 8
	ssnmEventUpdateBytes         = 256
	ssnmEventRoutingContextBytes = 4
)

// ssnmEventBytes is the accounted size of one delivered event.
func ssnmEventBytes(event m3ua.SSNMEvent) int {
	contexts := len(event.Report.Scope.RoutingContexts)
	for _, update := range event.Updated {
		contexts += len(update.Availability.Scope.RoutingContexts) + len(update.Congestion.Scope.RoutingContexts)
	}
	identities := len(event.Partition.SignallingGateway) + len(event.Partition.ApplicationServer) +
		len(event.Report.Partition.SignallingGateway) + len(event.Report.Partition.ApplicationServer)
	return ssnmEventBaseBytes +
		ssnmEventDestinationBytes*len(event.Report.Destinations) +
		ssnmEventUpdateBytes*len(event.Updated) +
		ssnmEventRoutingContextBytes*contexts +
		len(event.Reason) + identities
}

// ssnmWorkloadEventBytes is the accounted size of the report one generated
// message of destinations Affected Point Codes produces: every destination
// explicit and updated once, in the availability dimension, under the
// fixture's one-Routing-Context scope, with no congestion dimension and no
// reason. Partition identity strings are left empty: a standalone partition,
// which every fixture association is, has none, and in any other partition
// they only add bytes, so the size is a lower bound for every partition.
func ssnmWorkloadEventBytes(destinations int) int {
	scope := ssnmScope()
	event := m3ua.SSNMEvent{
		Kind:      m3ua.SSNMReportEvent,
		ReportSet: true,
		Report:    m3ua.SSNMReport{Scope: scope, Destinations: make([]m3ua.PointCodeRange, destinations)},
		Updated:   make([]m3ua.SSNMDestinationKnowledge, destinations),
	}
	for index := range event.Updated {
		event.Updated[index].Availability.Scope = scope
		event.Updated[index].AvailabilitySet = true
	}
	return ssnmEventBytes(event)
}

// The subscription cap a paused queue's continuity loss is attributed to.
const (
	ssnmBindingCount = "count"
	ssnmBindingBytes = "bytes"
)

// ssnmCapsReached reports which caps the retained queue of a pause record
// shows reached at its continuity loss. The count cap is reached when the
// queue retained exactly its event limit. The byte cap is reached when the
// retained accounted bytes fit the limit and the smallest event the
// generator can queue after the preload would not have fitted beside them,
// so no further event could have been queued.
func ssnmCapsReached(pause *ssnmPauseRecord) (count, bytes bool) {
	if !pause.ContinuityLossObserved {
		return false, false
	}
	count = pause.QueuedAtLoss == pause.QueueLimit
	bytes = pause.QueuedBytesAtLoss <= pause.QueueByteLimit &&
		pause.QueuedBytesAtLoss+pause.SmallestEventBytes > pause.QueueByteLimit
	return count, bytes
}

// ssnmBindingCap names the cap that bound the queue: count when the count
// cap was reached, which alone explains the loss, otherwise bytes when the
// byte cap was; empty when neither was.
func ssnmBindingCap(count, bytes bool) string {
	switch {
	case count:
		return ssnmBindingCount
	case bytes:
		return ssnmBindingBytes
	default:
		return ""
	}
}

// ssnmCapFailure checks the F3 overflow evidence: the paused subscriber
// observed its continuity loss, its retained queue exceeds neither cap, and
// one cap was actually reached, the one the record names. A loss with
// neither cap reached is the library losing continuity early. It returns
// the violation, or "" when the evidence holds.
func ssnmCapFailure(pause *ssnmPauseRecord) string {
	switch {
	case pause.QueuedAtLoss > pause.QueueLimit:
		return fmt.Sprintf("retained %d events, over the %d-event cap", pause.QueuedAtLoss, pause.QueueLimit)
	case pause.QueuedBytesAtLoss > pause.QueueByteLimit:
		return fmt.Sprintf("retained %d accounted bytes, over the %d-byte cap", pause.QueuedBytesAtLoss, pause.QueueByteLimit)
	case !pause.ContinuityLossObserved:
		return "observed no continuity loss"
	case pause.QueuedAtLoss > 0 && pause.SmallestQueuedEventBytes < pause.SmallestEventBytes:
		return fmt.Sprintf("retained an event of %d accounted bytes, below the workload's smallest %d, so the byte cap cannot be judged", pause.SmallestQueuedEventBytes, pause.SmallestEventBytes)
	}
	count, bytes := ssnmCapsReached(pause)
	binding := ssnmBindingCap(count, bytes)
	switch {
	case binding == "":
		return fmt.Sprintf("lost continuity with %d events and %d accounted bytes retained, below both caps: %d events, and %d bytes that another %d-byte event would fit",
			pause.QueuedAtLoss, pause.QueuedBytesAtLoss, pause.QueueLimit, pause.QueueByteLimit, pause.SmallestEventBytes)
	case pause.BindingCap != binding || pause.CountCapEnforced != count || pause.ByteCapEnforced != bytes:
		return fmt.Sprintf("recorded binding cap %q (count %t, bytes %t) but its retained queue shows %q (count %t, bytes %t)",
			pause.BindingCap, pause.CountCapEnforced, pause.ByteCapEnforced, binding, count, bytes)
	}
	return ""
}
