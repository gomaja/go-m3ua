package m3ua

import "slices"

const (
	ssnmReportEventByteLimit = DefaultSSNMSubscriptionQueueBytes - 1
	ssnmReportFragmentBytes  = 256 << 10
)

// SSNMReportPatch is one bounded piece of retained destination knowledge.
// Only SSNMReportPatchEvent carries it; complete report observations keep
// DestinationUpdates as whole replacements.
type SSNMReportPatch struct {
	// State replaces a destination or supplies the source-history tail to append.
	State SSNMDestinationKnowledge
	// SourcesContinued appends only source lists to the preceding replacement,
	// preserving both scalar dimensions. Apply patches once in stream order.
	SourcesContinued bool
}

// The raw report is emitted once. Partial knowledge uses a separate kind and
// field, so a whole-destination consumer cannot mistake a tail for a replacement.
// Validated RC/APC parameters contain at most 16382 words (RFC 4666 §3.2).
// Scalars plus one maximal source cost at most 198182 accounted bytes, so each
// iteration can consume a scalar or source atom within the 256 KiB ceiling.
// The existing store mutex serializes the sequence without callbacks or waits.
func (s *ssnmState) publishReportFragmentsLocked(event SSNMEvent) {
	raw := event
	raw.DestinationUpdates = nil
	raw.ReportContinues = len(event.DestinationUpdates) > 0
	s.publishSingleEventLocked(raw)
	for index, entry := range event.DestinationUpdates {
		available, congested := entry.AvailabilitySources, entry.CongestionSources
		entry.AvailabilitySources, entry.CongestionSources = nil, nil
		continued := false
		for {
			chunk := SSNMEvent{Kind: SSNMReportPatchEvent, Partition: event.Partition, Epoch: event.Epoch, ReportPatch: &SSNMReportPatch{State: entry, SourcesContinued: continued}}
			base, _ := ssnmEventAccountedBytes(chunk, ssnmReportFragmentBytes)
			remaining := ssnmReportFragmentBytes - base
			na, nc := 0, 0
			for na < len(available) && reserveSSNMEventBytes(&remaining, 1, ssnmEventStateBytes+4*len(available[na].Scope.RoutingContexts)) {
				na++
			}
			if na == len(available) {
				for nc < len(congested) && reserveSSNMEventBytes(&remaining, 1, ssnmEventStateBytes+4*len(congested[nc].Scope.RoutingContexts)) {
					nc++
				}
			}
			chunk.ReportPatch.State.AvailabilitySources = available[:na:na]
			chunk.ReportPatch.State.CongestionSources = congested[:nc:nc]
			available, congested = available[na:], congested[nc:]
			chunk.ReportContinues = len(available)+len(congested) > 0 || index+1 < len(event.DestinationUpdates)
			chunk.Revision = s.nextRevisionLocked()
			s.publishSingleEventLocked(chunk)
			if len(available)+len(congested) == 0 {
				break
			}
			entry = SSNMDestinationKnowledge{Destination: entry.Destination}
			continued = true
		}
	}
}

// ApplyReport applies an owned observation or patch to its matching replica.
// Observations replace complete DestinationUpdates; patch events use only
// ReportPatch, appending sources when SourcesContinued is set. False means
// another kind, partition/epoch, an invalid mixed shape, or an absent target for
// a continuation. No mutation occurs on false. Apply once in stream order;
// missing/reordered/replayed fragments are not validated by this helper.
// The replica shares no mutable slices with the event. Finish through
// ReportContinues=false before exposing the complete report, or use Resync
// after continuity loss.
func (e SSNMEvent) ApplyReport(partition *SSNMPartitionKnowledge) bool {
	if partition == nil || e.Epoch == 0 || partition.Epoch != e.Epoch || partition.Partition != e.Partition {
		return false
	}
	updates := e.DestinationUpdates
	continued := false
	switch e.Kind {
	case SSNMReportObservedEvent:
		if e.ReportPatch != nil {
			return false
		}
	case SSNMReportPatchEvent:
		if e.ReportPatch == nil || len(updates) != 0 || e.ReportSet {
			return false
		}
		continued = e.ReportPatch.SourcesContinued
		updates = []SSNMDestinationKnowledge{e.ReportPatch.State}
	default:
		return false
	}
	find := func(destination PointCodeRange) (int, bool) {
		return slices.BinarySearchFunc(partition.Destinations, destination, func(a SSNMDestinationKnowledge, b PointCodeRange) int {
			return compareSSNMDestinationKeys(ssnmDestinationKeyFor(a.Destination), ssnmDestinationKeyFor(b))
		})
	}
	if continued {
		if _, found := find(updates[0].Destination); !found {
			return false
		}
	}
	owned := e.clone()
	updates = owned.DestinationUpdates
	if owned.ReportPatch != nil {
		updates = []SSNMDestinationKnowledge{owned.ReportPatch.State}
	}
	for _, update := range updates {
		index, found := find(update.Destination)
		if continued {
			previous := &partition.Destinations[index]
			previous.AvailabilitySources = append(previous.AvailabilitySources, update.AvailabilitySources...)
			previous.CongestionSources = append(previous.CongestionSources, update.CongestionSources...)
		} else if found {
			partition.Destinations[index] = update
		} else {
			partition.Destinations = slices.Insert(partition.Destinations, index, update)
		}
	}
	return true
}
