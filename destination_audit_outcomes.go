package m3ua

import "slices"

const (
	auditOutcomeReasonCount        = 7
	auditOutcomeMaxRoutingContexts = 64
)

// One report or automation pass retains at most one diagnostic per fixed reason.
// Each has no destination, identity name or Info String. At most 64 Routing
// Contexts cost 256 bytes. With the 512-byte event base and even the general
// 160-byte reason ceiling, one event costs at most 928 bytes; the longest
// fixed reason is 16 bytes, yielding 784 bytes. The diagnostics use at most
// seven slots and 7*928 = 6496 bytes. Add the actual report-observation and
// report-patch event count; only an unfragmented observation adds one slot.
// Report payloads have separate bounded destination-delta accounting.
// Default subscriptions provide 256 slots and 1 MiB.
//
// Scope borrows immutable scheduled job storage until publishLocked clones it.
// All methods run under ssnmState.mu, with no callbacks, waits or goroutines.
type auditOutcomeBatch struct {
	sourceJobs map[destinationAuditKey]struct{}
	sources    map[auditSourceRetirementKey]uint8
	outcomes   [auditOutcomeReasonCount]auditOutcomeSummary
}

type auditOutcomeSummary struct {
	count              uint64
	association        AssociationID
	associationOmitted bool
	scope              WireScope
	scopeOmitted       bool
}

func auditOutcomeReason(index int) string {
	switch index {
	case 0:
		return "queue-full"
	case 1:
		return "rate-deferred"
	case 2:
		return "reporter-retired"
	case 3:
		return "send-deferred"
	case 4:
		return "contended"
	case 5:
		return "send-failed"
	case 6:
		return "submitted"
	default:
		return ""
	}
}

// add receives only actual job outcome transitions; its caller suppresses repeats.
// Unknown reasons are excluded so neither arbitrary text nor extra slots enter
// diagnostics. Production callers use only the seven reasons above.
func (b *auditOutcomeBatch) add(job *destinationAuditJob, outcome string) {
	for index := range b.outcomes {
		if auditOutcomeReason(index) != outcome {
			continue
		}
		summary := &b.outcomes[index]
		if summary.count == 0 {
			summary.association = job.key.association
			summary.scopeOmitted = len(job.scope.RoutingContexts) > auditOutcomeMaxRoutingContexts
			if !summary.scopeOmitted {
				summary.scope = job.scope
			}
		} else {
			if summary.association != job.key.association {
				summary.associationOmitted = true
			}
			if !summary.scopeOmitted && (summary.scope.NetworkAppearance != job.scope.NetworkAppearance || summary.scope.NetworkAppearanceSet != job.scope.NetworkAppearanceSet || summary.scope.RoutingContextSet != job.scope.RoutingContextSet || !slices.Equal(summary.scope.RoutingContexts, job.scope.RoutingContexts)) {
				summary.scopeOmitted = true
				summary.scope = WireScope{}
			}
		}
		summary.count++
		return
	}
}

func (b *auditOutcomeBatch) publishLocked(s *ssnmState) {
	s.retireAuditSourcesLocked(b.sources, b.sourceJobs, b)
	for index := range b.outcomes {
		summary := &b.outcomes[index]
		if summary.count == 0 {
			continue
		}
		association := summary.association
		if summary.associationOmitted {
			association = 0
		}
		s.publishLocked(SSNMEvent{
			Kind:                    SSNMPublicationOutcomeEvent,
			PublicationKind:         CongestionPublicationDAUD,
			Revision:                s.nextRevisionLocked(),
			Reason:                  auditOutcomeReason(index),
			PublicationCount:        summary.count,
			PublicationScopeOmitted: summary.scopeOmitted,
			Report:                  SSNMReport{Association: association, Scope: summary.scope.clone()},
			ReportSet:               false,
		})
	}
	*b = auditOutcomeBatch{}
}
