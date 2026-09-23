package perfstats

import "time"

// This file fixes the transport-stall detection BEFORE any capacity campaign
// run, on the same terms as the backlog rule in backlog.go: the signal and the
// threshold are named in advance, and neither may be changed to move a result.
//
// The reference environment intermittently stalls the SCTP transport for about
// a second. It reproduces on both the previous and the current go-sctp
// release, so it is a property of that environment and not a candidate
// regression. The response is neither to drop the affected runs, nor to widen
// a threshold for them, nor to exclude them as outliers: a run whose evidence
// a stall contaminated is reported inconclusive with the stall named, and the
// stall itself is carried into the decision so it reaches the report.

// StallThreshold is the shortest single send call this method reports as a
// transport stall. It is one second, taken from SCTP's own retransmission
// floor rather than from any observation: RFC 9260 Section 16 recommends
// RTO.Min of 1 second and RTO.Initial of 1 second, so no SCTP retransmission
// timeout can expire in less than a second. A single send call that blocked
// for at least that long spans at least one whole minimum retransmission
// timeout.
//
// Nothing in the fixture's own send path accounts for a block that long. The
// scheduler releases work in 100 microsecond quanta, the socket is configured
// with Nagle disabled and SACK delay zero, and the send call is the only place
// a send worker waits on the transport; measured send calls run three to four
// orders of magnitude shorter.
//
// RFC 9260 is the current SCTP Proposed Standard: the RFC Editor record and
// the IETF Datatracker agree that no RFC obsoletes or updates it, and none of
// its errata touch Section 16.
//
// The threshold is predeclared for the same reason the backlog comparisons
// are. Raising it so a stalled row reports clean, and lowering it so an
// inconvenient row can be dismissed as environmental, are both the post-hoc
// threshold change budgets section 5 forbids.
const StallThreshold = time.Second

const (
	TransportStallReason       = "transport-stall-detected"
	StallEvidenceMissingReason = "transport-stall-evidence-missing"
)

// StallObservation is one run's transport-stall evidence.
//
// LongestSend is the fixture's sender-record send_duration.max_ns. The fixture
// already records it: startSendWorkers times every
// WriteData call and senderCounters.complete feeds the result
// to the send-duration histogram, whose Max is the exact observed maximum and
// not a histogram bucket bound (only p50, p95 and p99 are bucket bounds).
//
// It is the signal because it measures the transport blocking the sender
// directly. The offered schedule is open loop, so a transport block shows up
// first, and unambiguously, as a send call that does not return; the
// outstanding-cap refusals and missing deliveries that follow are its
// consequences, and dispatch lag and echo round-trip time also rise when the
// fixture is merely loaded. No derived counter is used in its place.
type StallObservation struct {
	LongestSend time.Duration `json:"longest_send_ns"`
}

// Stalled reports whether this run demonstrated a transport stall.
func (observation StallObservation) Stalled() bool {
	return observation.LongestSend >= StallThreshold
}
