package main

import (
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/gomaja/go-m3ua"
)

// One-SGP failure trial (performance budgets section 4, "One SGP failure").
// It is off unless -sgp-failure is set, so every existing mode keeps its exact
// flags, specification and records.
//
// The trial runs the routed topology with MTPTransfer as the sender. At a
// declared shared-clock instant inside the measurement window the receiver
// ends both associations of SGP sg-a/p0 through the library's own
// Association.Close or, with -sgp-failure-kind=abort, Association.Abort. The
// route inventory keeps a same-SG/AS alternative active: every SGP serves the
// "primary" Application Server, so the routes frozen on sg-a/p0 can only move
// to sg-a/p1, and MTPTransfer must select it.
const (
	// sgpFailureKindClose and sgpFailureKindAbort are the -sgp-failure-kind
	// values: the library call that ends the failed SGP's associations.
	sgpFailureKindClose = "close"
	sgpFailureKindAbort = "abort"

	// sgpFailureKindShutdown is the recorded kind of a close failure. The SGP
	// ends its associations with Association.Close, which the SCTP dependency
	// performs as a graceful SHUTDOWN (RFC 9260 Section 9.2) with an ABORT
	// fallback after three seconds, so the ASP's SCTP layer reports
	// SHUTDOWN_COMPLETE. The record names the SCTP procedure, as it did while
	// close was the only kind, so a close trial's specification is unchanged.
	// An abort failure is recorded as sgpFailureKindAbort: Association.Abort
	// sends one ABORT with the User-Initiated Abort cause (RFC 9260 Section
	// 9.1), and the ASP's SCTP layer reports COMMUNICATION LOST (Section
	// 11.2.5) instead.
	sgpFailureKindShutdown = "shutdown"

	// The section 4 budgets: "usable alternative selection within 100 ms" and
	// "healthy-path delivery returns to at least 90% of pre-failure rate
	// within 1 s", measured from the library's transport-failure notification.
	sgpFailureSelectionBudget = 100 * time.Millisecond
	sgpFailureRecoveryBudget  = time.Second
	sgpFailureRecoveryPercent = 90

	// sgpFailureBin is the width of the receiver's delivery-rate bins.
	sgpFailureBin = 100 * time.Millisecond

	// The pre-failure rate is the mean over whole bins from one second after
	// the window start to the fault, and needs at least this much of it.
	sgpFailurePreRateSkip      = time.Second
	sgpFailureMinimumPreFault  = sgpFailurePreRateSkip + time.Second
	sgpFailureMinimumPostFault = 10 * time.Second

	// sgpFailureOutcomeSamples bounds the per-message failed-path outcome
	// detail a record keeps; the counters are never bounded.
	sgpFailureOutcomeSamples = 256
)

// The failed SGP and its same-SG/AS alternative are fixed by the topology:
// sg-a/p0 fails, and sg-a/p1 serves the same Signalling Gateway path and the
// same preferred Application Server.
var (
	sgpFailureFailed      = m3ua.SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "p0"}
	sgpFailureAlternative = m3ua.SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "p1"}
)

// sgpFailureSpec is the failure declaration of a measurement cohort. The ASP
// declares it and the SGP accepts the cohort only when it equals the one its
// own -sgp-failure flag describes, so both processes provably ran the same
// trial. Warm-up cohorts carry none.
type sgpFailureSpec struct {
	Kind            string           `json:"kind"`
	SGP             m3ua.SGPIdentity `json:"sgp"`
	Alternative     m3ua.SGPIdentity `json:"alternative"`
	Offset          time.Duration    `json:"offset_ns"`
	SelectionBudget time.Duration    `json:"selection_budget_ns"`
	RecoveryBudget  time.Duration    `json:"recovery_budget_ns"`
	RecoveryPercent int              `json:"recovery_percent"`
	Bin             time.Duration    `json:"bin_ns"`
}

// newSGPFailureSpec is the declaration a -sgp-failure offset and
// -sgp-failure-kind describe.
func newSGPFailureSpec(offset time.Duration, kind string) *sgpFailureSpec {
	return &sgpFailureSpec{
		Kind: sgpFailureRecordedKind(kind), SGP: sgpFailureFailed, Alternative: sgpFailureAlternative, Offset: offset,
		SelectionBudget: sgpFailureSelectionBudget, RecoveryBudget: sgpFailureRecoveryBudget,
		RecoveryPercent: sgpFailureRecoveryPercent, Bin: sgpFailureBin,
	}
}

// sgpFailureRecordedKind is the kind a failure declaration records for a
// -sgp-failure-kind value: the SCTP procedure the failure puts on the wire.
func sgpFailureRecordedKind(kind string) string {
	if kind == sgpFailureKindAbort {
		return sgpFailureKindAbort
	}
	return sgpFailureKindShutdown
}

// sgpFailureMethod names the Association method a recorded kind injects.
func sgpFailureMethod(recordedKind string) string {
	if recordedKind == sgpFailureKindAbort {
		return "Abort"
	}
	return "Close"
}

// validateSGPFailureConfig bounds -sgp-failure and -sgp-failure-kind after the
// rest of the configuration has been validated.
func validateSGPFailureConfig(flagSet *flag.FlagSet, config commandConfig) error {
	kindSet := false
	flagSet.Visit(func(set *flag.Flag) { kindSet = kindSet || set.Name == "sgp-failure-kind" })
	if config.SGPFailure == 0 {
		if kindSet {
			return errors.New("-sgp-failure-kind requires -sgp-failure")
		}
		return nil
	}
	switch {
	case config.SGPFailureKind != sgpFailureKindClose && config.SGPFailureKind != sgpFailureKindAbort:
		return fmt.Errorf("sgp-failure-kind must be %s or %s", sgpFailureKindClose, sgpFailureKindAbort)
	case config.SGPFailure < 0:
		return errors.New("sgp-failure must be a positive offset into the measurement window")
	case config.Mode != modeRouted:
		return errors.New("-sgp-failure runs with -mode=routed only: MTPTransfer must select the alternative")
	case !config.SameHostClock:
		return errors.New("-sgp-failure requires -same-host-clock: the fault, notification and delivery times use the shared Linux monotonic clock")
	case config.SSNM.enabled():
		return errors.New("-sgp-failure and SSNM load are separate workloads")
	}
	if config.Role == "asp" {
		if config.SGPFailure < sgpFailureMinimumPreFault {
			return fmt.Errorf("sgp-failure must leave at least %s of pre-failure traffic", sgpFailureMinimumPreFault)
		}
		if config.Duration-config.SGPFailure < sgpFailureMinimumPostFault {
			return fmt.Errorf("sgp-failure must leave at least %s of the measurement window after the fault", sgpFailureMinimumPostFault)
		}
	}
	return nil
}

// validateSGPFailureSpec checks a declared failure against the cohort and
// against the receiver's own flags, which are the only failure it may inject.
func validateSGPFailureSpec(specification runSpec, offset time.Duration, kind string) error {
	declared := specification.SGPFailure
	if declared == nil {
		return nil
	}
	switch {
	case offset == 0:
		return errors.New("this receiver was not started with -sgp-failure and injects no failure")
	case declared.Kind != sgpFailureRecordedKind(kind):
		return fmt.Errorf("the declared SGP failure is %q, but this receiver's -sgp-failure-kind=%s injects %q", declared.Kind, kind, sgpFailureRecordedKind(kind))
	case *declared != *newSGPFailureSpec(offset, kind):
		return errors.New("the declared SGP failure differs from this receiver's -sgp-failure")
	case specification.Mode != modeRouted || specification.Clock == nil:
		return errors.New("an SGP failure cohort is a shared-clock routed cohort")
	case declared.Offset < sgpFailureMinimumPreFault || specification.Duration-declared.Offset < sgpFailureMinimumPostFault:
		return errors.New("the SGP failure offset leaves too little of the window before or after the fault")
	}
	return nil
}

// sgpFailureInstant is the shared-clock instant the declared fault is due.
func sgpFailureInstant(specification runSpec) int64 {
	return specification.Clock.Start + int64(specification.SGPFailure.Offset)
}
