package main

import (
	"errors"
	"time"

	"github.com/gomaja/go-m3ua/internal/perfstats"
)

// ssnmSpecEvidence is the SSNM load declaration of a perftraffic cohort
// specification. It is absent from every run without SSNM load.
type ssnmSpecEvidence struct {
	Rate                   *uint64        `json:"rate"`
	APCs                   *int           `json:"apcs"`
	Records                *int           `json:"records"`
	Subscribers            *int           `json:"subscribers"`
	SubscriptionQueueBytes *int           `json:"subscription_queue_bytes"`
	PauseOffset            *time.Duration `json:"pause_offset_ns"`
	PauseDuration          *time.Duration `json:"pause_duration_ns"`
	Phase                  *string        `json:"phase"`
	Anchor                 *int64         `json:"anchor_ns"`
}

// ssnmIdentity is the SSNM part of the workload identity. It is zero for a
// run without SSNM load, so an SSNM-loaded campaign and its matched
// no-update control can never be mixed into one capacity decision. The
// per-run anchor is excluded, like the cohort name and seed, and so is the
// phase: a failed warm-up ran the same disturbance as the measurement.
type ssnmIdentity struct {
	Rate        uint64
	APCs        int
	Records     int
	Subscribers int
	// SubscriptionQueueBytes is the ASP subscriptions' byte limit in force.
	// It decides whether a paused subscriber's overflow is bound by count or
	// by bytes, so campaigns under different limits never mix.
	SubscriptionQueueBytes int
	PauseOffset            time.Duration
	PauseDuration          time.Duration
	// Budgets are the time budgets the ASP judged the SSNM verdict against,
	// from its manifest. The spec does not carry them, so they are zero in an
	// identity derived from the spec alone.
	Budgets ssnmBudgetIdentity
}

// ssnmBudgetsEvidence is the manifest's ssnm_budgets record.
type ssnmBudgetsEvidence struct {
	ApplyP99 *time.Duration `json:"apply_p99_ns"`
	Resync   *time.Duration `json:"resync_ns"`
	Recovery *time.Duration `json:"recovery_ns"`
}

type ssnmBudgetIdentity struct {
	ApplyP99 time.Duration
	Resync   time.Duration
	Recovery time.Duration
}

// ssnmEvidence is the subset of a perftraffic ssnm record the decision reads.
type ssnmEvidence struct {
	Verdict   *string                `json:"verdict"`
	Workload  *ssnmSpecEvidence      `json:"workload"`
	Generator *ssnmGeneratorEvidence `json:"generator"`
}

type ssnmGeneratorEvidence struct {
	State *string `json:"state"`
}

const (
	ssnmPhaseMeasurement = "measurement"
	ssnmPhaseWarmup      = "warmup"
)

const (
	maximumSSNMRate        = 10_000
	maximumSSNMAPCs        = 1024
	maximumSSNMRecords     = 16_384
	maximumSSNMSubscribers = 16
	// minimumSSNMSubscriptionQueueBytes is the smallest subscription byte
	// limit SSNMStateConfig.SubscriptionQueueBytes accepts.
	minimumSSNMSubscriptionQueueBytes = 512
)

func ssnmIdentityFromSpec(spec *fixtureSpec) (ssnmIdentity, error) {
	declared := spec.SSNM
	if declared == nil {
		return ssnmIdentity{}, nil
	}
	if declared.Rate == nil || declared.APCs == nil || declared.Records == nil || declared.Subscribers == nil || declared.SubscriptionQueueBytes == nil ||
		declared.PauseOffset == nil || declared.PauseDuration == nil || declared.Phase == nil || declared.Anchor == nil {
		return ssnmIdentity{}, errors.New("spec.ssnm rate, apcs, records, subscribers, subscription_queue_bytes, pause_offset_ns, pause_duration_ns, phase and anchor_ns are required")
	}
	identity := ssnmIdentity{
		Rate: *declared.Rate, APCs: *declared.APCs, Records: *declared.Records, Subscribers: *declared.Subscribers,
		SubscriptionQueueBytes: *declared.SubscriptionQueueBytes, PauseOffset: *declared.PauseOffset, PauseDuration: *declared.PauseDuration,
	}
	switch {
	case identity.Rate == 0 || identity.Rate > maximumSSNMRate:
		return ssnmIdentity{}, errors.New("spec.ssnm rate must be between 1 and 10000")
	case identity.APCs < 1 || identity.APCs > maximumSSNMAPCs:
		return ssnmIdentity{}, errors.New("spec.ssnm apcs must be between 1 and 1024")
	case identity.Records < identity.APCs || identity.Records > maximumSSNMRecords || identity.Records%identity.APCs != 0:
		return ssnmIdentity{}, errors.New("spec.ssnm records must be a multiple of apcs and at most 16384")
	case identity.Subscribers < 1 || identity.Subscribers > maximumSSNMSubscribers:
		return ssnmIdentity{}, errors.New("spec.ssnm subscribers must be between 1 and 16")
	case identity.SubscriptionQueueBytes < minimumSSNMSubscriptionQueueBytes:
		return ssnmIdentity{}, errors.New("spec.ssnm subscription_queue_bytes must be at least 512")
	case identity.PauseOffset < 0 || identity.PauseDuration < 0 || identity.PauseDuration == 0 && identity.PauseOffset != 0:
		return ssnmIdentity{}, errors.New("spec.ssnm pause must be absent or a non-negative offset with a positive duration")
	case identity.PauseDuration > 0 && identity.Subscribers < 2:
		return ssnmIdentity{}, errors.New("spec.ssnm pause needs a healthy subscriber")
	case *declared.Phase != ssnmPhaseMeasurement && *declared.Phase != ssnmPhaseWarmup:
		return ssnmIdentity{}, errors.New("spec.ssnm phase must be measurement or warmup")
	case spec.Mode == nil || *spec.Mode != "throughput":
		return ssnmIdentity{}, errors.New("spec.ssnm load is defined for throughput cohorts only")
	case spec.SharedClock == nil || spec.SharedClock.Start == nil:
		return ssnmIdentity{}, errors.New("spec.ssnm load requires a shared clock window")
	case *declared.Anchor <= 0 || *declared.Anchor > *spec.SharedClock.Start:
		return ssnmIdentity{}, errors.New("spec.ssnm anchor_ns must be positive and not after the measurement start")
	}
	return identity, nil
}

// ssnmBudgetsFromManifest reads and bounds the ASP's SSNM budgets.
func ssnmBudgetsFromManifest(manifest *fixtureManifest) (ssnmBudgetIdentity, error) {
	if manifest == nil || manifest.SSNMBudgets == nil {
		return ssnmBudgetIdentity{}, errors.New("SSNM-loaded sender manifest requires ssnm_budgets")
	}
	declared := manifest.SSNMBudgets
	if declared.ApplyP99 == nil || declared.Resync == nil || declared.Recovery == nil {
		return ssnmBudgetIdentity{}, errors.New("manifest ssnm_budgets apply_p99_ns, resync_ns and recovery_ns are required")
	}
	budgets := ssnmBudgetIdentity{ApplyP99: *declared.ApplyP99, Resync: *declared.Resync, Recovery: *declared.Recovery}
	if budgets.ApplyP99 <= 0 || budgets.Resync <= 0 || budgets.Recovery <= 0 {
		return ssnmBudgetIdentity{}, errors.New("manifest ssnm_budgets must be positive durations")
	}
	return budgets, nil
}

// rejectStraySSNM refuses SSNM evidence in records whose cohort cannot carry
// SSNM load: the bidirectional records and legacy single sender records. The
// unidirectional path applies the same rule through ssnmCohortVerdict.
func rejectStraySSNM(records ...*fixtureEvidence) error {
	for _, record := range records {
		if record == nil {
			continue
		}
		if record.SSNM != nil || record.Spec != nil && record.Spec.SSNM != nil || record.Manifest != nil && record.Manifest.SSNMBudgets != nil {
			return errors.New("ssnm evidence is present but this cohort cannot carry SSNM load")
		}
	}
	return nil
}

// ssnmCohortVerdict validates the SSNM evidence a loaded cohort must carry
// and returns the sender's SSNM verdict and the budgets it was judged
// against. warmup marks a failed warm-up
// accepted as probe evidence (cohortPhase): its spec declares the warm-up
// phase, the SGP record still carries the generator view, and the ASP record
// carries no SSNM result, because perftraffic judges SSNM for the measurement
// cohort only. A warm-up therefore contributes no SSNM verdict; it can never
// pass anyway.
func ssnmCohortVerdict(identity ssnmIdentity, warmup bool, sender, receiver *fixtureEvidence) (string, ssnmBudgetIdentity, error) {
	verdict, err := ssnmLoadedVerdict(identity, warmup, sender, receiver)
	if err != nil || identity == (ssnmIdentity{}) {
		return verdict, ssnmBudgetIdentity{}, err
	}
	budgets, err := ssnmBudgetsFromManifest(sender.Manifest)
	return verdict, budgets, err
}

func ssnmLoadedVerdict(identity ssnmIdentity, warmup bool, sender, receiver *fixtureEvidence) (string, error) {
	if identity == (ssnmIdentity{}) {
		if sender.SSNM != nil || receiver.SSNM != nil || sender.Manifest != nil && sender.Manifest.SSNMBudgets != nil {
			return "", errors.New("ssnm evidence is present but the spec declares no SSNM load")
		}
		return "", nil
	}
	phase := ssnmPhaseMeasurement
	if warmup {
		phase = ssnmPhaseWarmup
	}
	if sender.Spec.SSNM.Phase == nil || *sender.Spec.SSNM.Phase != phase {
		return "", errors.New("spec.ssnm phase must be the cohort phase")
	}
	if receiver.SSNM == nil || receiver.SSNM.Generator == nil || receiver.SSNM.Generator.State == nil {
		return "", errors.New("SSNM-loaded receiver record requires ssnm generator evidence")
	}
	if warmup {
		if sender.SSNM != nil {
			return "", errors.New("a warm-up sender record carries no ssnm result")
		}
		return "", nil
	}
	if sender.SSNM == nil || sender.SSNM.Verdict == nil || sender.SSNM.Workload == nil {
		return "", errors.New("SSNM-loaded sender record requires ssnm verdict and workload")
	}
	recorded, err := ssnmIdentityFromSpec(&fixtureSpec{SSNM: sender.SSNM.Workload, Mode: sender.Spec.Mode, SharedClock: sender.Spec.SharedClock})
	if err != nil || recorded != identity || *sender.SSNM.Workload.Phase != phase {
		return "", errors.New("sender ssnm workload does not match the spec")
	}
	switch verdict := *sender.SSNM.Verdict; verdict {
	case "pass", "fail", "inconclusive":
		return verdict, nil
	default:
		return "", errors.New("sender ssnm verdict must be pass, fail or inconclusive")
	}
}

// applySSNMDecision folds the SSNM verdict into a unidirectional probe
// decision. Indication loss fails the probe exactly like DATA loss; an SSNM
// disturbance that did not run at its declared intensity cannot support a
// pass. A transport stall keeps its inconclusive DATA decision.
func applySSNMDecision(result *probeDecision, verdict string, stalled bool) {
	switch verdict {
	case "fail":
		if !stalled {
			result.Decision = string(perfstats.Fail)
			result.Reason = "ssnm-acceptance-failed"
		}
	case "inconclusive":
		if result.Decision == string(perfstats.Pass) {
			result.Decision = string(perfstats.Inconclusive)
			result.Reason = "ssnm-intensity-not-demonstrated"
		}
	}
}
