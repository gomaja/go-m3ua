package main

import (
	"errors"

	"github.com/gomaja/go-m3ua/internal/perfstats"
)

// routeReferenceSpecEvidence is the application route-reference workload a
// perftraffic routed-direct cohort declares. It is absent from every other
// run.
type routeReferenceSpecEvidence struct {
	Mode   *string `json:"mode"`
	Rate   *uint64 `json:"rate"`
	Peak   *int    `json:"peak"`
	Stable *int    `json:"stable"`
	Cycle  *string `json:"cycle"`
}

// routeReferenceIdentity is the reference part of the workload identity. It
// is zero without the workload, so a churned campaign, its unchanged-reference
// control and a plain routed-direct campaign can never be mixed into one
// capacity decision.
type routeReferenceIdentity struct {
	Mode   string
	Rate   uint64
	Peak   int
	Stable int
	Cycle  string
}

// routeReferenceEvidence is the subset of a perftraffic route_references
// record the decision reads.
type routeReferenceEvidence struct {
	Verdict  *string                     `json:"verdict"`
	Workload *routeReferenceSpecEvidence `json:"workload"`
}

const (
	routeReferencesStatic     = "static"
	routeReferencesChurn      = "churn"
	routeReferencePeak        = 1000
	routeReferenceStable      = 1000
	routeReferenceMaxRate     = 100_000
	routeReferenceChurnCycle  = "0-1-1000-0"
	routeReferenceStaticCycle = "static"
)

var errRouteReferenceShape = errors.New("spec.route_references must be the static control or the 0-1-1000-0 churn")

func routeReferenceIdentityFrom(declared *routeReferenceSpecEvidence) (routeReferenceIdentity, error) {
	if declared.Mode == nil || declared.Rate == nil || declared.Peak == nil || declared.Stable == nil || declared.Cycle == nil {
		return routeReferenceIdentity{}, errors.New("spec.route_references mode, rate, peak, stable and cycle are required")
	}
	identity := routeReferenceIdentity{Mode: *declared.Mode, Rate: *declared.Rate, Peak: *declared.Peak, Stable: *declared.Stable, Cycle: *declared.Cycle}
	switch identity {
	case routeReferenceIdentity{Mode: routeReferencesStatic, Stable: routeReferenceStable, Cycle: routeReferenceStaticCycle}:
		return identity, nil
	case routeReferenceIdentity{Mode: routeReferencesChurn, Rate: identity.Rate, Peak: routeReferencePeak, Stable: routeReferenceStable, Cycle: routeReferenceChurnCycle}:
		if identity.Rate > 0 && identity.Rate <= routeReferenceMaxRate {
			return identity, nil
		}
	}
	return routeReferenceIdentity{}, errRouteReferenceShape
}

func routeReferenceIdentityFromSpec(spec *fixtureSpec) (routeReferenceIdentity, error) {
	if spec.RouteReferences == nil {
		return routeReferenceIdentity{}, nil
	}
	identity, err := routeReferenceIdentityFrom(spec.RouteReferences)
	switch {
	case err != nil:
		return routeReferenceIdentity{}, err
	case spec.Mode == nil || *spec.Mode != routedDirectMode:
		return routeReferenceIdentity{}, errors.New("spec.route_references is defined for routed-direct cohorts only")
	case spec.SharedClock == nil:
		return routeReferenceIdentity{}, errors.New("spec.route_references requires a shared clock window")
	}
	return identity, nil
}

// rejectStrayRouteReferences refuses route-reference evidence in records
// whose cohort cannot carry it: legacy single sender records and
// bidirectional records.
func rejectStrayRouteReferences(records ...*fixtureEvidence) error {
	for _, record := range records {
		if record != nil && (record.RouteReferences != nil || record.Spec != nil && record.Spec.RouteReferences != nil) {
			return errors.New("route-reference evidence is present but this cohort cannot carry the route-reference workload")
		}
	}
	return nil
}

// routeReferenceCohortVerdict validates the evidence a route-reference cohort
// must carry and returns its verdict. The receiver record carries none.
func routeReferenceCohortVerdict(identity routeReferenceIdentity, sender, receiver *fixtureEvidence) (string, error) {
	if receiver.RouteReferences != nil {
		return "", errors.New("a receiver record carries no route_references result")
	}
	if identity == (routeReferenceIdentity{}) {
		if sender.RouteReferences != nil {
			return "", errors.New("route_references evidence is present but the spec declares no route-reference workload")
		}
		return "", nil
	}
	evidence := sender.RouteReferences
	if evidence == nil || evidence.Verdict == nil || evidence.Workload == nil {
		return "", errors.New("a route-reference sender record requires route_references verdict and workload")
	}
	recorded, err := routeReferenceIdentityFrom(evidence.Workload)
	if err != nil || recorded != identity {
		return "", errors.New("sender route_references workload does not match the spec")
	}
	switch verdict := *evidence.Verdict; verdict {
	case "pass", "fail", "inconclusive":
		return verdict, nil
	default:
		return "", errors.New("sender route_references verdict must be pass, fail or inconclusive")
	}
}

// applyRouteReferenceDecision folds the route-reference verdict into a
// unidirectional probe decision exactly as SSNM load is folded: a
// reconnection, AS deactivation or library indication fails the probe like
// DATA loss, and churn that did not run at its declared intensity cannot
// support a pass. A transport stall keeps its inconclusive DATA decision.
func applyRouteReferenceDecision(result *probeDecision, verdict string, stalled bool) {
	switch verdict {
	case "fail":
		if !stalled {
			result.Decision = string(perfstats.Fail)
			result.Reason = "route-reference-acceptance-failed"
		}
	case "inconclusive":
		if result.Decision == string(perfstats.Pass) {
			result.Decision = string(perfstats.Inconclusive)
			result.Reason = "route-reference-intensity-not-demonstrated"
		}
	}
}
