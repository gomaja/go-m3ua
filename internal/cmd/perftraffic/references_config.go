package main

import (
	"errors"
	"flag"
	"fmt"
)

// Application route-reference churn (performance budgets section 4,
// "Application route-reference churn"). Route references are owned by the
// application: an application route table referencing associations and
// paths. The library has no route-reference call and its route inventory is
// immutable, so the workload proves that application routing churn causes no
// library or protocol activity and costs little DATA capacity. It is off
// unless -route-references is set, so every existing mode keeps its exact
// flags, specification and records.
const (
	// routeReferencesStatic is the matched unchanged-reference control: the
	// same application table on the DATA path, with no reference changing.
	routeReferencesStatic = "static"
	// routeReferencesChurn adds and removes references open-loop at the
	// configured rate, cycling the churned set 0 -> 1 -> peak -> 0.
	routeReferencesChurn = "churn"

	routeReferencePeak        = 1000
	routeReferenceDefaultRate = 1000
	routeReferenceMaxRate     = 100_000
	routeReferenceChurnCycle  = "0-1-1000-0"
	routeReferenceStaticCycle = "static"
)

// routeReferenceConfig is the ASP's -route-references selection.
type routeReferenceConfig struct {
	Mode string
	Rate uint64
}

func (config routeReferenceConfig) enabled() bool { return config.Mode != "" }

// routeReferenceSpec is the reference workload a cohort declares. It is part
// of the capacity workload identity, so churned and control campaigns cannot
// mix and neither mixes with a plain routed-direct campaign.
type routeReferenceSpec struct {
	Mode   string `json:"mode"`
	Rate   uint64 `json:"rate"`
	Peak   int    `json:"peak"`
	Stable int    `json:"stable"`
	Cycle  string `json:"cycle"`
}

func (config routeReferenceConfig) spec() *routeReferenceSpec {
	if config.Mode == routeReferencesChurn {
		return &routeReferenceSpec{Mode: routeReferencesChurn, Rate: config.Rate, Peak: routeReferencePeak, Stable: routingRouteCount, Cycle: routeReferenceChurnCycle}
	}
	return &routeReferenceSpec{Mode: routeReferencesStatic, Stable: routingRouteCount, Cycle: routeReferenceStaticCycle}
}

func registerRouteReferenceFlags(flagSet *flag.FlagSet, config *routeReferenceConfig) {
	flagSet.StringVar(&config.Mode, "route-references", "", "routed-direct application route table (ASP): static (unchanged-reference control) or churn")
	flagSet.Uint64Var(&config.Rate, "route-reference-rate", routeReferenceDefaultRate, "route-reference churn (ASP): reference add/remove operations per second")
}

// validateRouteReferenceConfig bounds the reference workload after the rest
// of the configuration has been validated.
func validateRouteReferenceConfig(flagSet *flag.FlagSet, config *commandConfig) error {
	explicit := make(map[string]bool)
	flagSet.Visit(func(set *flag.Flag) { explicit[set.Name] = true })
	references := &config.RouteReferences
	if !references.enabled() {
		if explicit["route-reference-rate"] {
			return errors.New("-route-reference-rate requires -route-references=churn")
		}
		*references = routeReferenceConfig{}
		return nil
	}
	switch {
	case references.Mode != routeReferencesStatic && references.Mode != routeReferencesChurn:
		return fmt.Errorf("route-references must be %s or %s", routeReferencesStatic, routeReferencesChurn)
	case config.Role != "asp":
		return errors.New("-route-references configures the ASP's application route table; the SGP follows the cohort specification")
	case config.Mode != modeRoutedDirect:
		return errors.New("-route-references runs with -mode=routed-direct: the application selects the path")
	case !config.SameHostClock:
		return errors.New("-route-references requires -same-host-clock: the churn schedule and activity windows use the shared clock")
	case config.SSNM.enabled() || config.SGPFailure != 0:
		return errors.New("-route-references is a separate workload from SSNM load and the SGP failure trial")
	case references.Mode == routeReferencesStatic && explicit["route-reference-rate"]:
		return errors.New("-route-reference-rate applies to -route-references=churn only")
	case references.Mode == routeReferencesChurn && (references.Rate == 0 || references.Rate > routeReferenceMaxRate):
		return fmt.Errorf("route-reference-rate must be between 1 and %d", routeReferenceMaxRate)
	}
	if references.Mode == routeReferencesStatic {
		references.Rate = 0
	}
	return nil
}

// validateRouteReferenceSpec checks a declared reference workload.
func validateRouteReferenceSpec(specification runSpec) error {
	declared := specification.RouteReferences
	if declared == nil {
		return nil
	}
	if specification.Mode != modeRoutedDirect || specification.Clock == nil {
		return errors.New("a route-reference workload is a shared-clock routed-direct cohort")
	}
	switch *declared {
	case routeReferenceSpec{Mode: routeReferencesStatic, Stable: routingRouteCount, Cycle: routeReferenceStaticCycle}:
		return nil
	case routeReferenceSpec{Mode: routeReferencesChurn, Rate: declared.Rate, Peak: routeReferencePeak, Stable: routingRouteCount, Cycle: routeReferenceChurnCycle}:
		if declared.Rate > 0 && declared.Rate <= routeReferenceMaxRate {
			return nil
		}
	}
	return errors.New("the declared route-reference workload is not the static control or the 0-1-1000-0 churn")
}
