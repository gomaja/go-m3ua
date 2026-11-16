package m3ua

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

// Reflection lets the same boundary proof run against the earlier single-result
// method, without mistaking the intentional signature break for a red test.
func callStatusIdentityOffer(p *MTPStatusProcedure, status MTPDestinationStatus) (*MTPIndication, error) {
	values := reflect.ValueOf(p).MethodByName("Offered").Call([]reflect.Value{reflect.ValueOf(status)})
	var indication *MTPIndication
	if !values[0].IsNil() {
		indication = values[0].Interface().(*MTPIndication)
	}
	var err error
	if len(values) == 2 && !values[1].IsNil() {
		err = values[1].Interface().(error)
	}
	return indication, err
}

func TestApplicationStatusProcedureBoundsRetainedRouteIdentity(t *testing.T) {
	for _, scenario := range []string{"congested", "clear", "received-unknown", "transport-unknown", "national-priority"} {
		t.Run(scenario, func(t *testing.T) {
			method := CongestionNationalWithoutPriorities
			if scenario == "national-priority" {
				method = CongestionNationalWithPriorities
			}
			p, err := NewMTPStatusProcedure(CongestionProfile{Method: method})
			if err != nil {
				t.Fatal(err)
			}
			valid := congestedProcedureStatus()
			valid.Destination.MTPRoute = MTPRouteID(strings.Repeat("é", 127) + "x")
			indication, err := callStatusIdentityOffer(p, valid)
			if err != nil || p.destination != valid.Destination {
				t.Fatal("255-octet application route refused or not retained", err)
			}
			if method != CongestionNationalWithPriorities && (indication == nil || indication.Destination != valid) {
				t.Fatal("at-limit route lost its initial status", indication)
			}
			beforeDestination, beforeInterval := p.destination, p.interval
			invalid := valid
			invalid.Destination.MTPRoute = MTPRouteID(strings.Repeat("é", 128))
			switch scenario {
			case "clear":
				invalid.Congested = false
			case "received-unknown":
				invalid.ReceivedCongestionUnknown = true
			case "transport-unknown":
				invalid.TransportCongestionUnknown = true
			}
			indication, err = callStatusIdentityOffer(p, invalid)
			if !errors.Is(err, ErrInvalidParameterValue) || indication != nil {
				t.Error("application status accepted an overlong route identity", indication, err)
			} else {
				requireIdentityLengthDetails(t, err, 256, 255)
			}
			if p.destination != beforeDestination || p.interval != beforeInterval {
				t.Error("rejected route identity changed the destination or offered cadence")
			}
			if indication, err = callStatusIdentityOffer(p, valid); err != nil || indication != nil {
				t.Error("rejected route identity restarted the valid interval", indication, err)
			}
		})
	}
}
