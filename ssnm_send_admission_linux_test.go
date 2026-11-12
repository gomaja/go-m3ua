package m3ua

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestDUPUSingleDestinationOverSCTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	asp, sgp, err := setupConn(t, ctx, 3400)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = asp.Close(); _ = sgp.Close() }()
	observeSSNM(t, asp)
	build := func(apc *params.Param) *messages.DestinationUserPartUnavailable {
		return messages.NewDestinationUserPartUnavailable(params.NewNetworkAppearance(0), params.NewRoutingContext(1), apc, params.NewUserCause(params.SCCP, params.Unequipped), nil)
	}
	for _, apc := range []*params.Param{params.NewAffectedPointCode(100, 200), params.NewAffectedPointCodeWithMask(1, 300)} {
		if _, err := sgp.WriteSignal(build(apc)); !errors.Is(err, ErrInvalidParameterValue) {
			t.Fatalf("invalid DUPU admitted: %v", err)
		}
	}
	if _, err := sgp.WriteSignal(build(params.NewAffectedPointCode(0x123456))); err != nil {
		t.Fatal(err)
	}
	report := nextSSNMReport(t, asp)
	if report.Kind != SSNMDestinationUserPartUnavailableReport || len(report.Destinations) != 1 || report.Destinations[0] != (PointCodeRange{PointCode: 0x123456}) {
		t.Fatalf("DUPU report=%+v", report)
	}
}
