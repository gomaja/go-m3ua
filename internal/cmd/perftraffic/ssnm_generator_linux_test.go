package main

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

// TestSSNMGeneratorLeavesSGPWritesToTheLibrary runs the SGP generator over a
// real association and then publishes once more after its horizon. The
// library bounds its own writes (AssociationConfig.ControlWriteTimeout), and a
// socket write deadline also bounds them and closes the association once it
// passes, so the fixture must not install one on the SGP associations: a
// publication after the generator stopped has to reach the ASP like any
// other, with both ends still up.
func TestSSNMGeneratorLeavesSGPWritesToTheLibrary(testContext *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	clock, err := newMeasurementClock()
	if err != nil {
		testContext.Fatal(err)
	}
	ssnm := ssnmConfig{Rate: 20, APCs: 1, Records: 16, Subscribers: 1}
	control := newReceiverControl(1, maxOutstanding)
	// The reverse driver is only a probe here: registration hands it the
	// accepted SGP association exactly as the bidirectional receiver sees it.
	control.driver = &reverseDriver{ctx: ctx}
	control.ssnm = newSSNMGenerator(ctx, commandConfig{SSNM: ssnm}, clock)
	sgp, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleSGP})
	if err != nil {
		testContext.Fatal(err)
	}
	defer func() { _ = sgp.Close() }()
	control.ssnm.setReporter(sgp)
	address, err := sctp.ResolveSCTPAddr("sctp", "127.0.0.1:0")
	if err != nil {
		testContext.Fatal(err)
	}
	listener, err := sgp.Listen("m3ua", address, m3ua.NewListenerConfig(associationConfig("sgp")))
	if err != nil {
		testContext.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	fatal := make(chan error, 1)
	go acceptAndRead(ctx, listener, 1, control, fatal)
	server := httptest.NewServer(control.handler())
	defer server.Close()

	aspConfig := commandConfig{SCTPAddress: listener.Addr().String(), Associations: 1, SSNM: ssnm}
	asp, err := m3ua.NewEndpoint(senderEndpointConfig(aspConfig))
	if err != nil {
		testContext.Fatal(err)
	}
	defer func() { _ = asp.Close() }()
	aspAssociations, release, err := establishSenderAssociations(ctx, aspConfig, m3uaConnector{endpoint: asp})
	if err != nil {
		testContext.Fatal(err)
	}
	defer release()
	if err := waitForReady(ctx, server.URL, 1); err != nil {
		testContext.Fatal(err)
	}
	if err := control.ssnm.preloadStore(); err != nil {
		testContext.Fatalf("preload: %v", err)
	}

	domain, err := clock.Domain()
	if err != nil {
		testContext.Fatal(err)
	}
	now, err := clock.Now()
	if err != nil {
		testContext.Fatal(err)
	}
	start := now + int64(200*time.Millisecond)
	specification := runSpec{
		Clock: &sharedClockWindow{Domain: domain, Start: start, End: start + int64(300*time.Millisecond)},
		Drain: 200 * time.Millisecond,
		SSNM:  workloadRef(ssnm.workload(ssnmPhaseMeasurement, start)),
	}
	if err := control.ssnm.acceptSpec(specification); err != nil {
		testContext.Fatal(err)
	}
	control.ssnm.begin()
	select {
	case <-control.ssnm.done:
	case <-ctx.Done():
		testContext.Fatal("generator did not finish")
	}
	record := control.ssnm.cohortRecord(specification)
	if record.State != ssnmGeneratorComplete || record.FailedTotal != 0 || record.SentTotal == 0 {
		testContext.Fatalf("generator = %+v", record)
	}

	// Wait until the shared clock is well past the generator's hard stop and
	// the second the SGP used to add to it.
	horizon := specification.Clock.End + int64(specification.Drain) + int64(1500*time.Millisecond)
	for {
		now, err := clock.Now()
		if err != nil {
			testContext.Fatal(err)
		}
		if now >= horizon {
			break
		}
		time.Sleep(time.Duration(min(horizon-now, int64(50*time.Millisecond))))
	}
	control.mutex.Lock()
	sgpAssociations := append([]*m3ua.Association(nil), control.driver.associations...)
	control.mutex.Unlock()
	if len(sgpAssociations) != 1 {
		testContext.Fatalf("registered SGP associations = %d", len(sgpAssociations))
	}
	request := control.ssnm.request(control.ssnm.plan.chunk(0))
	if err := sgp.ReportDestinationAvailability(request); err != nil {
		var delivery *m3ua.SSNMDeliveryError
		if errors.As(err, &delivery) {
			for _, failure := range delivery.Failed {
				testContext.Logf("association %d: %v", failure.Association, failure.Cause)
			}
		}
		testContext.Fatalf("publication after the generator horizon: %v (SGP association: %v)", err, sgpAssociations[0].Err())
	}
	// Give a failed write's teardown time to show before checking both ends.
	time.Sleep(200 * time.Millisecond)
	for side, association := range map[string]*m3ua.Association{"SGP": sgpAssociations[0], "ASP": aspAssociations[0]} {
		select {
		case <-association.Done():
			testContext.Fatalf("%s association closed after the generator horizon: %v", side, association.Err())
		default:
		}
	}
	select {
	case err := <-fatal:
		testContext.Fatalf("receiver read loop failed: %v", err)
	default:
	}
	// End the read loop before the deferred closes tear the association down.
	cancel()
}
