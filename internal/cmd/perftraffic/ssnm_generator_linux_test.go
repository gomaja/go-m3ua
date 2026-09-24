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
	ssnm := ssnmConfig{TotalRate: 20, APCs: 1, Records: 16, Subscribers: 1}
	control := newReceiverControl(1, maxOutstanding)
	// The reverse driver is only a probe here: registration hands it the
	// accepted SGP association exactly as the bidirectional receiver sees it.
	control.driver = &reverseDriver{ctx: ctx}
	control.ssnm = newSSNMGenerator(ctx, commandConfig{Associations: 1, SSNM: ssnm}, clock)
	sgp, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleSGP})
	if err != nil {
		testContext.Fatal(err)
	}
	defer func() { _ = sgp.Close() }()
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
	if err := control.ssnm.preloadStep(0); err != nil {
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
		Associations: 1,
		Clock:        &sharedClockWindow{Domain: domain, Start: start, End: start + int64(300*time.Millisecond)},
		Drain:        200 * time.Millisecond,
		SSNM:         workloadRef(ssnm.workload(ssnmPhaseMeasurement, start)),
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
	request := m3ua.DestinationAvailabilityRequest{
		Scope:        ssnmScope(),
		Destinations: []m3ua.PointCodeRange{{PointCode: control.ssnm.plan.pointCode(0, 0)}},
		Availability: m3ua.DestinationUnavailable,
	}
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

// TestSSNMTotalRateOverEightAssociations runs the SSNM workload end to end
// over eight real associations: the ASP requests the stepped preload over the
// control endpoint, the SGP generator writes message m on association m mod 8
// only at the total rate, and the ASP's subscribers, final-store check and
// verdict judge what arrived. It covers the steady row's shape (one-APC
// messages at 1,000/s) and the large row's (1,024-APC messages at 10/s, each
// partition holding 2,048 records, 16,384 in the store).
func TestSSNMTotalRateOverEightAssociations(testContext *testing.T) {
	const associations = 8
	for _, row := range []struct {
		name   string
		ssnm   ssnmConfig
		window time.Duration
	}{
		{"steady shape", ssnmConfig{TotalRate: 1000, APCs: 1, Records: 64, Subscribers: 3}, 600 * time.Millisecond},
		{"large shape", ssnmConfig{TotalRate: 10, APCs: ssnmMaxAPCs, Records: 2048, Subscribers: 3}, 1600 * time.Millisecond},
	} {
		testContext.Run(row.name, func(testContext *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			clock, err := newMeasurementClock()
			if err != nil {
				testContext.Fatal(err)
			}
			ssnm := row.ssnm
			ssnm.QueueBytes = m3ua.DefaultSSNMSubscriptionQueueBytes
			// The race detector slows every apply; the contract's 100 ms
			// apply budget is judged by release-build runs, and this test
			// judges where each message went.
			ssnm.Budgets = ssnmBudgets{ApplyP99: time.Second, Resync: ssnmDefaultResyncBudget, Recovery: ssnmDefaultRecoveryBudget}
			control := newReceiverControl(associations, maxOutstanding)
			control.ssnm = newSSNMGenerator(ctx, commandConfig{Associations: associations, SSNM: ssnm}, clock)
			sgp, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleSGP})
			if err != nil {
				testContext.Fatal(err)
			}
			defer func() { _ = sgp.Close() }()
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
			go acceptAndRead(ctx, listener, associations, control, fatal)
			server := httptest.NewServer(control.handler())
			defer server.Close()

			aspConfig := commandConfig{SCTPAddress: listener.Addr().String(), Associations: associations, SSNM: ssnm, PeerControl: server.URL, Drain: 200 * time.Millisecond, Duration: row.window}
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
			if err := waitForReady(ctx, server.URL, associations); err != nil {
				testContext.Fatal(err)
			}
			run, err := startSSNMLoad(ctx, aspConfig, asp, aspAssociations)
			if err != nil {
				testContext.Fatal(err)
			}
			defer run.close()
			if run.preload.StoreRecords != associations*ssnm.Records {
				testContext.Fatalf("store holds %d records after the preload, want %d", run.preload.StoreRecords, associations*ssnm.Records)
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
				Associations: associations,
				Clock:        &sharedClockWindow{Domain: domain, Start: start, End: start + int64(row.window)},
				Drain:        200 * time.Millisecond,
			}
			if err := run.attach(&specification, ssnmPhaseMeasurement); err != nil {
				testContext.Fatal(err)
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
			measurement := &cohortResult{Receiver: runRecord{SSNM: &ssnmRecord{Generator: control.ssnm.cohortRecord(specification)}}}
			run.finish(ctx, measurement)
			record := measurement.Sender.SSNM
			if record == nil {
				testContext.Fatal("finish attached no ssnm record")
			}
			generator := record.Generator
			plan := newSSNMPlan(ssnm, associations)
			offered := ssnm.TotalRate * uint64(row.window/time.Millisecond) / 1000
			testContext.Logf("verdict %s; offered %d (%v per association), sent %d, dispatch lag max %s, report duration p99 %s, send-buffer waits %d; delay p50 %s p99 %s max %s over %d receipts; store %d records",
				record.Verdict, generator.Offered, generator.OfferedPerAssociation, generator.SentTotal, generator.DispatchLag.Max, generator.ReportDuration.P99, generator.SendBufferWaits,
				record.Delay.Delay.P50, record.Delay.Delay.P99, record.Delay.Delay.Max, record.Delay.Delay.Count, record.Store.RecordsAtEnd)
			if record.Verdict != ssnmVerdictPass {
				testContext.Fatalf("verdict %s reasons %q", record.Verdict, record.Reasons)
			}
			if generator.Offered != offered || generator.Associations != associations || len(generator.OfferedPerAssociation) != associations {
				testContext.Fatalf("generator offered %d over %d associations, want %d over %d", generator.Offered, generator.Associations, offered, associations)
			}
			for association, share := range generator.OfferedPerAssociation {
				if want := plan.messagesFor(association, generator.EndMessage) - plan.messagesFor(association, generator.FirstMessage); share != want {
					testContext.Fatalf("association %d offered %d, want %d", association, share, want)
				}
			}
			expected := plan.expectedPositions(generator.SentTotal)
			for _, subscriber := range record.Subscribers {
				if subscriber.Partitions != associations || subscriber.MisScoped != 0 || subscriber.Unexpected != 0 || subscriber.Gaps != 0 {
					testContext.Fatalf("subscriber %d = %+v", subscriber.Index, subscriber)
				}
				for association, position := range subscriber.FinalPositions {
					if position != expected[association] {
						testContext.Fatalf("subscriber %d ended association %d's partition at %d, want %d", subscriber.Index, association, position, expected[association])
					}
				}
				if want := associations*plan.preloadMessages() + generator.SentTotal; subscriber.Accepted != want {
					testContext.Fatalf("subscriber %d accepted %d reports, want %d: each message reached one partition", subscriber.Index, subscriber.Accepted, want)
				}
			}
			if record.Store.RecordsAtEnd != associations*ssnm.Records || !record.Store.StateValidated || record.Store.StateMismatches != 0 {
				testContext.Fatalf("store = %+v", record.Store)
			}
			if record.Delay.Delay.Count != uint64(ssnm.Subscribers)*offered {
				testContext.Fatalf("delay join counted %d receipts, want %d", record.Delay.Delay.Count, uint64(ssnm.Subscribers)*offered)
			}
			select {
			case err := <-fatal:
				testContext.Fatalf("receiver read loop failed: %v", err)
			default:
			}
			cancel()
		})
	}
}
