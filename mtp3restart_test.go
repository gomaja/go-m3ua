package m3ua

import (
	"errors"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestMTP3RestartPublishesIsolationAndFinalState(t *testing.T) {
	listener, firstApplicationServer, first, firstSent := restartFixture(t, 1, 2)
	secondApplicationServer := listener.as.get(2)
	second, secondSent := addDistributionASP(t, listener, StateASPInactive, 1, 2)
	restartAttachConn(listener, second)
	restartActivateASP(firstApplicationServer, first, 1)
	restartActivateASP(secondApplicationServer, second, 2)
	firstSent.reset()
	secondSent.reset()

	affected := []AffectedDestination{
		restartDestination(1, 0x111111, 0),
		restartDestination(1, 0x222222, 0),
		restartDestination(1, 0x333333, 0),
		restartDestination(1, 0x444400, 8),
	}
	restart, err := listener.BeginMTP3Restart(affected...)
	if err != nil {
		t.Fatalf("begin MTP3 restart: %v", err)
	}
	if restart == nil {
		t.Fatal("begin MTP3 restart returned a nil handle")
	}

	beginMessages := ssnmMessages(firstSent.snapshot())
	if len(beginMessages) != len(affected) {
		t.Fatalf("restart begin emitted %d SSNM messages, want %d", len(beginMessages), len(affected))
	}
	for index, message := range beginMessages {
		if _, ok := message.(*messages.DestinationUnavailable); !ok {
			t.Fatalf("restart begin message %d = %T, want DUNA", index, message)
		}
		assertRestartScope(t, message, affected[index])
	}
	if got := len(ssnmMessages(secondSent.snapshot())); got != 0 {
		t.Fatalf("unconcerned ASP received %d restart DUNAs, want 0", got)
	}
	if first.State() != StateASPActive || !first.activeForRoutingContext(1) {
		t.Fatalf("restart changed ASP state/scope to %v, active RC 1 = %v", first.State(), first.activeForRoutingContext(1))
	}
	if got := firstApplicationServer.State(); got != ASActive {
		t.Fatalf("restart changed AS state to %v, want ACTIVE", got)
	}

	firstSent.reset()
	if err := restart.Update(affected[0], DestinationAvailable); err != nil {
		t.Fatalf("stage available: %v", err)
	}
	if err := restart.Update(affected[1], DestinationRestricted); err != nil {
		t.Fatalf("stage restricted: %v", err)
	}
	if err := restart.Update(affected[2], DestinationUnavailable); err != nil {
		t.Fatalf("stage unavailable: %v", err)
	}
	if err := restart.Update(affected[3], DestinationCongested); err != nil {
		t.Fatalf("stage congested: %v", err)
	}
	if got := len(ssnmMessages(firstSent.snapshot())); got != 0 {
		t.Fatalf("staged restart updates emitted %d SSNM messages before completion", got)
	}

	if err := restart.Complete(); err != nil {
		t.Fatalf("complete MTP3 restart: %v", err)
	}
	wantKinds := []any{
		(*messages.DestinationAvailable)(nil),
		(*messages.DestinationRestricted)(nil),
		(*messages.SignallingCongestion)(nil),
		(*messages.DestinationAvailable)(nil),
	}
	completed := ssnmMessages(firstSent.snapshot())
	if len(completed) != len(wantKinds) {
		t.Fatalf("restart completion emitted %d SSNM messages, want %d: %v",
			len(completed), len(wantKinds), typeNames(completed))
	}
	for index, kind := range wantKinds {
		if !sameSSNMKind(completed[index], kind) {
			t.Fatalf("restart completion message %d = %T, want %T", index, completed[index], kind)
		}
	}
	if got := len(ssnmMessages(secondSent.snapshot())); got != 0 {
		t.Fatalf("unconcerned ASP received %d completion messages, want 0", got)
	}

	for index, want := range []DestinationState{
		DestinationAvailable,
		DestinationRestricted,
		DestinationUnavailable,
		DestinationCongested,
	} {
		state, known := listener.DestinationStateForNetworkAndRoutingContext(
			7, 1, affected[index].PointCode,
		)
		if !known || state != want {
			t.Errorf("destination %#x after completion = (%v, %v), want (%v, true)",
				affected[index].PointCode, state, known, want)
		}
	}
}

func TestMTP3RestartStagesOrdinaryDestinationReports(t *testing.T) {
	listener, applicationServer, asp, sent := restartFixture(t, 1)
	restartActivateASP(applicationServer, asp, 1)
	sent.reset()
	destination := restartDestination(1, 0x123456, 0)

	restart, err := listener.BeginMTP3Restart(destination)
	if err != nil {
		t.Fatalf("begin MTP3 restart: %v", err)
	}
	sent.reset()
	if err := listener.ReportDestinationRangeForNetworkAndRoutingContext(
		7, 1, destination.PointCode, destination.Mask, DestinationAvailable,
	); err != nil {
		t.Fatalf("report during restart: %v", err)
	}
	if got := len(ssnmMessages(sent.snapshot())); got != 0 {
		t.Fatalf("ordinary report emitted %d SSNM messages during restart", got)
	}
	if state, known := listener.DestinationStateForNetworkAndRoutingContext(
		7, 1, destination.PointCode,
	); !known || state != DestinationUnavailable {
		t.Fatalf("effective state during restart = (%v, %v), want (Unavailable, true)", state, known)
	}

	if err := restart.Complete(); err != nil {
		t.Fatalf("complete MTP3 restart: %v", err)
	}
	completed := ssnmMessages(sent.snapshot())
	if len(completed) != 1 {
		t.Fatalf("completion emitted %d messages, want one DAVA", len(completed))
	}
	if _, ok := completed[0].(*messages.DestinationAvailable); !ok {
		t.Fatalf("completion message = %T, want DAVA", completed[0])
	}
}

func TestDialingSGPAssociationRunsMTP3RestartProcedure(t *testing.T) {
	listener, applicationServer, association, sent := restartFixture(t, 1)
	association.listener = nil
	association.mtp3Restarts = &mtp3RestartRegistry{}
	association.destinations = newDestinations()
	restartActivateASP(applicationServer, association, 1)
	sent.reset()
	destination := restartDestination(1, 0x123456, 0)

	restart, err := association.BeginMTP3Restart(destination)
	if err != nil {
		t.Fatalf("begin dialing SGP MTP3 restart: %v", err)
	}
	assertOnlySSNMKind(t, sent.snapshot(), (*messages.DestinationUnavailable)(nil))
	sent.reset()

	if err := association.ReportDestinationRangeForNetworkAndRoutingContext(
		7, 1, destination.PointCode, destination.Mask, DestinationAvailable,
	); err != nil {
		t.Fatalf("report during dialing SGP restart: %v", err)
	}
	if got := len(ssnmMessages(sent.snapshot())); got != 0 {
		t.Fatalf("staged dialing SGP update emitted %d messages before completion", got)
	}
	if err := restart.Complete(); err != nil {
		t.Fatalf("complete dialing SGP MTP3 restart: %v", err)
	}
	assertOnlySSNMKind(t, sent.snapshot(), (*messages.DestinationAvailable)(nil))
	if got := association.DestinationStateForNetworkAndRoutingContext(7, 1, destination.PointCode); got != DestinationAvailable {
		t.Fatalf("destination state after dialing SGP restart = %v, want available", got)
	}

	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestASPAssociationRejectsMTP3RestartProcedure(t *testing.T) {
	association, _ := newTestConn(t, StateASPActive, RoleASP)
	restart, err := association.BeginMTP3Restart(restartDestination(1, 0x123456, 0))
	if restart != nil || !errors.Is(err, ErrUnsupportedRole) {
		t.Fatalf("ASP BeginMTP3Restart = (%v, %v), want (nil, ErrUnsupportedRole)", restart, err)
	}
}

func TestASPListenerRejectsMTP3RestartProcedure(t *testing.T) {
	endpoint, err := NewEndpoint(RoleASP)
	if err != nil {
		t.Fatalf("NewEndpoint(RoleASP): %v", err)
	}
	listener := newListener(endpoint, NewListenerConfig(mcASPConfig(1)))
	restart, err := listener.BeginMTP3Restart(restartDestination(1, 0x123456, 0))
	if restart != nil || !errors.Is(err, ErrUnsupportedRole) {
		t.Fatalf("ASP Listener BeginMTP3Restart = (%v, %v), want (nil, ErrUnsupportedRole)", restart, err)
	}
}

func TestDialingSGPRestartHandlesAreInvalidatedByAssociationClose(t *testing.T) {
	_, applicationServer, association, _ := restartFixture(t, 1)
	association.listener = nil
	association.mtp3Restarts = &mtp3RestartRegistry{}
	association.destinations = newDestinations()
	association.releaseEndpointStateOwner = func() {}
	restartActivateASP(applicationServer, association, 1)
	destination := restartDestination(1, 0x123456, 0)

	restart, err := association.BeginMTP3Restart(destination)
	if err != nil {
		t.Fatalf("begin dialing SGP MTP3 restart: %v", err)
	}
	if err := association.Close(); err != nil {
		t.Fatalf("close dialing SGP Association: %v", err)
	}
	if err := restart.Update(destination, DestinationAvailable); !errors.Is(err, ErrStaleMTP3Restart) {
		t.Fatalf("update after Association.Close = %v, want ErrStaleMTP3Restart", err)
	}
	if err := restart.Complete(); !errors.Is(err, ErrStaleMTP3Restart) {
		t.Fatalf("complete after Association.Close = %v, want ErrStaleMTP3Restart", err)
	}
}

func TestDialingSGPRestartIsInvalidatedBeforeAssociationCleanup(t *testing.T) {
	_, applicationServer, association, _ := restartFixture(t, 1)
	association.listener = nil
	association.mtp3Restarts = &mtp3RestartRegistry{}
	association.destinations = newDestinations()
	association.releaseEndpointStateOwner = func() {}
	restartActivateASP(applicationServer, association, 1)
	destination := restartDestination(1, 0x123456, 0)

	restart, err := association.BeginMTP3Restart(destination)
	if err != nil {
		t.Fatalf("begin dialing SGP MTP3 restart: %v", err)
	}
	if err := restart.Update(destination, DestinationAvailable); err != nil {
		t.Fatalf("stage available destination: %v", err)
	}
	restart.target.publish = func([]DestinationRange, bool, bool, bool) error { return nil }

	association.muState.Lock()
	closeResult := make(chan error, 1)
	go func() {
		closeResult <- association.Close()
	}()
	<-association.Done()
	for range 100 {
		runtime.Gosched()
	}

	completeErr := restart.Complete()
	association.muState.Unlock()
	if err := <-closeResult; err != nil {
		t.Fatalf("close dialing SGP Association: %v", err)
	}
	if !errors.Is(completeErr, ErrStaleMTP3Restart) {
		t.Fatalf("complete after Association.Done = %v, want ErrStaleMTP3Restart", completeErr)
	}
	if got := association.DestinationStateForNetworkAndRoutingContext(7, 1, destination.PointCode); got != DestinationUnavailable {
		t.Fatalf("destination state after close race = %v, want unavailable", got)
	}
}

func TestAcceptedSGPAssociationUsesListenerMTP3RestartState(t *testing.T) {
	listener, applicationServer, association, _ := restartFixture(t, 1)
	restartActivateASP(applicationServer, association, 1)
	destination := restartDestination(1, 0x123456, 0)

	restart, err := association.BeginMTP3Restart(destination)
	if err != nil {
		t.Fatalf("accepted SGP begin MTP3 restart: %v", err)
	}
	if overlapping, overlapErr := listener.BeginMTP3Restart(destination); overlapping != nil ||
		!errors.Is(overlapErr, ErrMTP3RestartInProgress) {
		t.Fatalf("Listener overlapping restart = (%v, %v), want (nil, ErrMTP3RestartInProgress)",
			overlapping, overlapErr)
	}
	if err := restart.Complete(); err != nil {
		t.Fatalf("complete accepted SGP restart: %v", err)
	}
}

func TestSGPAssociationMTP3RestartRequiresEstablishedOpenAssociation(t *testing.T) {
	association := newAssociation(RoleSGP, mcSGPConfig())
	destination := restartDestination(1, 0x123456, 0)

	if restart, err := association.BeginMTP3Restart(destination); restart != nil ||
		!errors.Is(err, ErrNotEstablished) {
		t.Fatalf("unestablished BeginMTP3Restart = (%v, %v), want (nil, ErrNotEstablished)", restart, err)
	}
	if err := association.Close(); err != nil {
		t.Fatalf("close unestablished SGP Association: %v", err)
	}
	if restart, err := association.BeginMTP3Restart(destination); restart != nil ||
		!errors.Is(err, ErrAssociationClosed) {
		t.Fatalf("closed BeginMTP3Restart = (%v, %v), want (nil, ErrAssociationClosed)", restart, err)
	}
}

func TestMTP3RestartForcesDAUDUnavailableUntilCompletion(t *testing.T) {
	listener, applicationServer, asp, sent := restartFixture(t, 1)
	restartActivateASP(applicationServer, asp, 1)
	destination := restartDestination(1, 0x123456, 0)
	if err := listener.ReportDestinationRangeForNetworkAndRoutingContext(
		7, 1, destination.PointCode, destination.Mask, DestinationAvailable,
	); err != nil {
		t.Fatalf("seed available destination: %v", err)
	}
	sent.reset()

	restart, err := listener.BeginMTP3Restart(destination)
	if err != nil {
		t.Fatalf("begin MTP3 restart: %v", err)
	}
	sent.reset()
	if err := restart.Update(destination, DestinationAvailable); err != nil {
		t.Fatalf("stage available: %v", err)
	}
	listener.destinations.setRanges([]DestinationRange{{
		NetworkAppearance:    7,
		NetworkAppearanceSet: true,
		RoutingContext:       1,
		RoutingContextSet:    true,
		PointCode:            destination.PointCode &^ 0xff,
		Mask:                 8,
		State:                DestinationAvailable,
	}})
	audit := messages.NewDestinationStateAudit(
		params.NewNetworkAppearance(7), params.NewRoutingContext(1),
		params.NewAffectedPointCode(destination.PointCode), nil,
	)
	if err := asp.handleDestinationStateAudit(audit); err != nil {
		t.Fatalf("audit during restart: %v", err)
	}
	assertOnlySSNMKind(t, sent.snapshot(), (*messages.DestinationUnavailable)(nil))
	sent.reset()
	broadAudit := messages.NewDestinationStateAudit(
		params.NewNetworkAppearance(7), params.NewRoutingContext(1),
		params.NewAffectedPointCodeWithMask(8, destination.PointCode), nil,
	)
	if err := asp.handleDestinationStateAudit(broadAudit); err != nil {
		t.Fatalf("overlapping range audit during restart: %v", err)
	}
	assertOnlySSNMKind(t, sent.snapshot(), (*messages.DestinationUnavailable)(nil))

	sent.reset()
	if err := restart.Complete(); err != nil {
		t.Fatalf("complete MTP3 restart: %v", err)
	}
	sent.reset()
	if err := asp.handleDestinationStateAudit(audit); err != nil {
		t.Fatalf("audit after restart: %v", err)
	}
	assertOnlySSNMKind(t, sent.snapshot(), (*messages.DestinationAvailable)(nil))
}

func TestMTP3RestartCompletionUsesCurrentActiveSnapshot(t *testing.T) {
	listener, applicationServer, first, firstSent := restartFixture(t, 1)
	second, secondSent := addDistributionASP(t, listener, StateASPInactive, 1)
	restartAttachConn(listener, second)
	restartActivateASP(applicationServer, first, 1)
	destination := restartDestination(1, 0x123456, 0)

	restart, err := listener.BeginMTP3Restart(destination)
	if err != nil {
		t.Fatalf("begin MTP3 restart: %v", err)
	}
	if err := restart.Update(destination, DestinationAvailable); err != nil {
		t.Fatalf("stage available: %v", err)
	}
	first.noteRoutingContextsInactive([]uint32{1})
	first.setState(StateASPInactive)
	applicationServer.setASPState(first, StateASPInactive, time.Hour)
	restartActivateASP(applicationServer, second, 1)
	firstSent.reset()
	secondSent.reset()

	if err := restart.Complete(); err != nil {
		t.Fatalf("complete MTP3 restart: %v", err)
	}
	if got := len(ssnmMessages(firstSent.snapshot())); got != 0 {
		t.Fatalf("now-inactive ASP received %d completion messages, want 0", got)
	}
	assertOnlySSNMKind(t, secondSent.snapshot(), (*messages.DestinationAvailable)(nil))
}

func TestMTP3RestartFanoutContinuesAfterPeerFailure(t *testing.T) {
	listener, applicationServer, failing, _ := restartFixture(t, 1)
	healthy, healthySent := addDistributionASP(t, listener, StateASPInactive, 1)
	restartAttachConn(listener, healthy)
	restartActivateASP(applicationServer, failing, 1)
	restartActivateASP(applicationServer, healthy, 1)
	failure := errors.New("injected restart write failure")
	failing.signalWriter = func(messages.M3UA) (int, error) { return 0, failure }

	restart, err := listener.BeginMTP3Restart(restartDestination(1, 0x123456, 0))
	if restart == nil {
		t.Fatal("failed fanout lost the active restart handle")
	}
	if !errors.Is(err, failure) {
		t.Fatalf("begin error = %v, want injected failure", err)
	}
	assertOnlySSNMKind(t, healthySent.snapshot(), (*messages.DestinationUnavailable)(nil))

	failing.signalWriter = func(message messages.M3UA) (int, error) { return message.MarshalLen(), nil }
	if err := restart.Complete(); err != nil {
		t.Fatalf("complete failed restart: %v", err)
	}
}

func TestMTP3RestartValidationOverlapAndIdempotence(t *testing.T) {
	listener, applicationServer, asp, sent := restartFixture(t, 1)
	restartActivateASP(applicationServer, asp, 1)
	sent.reset()

	invalid := restartDestination(2, 0x123456, 0)
	if restart, err := listener.BeginMTP3Restart(invalid); restart != nil || !errors.Is(err, ErrInvalidRoutingContext) {
		t.Fatalf("invalid begin = (%v, %v), want (nil, ErrInvalidRoutingContext)", restart, err)
	}
	if got := len(ssnmMessages(sent.snapshot())); got != 0 {
		t.Fatalf("invalid begin emitted %d SSNM messages", got)
	}
	if _, known := listener.DestinationStateForNetworkAndRoutingContext(7, 2, invalid.PointCode); known {
		t.Fatal("invalid begin committed destination state")
	}

	destination := restartDestination(1, 0x123456, 8)
	restart, err := listener.BeginMTP3Restart(destination)
	if err != nil {
		t.Fatalf("begin MTP3 restart: %v", err)
	}
	writesAfterBegin := len(ssnmMessages(sent.snapshot()))
	if overlapping, err := listener.BeginMTP3Restart(
		restartDestination(1, 0x123400, 4),
	); overlapping != nil || !errors.Is(err, ErrMTP3RestartInProgress) {
		t.Fatalf("overlapping begin = (%v, %v), want (nil, ErrMTP3RestartInProgress)", overlapping, err)
	}
	if got := len(ssnmMessages(sent.snapshot())); got != writesAfterBegin {
		t.Fatalf("overlapping begin emitted %d new messages", got-writesAfterBegin)
	}

	if err := restart.Complete(); err != nil {
		t.Fatalf("complete MTP3 restart: %v", err)
	}
	writesAfterComplete := len(ssnmMessages(sent.snapshot()))
	if err := restart.Complete(); err != nil {
		t.Fatalf("idempotent complete: %v", err)
	}
	if got := len(ssnmMessages(sent.snapshot())); got != writesAfterComplete {
		t.Fatalf("second completion emitted %d messages", got-writesAfterComplete)
	}
	if err := restart.Update(destination, DestinationAvailable); !errors.Is(err, ErrStaleMTP3Restart) {
		t.Fatalf("update through completed handle = %v, want ErrStaleMTP3Restart", err)
	}
}

func TestMTP3RestartStatusPrecedesAspActiveAck(t *testing.T) {
	listener, _, asp, sent := restartFixture(t, 1)
	destination := restartDestination(1, 0x123456, 0)
	restart, err := listener.BeginMTP3Restart(destination)
	if err != nil {
		t.Fatalf("begin MTP3 restart: %v", err)
	}
	sent.reset()

	if err := asp.handleAspActive(messages.NewAspActive(
		asp.cfg.TrafficModeType.Copy(), params.NewRoutingContext(1), nil,
	)); err != nil {
		t.Fatalf("handle ASP Active: %v", err)
	}
	written := sent.snapshot()
	if got := typeNames(written); !reflect.DeepEqual(got, []string{"Destination Unavailable", "ASP Active Ack"}) {
		t.Fatalf("pre-Ack restart messages = %v, want [Destination Unavailable ASP Active Ack]", got)
	}
	assertRestartScope(t, written[0], destination)

	if err := restart.Complete(); err != nil {
		t.Fatalf("complete MTP3 restart: %v", err)
	}
}

func TestMTP3RestartPreAckWriteFailureWithholdsAck(t *testing.T) {
	listener, _, asp, _ := restartFixture(t, 1)
	restart, err := listener.BeginMTP3Restart(restartDestination(1, 0x123456, 0))
	if err != nil {
		t.Fatalf("begin MTP3 restart: %v", err)
	}
	failure := errors.New("injected pre-Ack DUNA failure")
	var attempts []messages.M3UA
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		attempts = append(attempts, message)
		return 0, failure
	}

	err = asp.handleAspActive(messages.NewAspActive(
		asp.cfg.TrafficModeType.Copy(), params.NewRoutingContext(1), nil,
	))
	if !errors.Is(err, failure) {
		t.Fatalf("handle ASP Active error = %v, want injected failure", err)
	}
	if got := typeNames(attempts); !reflect.DeepEqual(got, []string{"Destination Unavailable"}) {
		t.Fatalf("writes after failed restart status = %v, want only DUNA attempt", got)
	}

	asp.signalWriter = func(message messages.M3UA) (int, error) { return message.MarshalLen(), nil }
	if err := restart.Complete(); err != nil {
		t.Fatalf("complete MTP3 restart: %v", err)
	}
}

func TestMTP3RestartHandlesAreInvalidatedByListenerClose(t *testing.T) {
	listener, _, _, _ := restartFixture(t, 1)
	destination := restartDestination(1, 0x123456, 0)
	restart, err := listener.BeginMTP3Restart(destination)
	if err != nil {
		t.Fatalf("begin MTP3 restart: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("close Listener: %v", err)
	}
	if next, err := listener.BeginMTP3Restart(destination); next != nil || !errors.Is(err, ErrAssociationClosed) {
		t.Fatalf("begin after close = (%v, %v), want (nil, ErrAssociationClosed)", next, err)
	}
	if err := restart.Update(destination, DestinationAvailable); !errors.Is(err, ErrStaleMTP3Restart) {
		t.Fatalf("update after close = %v, want ErrStaleMTP3Restart", err)
	}
	if err := restart.Complete(); !errors.Is(err, ErrStaleMTP3Restart) {
		t.Fatalf("complete after close = %v, want ErrStaleMTP3Restart", err)
	}
	if err := listener.ReportDestinationRangeForNetworkAndRoutingContext(
		7, 1, destination.PointCode, destination.Mask, DestinationAvailable,
	); !errors.Is(err, ErrAssociationClosed) {
		t.Fatalf("report after close = %v, want ErrAssociationClosed", err)
	}
}

func TestMTP3RestartReportRacingCompletionCannotBeLost(t *testing.T) {
	listener, _, _, _ := restartFixture(t, 1)
	destination := restartDestination(1, 0x123456, 0)
	for iteration := 0; iteration < 200; iteration++ {
		restart, err := listener.BeginMTP3Restart(destination)
		if err != nil {
			t.Fatalf("iteration %d begin: %v", iteration, err)
		}
		if err := restart.Update(destination, DestinationUnavailable); err != nil {
			t.Fatalf("iteration %d seed unavailable update: %v", iteration, err)
		}
		start := make(chan struct{})
		var waitGroup sync.WaitGroup
		var reportErr, completeErr error
		waitGroup.Add(2)
		go func() {
			defer waitGroup.Done()
			<-start
			reportErr = listener.ReportDestinationRangeForNetworkAndRoutingContext(
				7, 1, destination.PointCode, destination.Mask, DestinationAvailable,
			)
		}()
		go func() {
			defer waitGroup.Done()
			<-start
			completeErr = restart.Complete()
		}()
		close(start)
		waitGroup.Wait()
		if reportErr != nil || completeErr != nil {
			t.Fatalf("iteration %d errors = (%v, %v)", iteration, reportErr, completeErr)
		}
		state, known := listener.DestinationStateForNetworkAndRoutingContext(
			7, 1, destination.PointCode,
		)
		if !known || state != DestinationAvailable {
			t.Fatalf("iteration %d final state = (%v, %v), want (Available, true)", iteration, state, known)
		}
	}
}

func TestDestinationStateSSNMOmitsEmptyRoutingContext(t *testing.T) {
	messagesToWrite := destinationStateSSNMs(DestinationRange{
		PointCode: 0x123456,
		State:     DestinationUnavailable,
	}, nil, DestinationUnavailable, false)
	if len(messagesToWrite) != 1 {
		t.Fatalf("built %d messages, want one", len(messagesToWrite))
	}
	duna, ok := messagesToWrite[0].(*messages.DestinationUnavailable)
	if !ok {
		t.Fatalf("message = %T, want DUNA", messagesToWrite[0])
	}
	if duna.RoutingContext != nil {
		t.Fatalf("dedicated-association DUNA carried empty Routing Context %#v", duna.RoutingContext)
	}
}

func FuzzMTP3RestartProcedure(f *testing.F) {
	f.Add(uint32(0x123456), uint8(0), uint32(1), uint8(DestinationAvailable), true, true)
	f.Add(uint32(0xffffff), uint8(24), uint32(2), uint8(255), true, false)
	f.Fuzz(func(t *testing.T, pointCode uint32, mask uint8, routingContext uint32, rawState uint8, routingContextSet, networkAppearanceSet bool) {
		config := newSGPAssociationConfigForTest(
			&HeartbeatInfo{Enabled: false}, 1, 2, 0,
			params.TrafficModeLoadshare, 7, 0, []uint32{1},
			params.ServiceIndSCCP, 0, 0, 1,
		)
		listener := newSGPListener(NewListenerConfig(config))
		listener.AssociationConfig.CorrelationID = nil
		destination := AffectedDestination{
			NetworkAppearance:    7,
			NetworkAppearanceSet: networkAppearanceSet,
			RoutingContext:       routingContext,
			RoutingContextSet:    routingContextSet,
			PointCode:            pointCode,
			Mask:                 mask,
		}
		restart, err := listener.BeginMTP3Restart(destination)
		if routingContextSet && routingContext != 1 {
			if restart != nil || !errors.Is(err, ErrInvalidRoutingContext) {
				t.Fatalf("invalid Routing Context begin = (%v, %v)", restart, err)
			}
			return
		}
		if err != nil || restart == nil {
			t.Fatalf("valid begin = (%v, %v)", restart, err)
		}
		state := DestinationState(rawState)
		updateErr := restart.Update(destination, state)
		if validDestinationState(state) {
			if updateErr != nil {
				t.Fatalf("valid update: %v", updateErr)
			}
		} else if !errors.Is(updateErr, ErrInvalidParameterValue) {
			t.Fatalf("invalid state update = %v, want ErrInvalidParameterValue", updateErr)
		}
		if err := restart.Complete(); err != nil {
			t.Fatalf("complete: %v", err)
		}
		if err := restart.Complete(); err != nil {
			t.Fatalf("idempotent complete: %v", err)
		}
	})
}

func restartFixture(t *testing.T, routingContexts ...uint32) (*Listener, *applicationServer, *Association, *distributionCapture) {
	t.Helper()
	listener, applicationServer, asp, sent := distributionFixtureForContexts(
		t, params.TrafficModeLoadshare, routingContexts,
		func(config *AssociationConfig) { config.NetworkAppearance = params.NewNetworkAppearance(7) },
	)
	restartAttachConn(listener, asp)
	return listener, applicationServer, asp, sent
}

func restartAttachConn(listener *Listener, connection *Association) {
	connection.listener = listener
	connection.mtp3Restarts = &listener.mtp3Restarts
	connection.cfg.NetworkAppearance = listener.AssociationConfig.NetworkAppearance.Copy()
	if listener.destinations == nil {
		listener.destinations = newDestinations()
	}
	connection.destinations = listener.destinations
}

func restartActivateASP(applicationServer *applicationServer, asp *Association, routingContext uint32) {
	asp.noteRoutingContextsActive([]uint32{routingContext})
	asp.setState(StateASPActive)
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
}

func restartDestination(routingContext, pointCode uint32, mask uint8) AffectedDestination {
	return AffectedDestination{
		NetworkAppearance:    7,
		NetworkAppearanceSet: true,
		RoutingContext:       routingContext,
		RoutingContextSet:    true,
		PointCode:            pointCode,
		Mask:                 mask,
	}
}

func assertRestartScope(t *testing.T, message messages.M3UA, want AffectedDestination) {
	t.Helper()
	networkAppearance, routingContext, affectedPointCode := ssnmScope(t, message)
	if networkAppearance == nil || networkAppearance.NetworkAppearance() != want.NetworkAppearance {
		t.Fatalf("Network Appearance = %v, want %d", networkAppearance, want.NetworkAppearance)
	}
	if routingContext == nil || !reflect.DeepEqual(routingContext.RoutingContexts(), []uint32{want.RoutingContext}) {
		t.Fatalf("Routing Contexts = %v, want [%d]", routingContext, want.RoutingContext)
	}
	pointCodes := affectedPointCode.AffectedPointCodes()
	masks := affectedPointCode.AffectedPointCodeMasks()
	if !reflect.DeepEqual(pointCodes, []uint32{want.PointCode & 0x00ffffff}) || !reflect.DeepEqual(masks, []uint8{want.Mask}) {
		t.Fatalf("Affected Point Code = (%v, %v), want ([%#x], [%d])", pointCodes, masks, want.PointCode&0x00ffffff, want.Mask)
	}
}

func assertOnlySSNMKind(t *testing.T, written []messages.M3UA, kind any) {
	t.Helper()
	ssnm := ssnmMessages(written)
	if len(ssnm) != 1 {
		t.Fatalf("received %d SSNM messages, want 1: %v", len(ssnm), typeNames(ssnm))
	}
	if !sameSSNMKind(ssnm[0], kind) {
		t.Fatalf("SSNM message = %T, want %T", ssnm[0], kind)
	}
}
