package m3ua

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestWriteSignalDataEnforcesSGPPerApplicationServerState(t *testing.T) {
	listener, firstApplicationServer, asp, sent := distributionFixtureForContexts(
		t, params.TrafficModeLoadshare, []uint32{1, 2}, nil,
	)
	secondApplicationServer := listener.as.get(associationConfigASKey(listener.AssociationConfig, 2))
	asp.noteRoutingContextsActive([]uint32{1})
	asp.setState(StateASPActive)
	firstApplicationServer.setASPState(asp, StateASPActive, time.Hour)
	secondApplicationServer.setASPState(asp, StateASPInactive, time.Hour)
	sent.reset()

	if _, err := asp.WriteSignal(distributionData(2, 3, "inactive AS")); !errors.Is(err, ErrRoutingContextNotActive) {
		t.Fatalf("WriteSignal(DATA RC 2) error = %v, want %v", err, ErrRoutingContextNotActive)
	}
	if got := len(dataMessages(sent.snapshot())); got != 0 {
		t.Fatalf("WriteSignal sent %d DATA messages for an inactive AS, want 0", got)
	}

	if _, err := asp.WriteSignal(distributionData(1, 3, "active AS")); err != nil {
		t.Fatalf("WriteSignal(DATA RC 1): %v", err)
	}
	if got := len(dataMessages(sent.snapshot())); got != 1 {
		t.Fatalf("WriteSignal sent %d DATA messages for the active AS, want 1", got)
	}
}

func TestWriteSignalDataEnforcesASPAcknowledgedScope(t *testing.T) {
	asp, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	asp.cfg.NetworkAppearance = params.NewNetworkAppearance(0)
	asp.noteRoutingContextsAcked(params.NewRoutingContext(1))
	var writes atomic.Int32
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		writes.Add(1)
		return message.MarshalLen(), nil
	}

	if _, err := asp.WriteSignal(distributionData(2, 3, "unacknowledged AS")); !errors.Is(err, ErrRoutingContextNotActive) {
		t.Fatalf("WriteSignal(DATA RC 2) error = %v, want %v", err, ErrRoutingContextNotActive)
	}
	if got := writes.Load(); got != 0 {
		t.Fatalf("WriteSignal performed %d writes for an unacknowledged AS, want 0", got)
	}
}

func TestWriteSignalSSNMRejectsInactiveSGPScopeAtomically(t *testing.T) {
	listener, firstApplicationServer, asp, sent := distributionFixtureForContexts(
		t, params.TrafficModeLoadshare, []uint32{1, 2}, nil,
	)
	secondApplicationServer := listener.as.get(associationConfigASKey(listener.AssociationConfig, 2))
	asp.noteRoutingContextsActive([]uint32{1})
	asp.setState(StateASPActive)
	firstApplicationServer.setASPState(asp, StateASPActive, time.Hour)
	secondApplicationServer.setASPState(asp, StateASPInactive, time.Hour)
	sent.reset()

	unavailable := messages.NewDestinationUnavailable(
		nil,
		params.NewRoutingContext(1, 2),
		params.NewAffectedPointCodeWithMask(0, 1234),
		nil,
	)
	if _, err := asp.WriteSignal(unavailable); !errors.Is(err, ErrRoutingContextNotActive) {
		t.Fatalf("WriteSignal(DUNA RC 1,2) error = %v, want %v", err, ErrRoutingContextNotActive)
	}
	if got := len(sent.snapshot()); got != 0 {
		t.Fatalf("WriteSignal atomically wrote %d messages although one SSNM scope was inactive, want 0", got)
	}
}

func TestWriteSignalUnscopedSSNMRequiresAnActiveAssociation(t *testing.T) {
	for _, endpoint := range []struct {
		name string
		role Role
	}{
		{name: "SGP", role: RoleSGP},
		{name: "ASP", role: RoleASP},
	} {
		t.Run(endpoint.name, func(t *testing.T) {
			asp, _ := newTestConnWithContexts(t, StateASPDown, endpoint.role)
			var writes atomic.Int32
			asp.signalWriter = func(message messages.M3UA) (int, error) {
				writes.Add(1)
				return message.MarshalLen(), nil
			}

			_, err := asp.WriteSignal(messages.NewDestinationUnavailable(
				nil, nil, params.NewAffectedPointCode(0x123456), nil,
			))
			if !errors.Is(err, ErrNotEstablished) {
				t.Fatalf("unscoped DUNA in ASP-DOWN error = %v, want ErrNotEstablished", err)
			}
			if got := writes.Load(); got != 0 {
				t.Fatalf("unscoped DUNA performed %d writes in ASP-DOWN, want 0", got)
			}
		})
	}
}

func TestWriteSignalSSNMRejectsAnEmptyAuthorizedScope(t *testing.T) {
	asp, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
	asp.muAuthorizedRCs.Lock()
	asp.authorizationResolved = true
	asp.authorizedRCs = nil
	asp.muAuthorizedRCs.Unlock()
	var writes atomic.Int32
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		writes.Add(1)
		return message.MarshalLen(), nil
	}

	_, err := asp.WriteSignal(messages.NewDestinationUnavailable(
		nil, nil, params.NewAffectedPointCode(0x123456), nil,
	))
	if !errors.Is(err, ErrNoConfiguredAS) {
		t.Fatalf("DUNA for empty authorized scope error = %v, want ErrNoConfiguredAS", err)
	}
	if got := writes.Load(); got != 0 {
		t.Fatalf("DUNA for empty authorized scope performed %d writes, want 0", got)
	}
}

func TestWriteSignalRejectsUnconfiguredNetworkAppearance(t *testing.T) {
	listener, applicationServer, asp, sent := distributionFixture(t, params.TrafficModeLoadshare)
	asp.noteRoutingContextsActive([]uint32{1})
	asp.setState(StateASPActive)
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	sent.reset()

	for _, test := range []struct {
		name    string
		message messages.M3UA
	}{
		{
			name: "DATA",
			message: messages.NewData(
				params.NewNetworkAppearance(7), params.NewRoutingContext(1),
				params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 3, []byte("payload")), nil,
			),
		},
		{
			name: "SSNM",
			message: messages.NewDestinationUnavailable(
				params.NewNetworkAppearance(7), params.NewRoutingContext(1),
				params.NewAffectedPointCode(0x123456), nil,
			),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := asp.WriteSignal(test.message); !errors.Is(err, ErrInvalidNetworkAppearance) {
				t.Fatalf("WriteSignal(%s) error = %v, want ErrInvalidNetworkAppearance", test.name, err)
			}
			if got := len(sent.snapshot()); got != 0 {
				t.Fatalf("WriteSignal(%s) performed %d writes, want 0", test.name, got)
			}
		})
	}
	_ = listener
}

func TestDirectWriteSignalDataParticipatesInInactiveBarrier(t *testing.T) {
	listener, applicationServer, asp, _ := distributionFixture(t, params.TrafficModeLoadshare)
	asp.noteRoutingContextsActive([]uint32{1})
	asp.setState(StateASPActive)
	applicationServer.setASPState(asp, StateASPActive, time.Hour)

	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	var started atomic.Bool
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.Data); ok {
			if started.CompareAndSwap(false, true) {
				close(writeStarted)
			}
			<-releaseWrite
		}
		return message.MarshalLen(), nil
	}
	writeDone := make(chan error, 1)
	go func() {
		_, err := asp.WriteSignal(distributionData(1, 3, "in flight"))
		writeDone <- err
	}()
	select {
	case <-writeStarted:
	case <-time.After(time.Second):
		t.Fatal("direct DATA write did not start")
	}

	quiesced := make(chan func(), 1)
	go func() { quiesced <- listener.as.quiesceASPFor(asp, []uint32{1}) }()
	select {
	case <-quiesced:
		t.Fatal("ASP became quiescent before its in-flight direct DATA write completed")
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseWrite)
	if err := <-writeDone; err != nil {
		t.Fatalf("in-flight WriteSignal(DATA): %v", err)
	}
	var notify func()
	select {
	case notify = <-quiesced:
	case <-time.After(time.Second):
		t.Fatal("ASP did not quiesce after the direct DATA write completed")
	}
	notify()

	if _, err := asp.WriteSignal(distributionData(1, 3, "too late")); !errors.Is(err, ErrRoutingContextNotActive) {
		t.Fatalf("WriteSignal(DATA) after quiescence error = %v, want %v", err, ErrRoutingContextNotActive)
	}
}

func TestASPDownAckWaitsForEveryDirectDataWriteAPI(t *testing.T) {
	tests := []struct {
		name string
		call func(*Association) error
	}{
		{
			name: "WriteToStreamWithRoutingContext",
			call: func(connection *Association) error {
				_, err := connection.WriteToStreamWithRoutingContext([]byte("payload"), 1, 1)
				return err
			},
		},
		{
			name: "WritePDToStreamWithRoutingContext",
			call: func(connection *Association) error {
				_, err := connection.WritePDToStreamWithRoutingContext(
					params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 3, []byte("payload")),
					1,
					1,
				)
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			listener, applicationServer, asp, _ := distributionFixture(t, params.TrafficModeLoadshare)
			asp.noteRoutingContextsActive([]uint32{1})
			asp.setState(StateASPActive)
			asp.maxMessageStreamID = 4
			applicationServer.setASPState(asp, StateASPActive, time.Hour)

			writeStarted := make(chan struct{})
			releaseWrite := make(chan struct{})
			asp.dataWriter = func(data []byte, _ *sctp.SndRcvInfo) (int, error) {
				close(writeStarted)
				<-releaseWrite
				return len(data), nil
			}
			acknowledged := make(chan struct{})
			var acked atomic.Bool
			asp.signalWriter = func(message messages.M3UA) (int, error) {
				if _, ok := message.(*messages.AspDownAck); ok && acked.CompareAndSwap(false, true) {
					close(acknowledged)
				}
				return message.MarshalLen(), nil
			}

			writeDone := make(chan error, 1)
			go func() { writeDone <- test.call(asp) }()
			select {
			case <-writeStarted:
			case err := <-writeDone:
				t.Fatalf("direct DATA write returned before reaching SCTP: %v", err)
			case <-time.After(time.Second):
				t.Fatal("direct DATA write did not start")
			}

			downDone := make(chan error, 1)
			go func() { downDone <- asp.handleAspDown(messages.NewAspDown(nil)) }()
			select {
			case <-acknowledged:
				t.Fatal("ASP Down Ack overtook an in-flight direct DATA write")
			case <-time.After(30 * time.Millisecond):
			}
			close(releaseWrite)
			if err := <-writeDone; err != nil {
				t.Fatalf("direct DATA write: %v", err)
			}
			select {
			case <-acknowledged:
			case <-time.After(time.Second):
				t.Fatal("ASP Down Ack was not written after DATA completed")
			}
			if err := <-downDone; err != nil {
				t.Fatalf("handleAspDown: %v", err)
			}
			if got := listener.ApplicationServerState(1); got == ASActive {
				t.Fatal("Application Server remained active after ASP Down")
			}
		})
	}
}

func TestASPDownAckWaitsForUnscopedDirectData(t *testing.T) {
	asp, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP)
	asp.as = newApplicationServers(time.Hour)
	asp.maxMessageStreamID = 4
	asp.recvStream.Store(0)
	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	asp.dataWriter = func(data []byte, _ *sctp.SndRcvInfo) (int, error) {
		close(writeStarted)
		<-releaseWrite
		return len(data), nil
	}
	acknowledged := make(chan struct{})
	var acked atomic.Bool
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.AspDownAck); ok && acked.CompareAndSwap(false, true) {
			close(acknowledged)
		}
		return message.MarshalLen(), nil
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := asp.WriteToStream([]byte("unscoped"), 1)
		writeDone <- err
	}()
	select {
	case <-writeStarted:
	case err := <-writeDone:
		t.Fatalf("unscoped DATA returned before SCTP: %v", err)
	case <-time.After(time.Second):
		t.Fatal("unscoped DATA did not start")
	}
	downDone := make(chan error, 1)
	go func() { downDone <- asp.handleAspDown(messages.NewAspDown(nil)) }()
	select {
	case <-acknowledged:
		t.Fatal("ASP Down Ack overtook unscoped DATA")
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseWrite)
	if err := <-writeDone; err != nil {
		t.Fatalf("unscoped DATA: %v", err)
	}
	select {
	case <-acknowledged:
	case <-time.After(time.Second):
		t.Fatal("ASP Down Ack was not written after unscoped DATA completed")
	}
	if err := <-downDone; err != nil {
		t.Fatalf("handleAspDown: %v", err)
	}
}

func TestASPDownAckWaitsForUnscopedSSNM(t *testing.T) {
	asp, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP)
	asp.as = newApplicationServers(time.Hour)
	asp.recvStream.Store(0)
	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	acknowledged := make(chan struct{})
	var ssnmStarted atomic.Bool
	var acked atomic.Bool
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		switch message.(type) {
		case *messages.DestinationUnavailable:
			if ssnmStarted.CompareAndSwap(false, true) {
				close(writeStarted)
			}
			<-releaseWrite
		case *messages.AspDownAck:
			if acked.CompareAndSwap(false, true) {
				close(acknowledged)
			}
		}
		return message.MarshalLen(), nil
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := asp.WriteSignal(messages.NewDestinationUnavailable(
			nil, nil, params.NewAffectedPointCode(0x123456), nil,
		))
		writeDone <- err
	}()
	select {
	case <-writeStarted:
	case <-time.After(time.Second):
		t.Fatal("unscoped SSNM write did not start")
	}
	downDone := make(chan error, 1)
	go func() { downDone <- asp.handleAspDown(messages.NewAspDown(nil)) }()
	select {
	case <-acknowledged:
		t.Fatal("ASP Down Ack overtook unscoped SSNM")
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseWrite)
	if err := <-writeDone; err != nil {
		t.Fatalf("unscoped SSNM: %v", err)
	}
	select {
	case <-acknowledged:
	case <-time.After(time.Second):
		t.Fatal("ASP Down Ack was not written after unscoped SSNM completed")
	}
	if err := <-downDone; err != nil {
		t.Fatalf("handleAspDown: %v", err)
	}
}

func TestASPInactiveAckWaitsForUnscopedDirectData(t *testing.T) {
	asp, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP)
	asp.maxMessageStreamID = 4
	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	asp.dataWriter = func(data []byte, _ *sctp.SndRcvInfo) (int, error) {
		close(writeStarted)
		<-releaseWrite
		return len(data), nil
	}
	acknowledged := make(chan struct{})
	var acked atomic.Bool
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.AspInactiveAck); ok && acked.CompareAndSwap(false, true) {
			close(acknowledged)
		}
		return message.MarshalLen(), nil
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := asp.WriteToStream([]byte("unscoped"), 1)
		writeDone <- err
	}()
	select {
	case <-writeStarted:
	case err := <-writeDone:
		t.Fatalf("unscoped DATA returned before SCTP: %v", err)
	case <-time.After(time.Second):
		t.Fatal("unscoped DATA did not start")
	}
	// The write has already resolved and locked the dedicated-association path.
	// Give the directly constructed Association a static AS so ASP Inactive can name
	// the Ack scope without changing the in-flight write's resolved scope.
	asp.cfg.RoutingContexts = params.NewRoutingContext(1)
	inactiveDone := make(chan error, 1)
	go func() { inactiveDone <- asp.handleAspInactive(messages.NewAspInactive(nil, nil)) }()
	select {
	case <-acknowledged:
		t.Fatal("ASP Inactive Ack overtook unscoped DATA")
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseWrite)
	if err := <-writeDone; err != nil {
		t.Fatalf("unscoped DATA: %v", err)
	}
	select {
	case <-acknowledged:
	case <-time.After(time.Second):
		t.Fatal("ASP Inactive Ack was not written after unscoped DATA completed")
	}
	if err := <-inactiveDone; err != nil {
		t.Fatalf("handleAspInactive: %v", err)
	}
}

func TestASPInactiveAckWaitsForUnscopedSSNM(t *testing.T) {
	asp, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP)
	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	acknowledged := make(chan struct{})
	var ssnmStarted atomic.Bool
	var acked atomic.Bool
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		switch message.(type) {
		case *messages.DestinationUnavailable:
			if ssnmStarted.CompareAndSwap(false, true) {
				close(writeStarted)
			}
			<-releaseWrite
		case *messages.AspInactiveAck:
			if acked.CompareAndSwap(false, true) {
				close(acknowledged)
			}
		}
		return message.MarshalLen(), nil
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := asp.WriteSignal(messages.NewDestinationUnavailable(
			nil, nil, params.NewAffectedPointCode(0x123456), nil,
		))
		writeDone <- err
	}()
	select {
	case <-writeStarted:
	case <-time.After(time.Second):
		t.Fatal("unscoped SSNM write did not start")
	}
	asp.cfg.RoutingContexts = params.NewRoutingContext(1)
	inactiveDone := make(chan error, 1)
	go func() { inactiveDone <- asp.handleAspInactive(messages.NewAspInactive(nil, nil)) }()
	select {
	case <-acknowledged:
		t.Fatal("ASP Inactive Ack overtook unscoped SSNM")
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseWrite)
	if err := <-writeDone; err != nil {
		t.Fatalf("unscoped SSNM: %v", err)
	}
	select {
	case <-acknowledged:
	case <-time.After(time.Second):
		t.Fatal("ASP Inactive Ack was not written after unscoped SSNM completed")
	}
	if err := <-inactiveDone; err != nil {
		t.Fatalf("handleAspInactive: %v", err)
	}
}

func dataMessages(messagesOnWire []messages.M3UA) []*messages.Data {
	data := make([]*messages.Data, 0, len(messagesOnWire))
	for _, message := range messagesOnWire {
		if payload, ok := message.(*messages.Data); ok {
			data = append(data, payload)
		}
	}
	return data
}
