package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

const (
	sctpNoDelay       = true
	sctpSACKDelay     = uint32(0)
	sctpSACKFrequency = uint32(1)
)

func associationConfig(role string) *m3ua.AssociationConfig {
	applicationServers := make([]m3ua.ASConfig, flowCount)
	for index := range applicationServers {
		applicationServers[index] = m3ua.ASConfig{
			ASKey: m3ua.ASKey{
				NetworkAppearance:    testNetworkAppearance,
				NetworkAppearanceSet: true,
				RoutingContext:       100 + uint32(index),
				RoutingContextSet:    true,
			},
			TrafficMode: params.TrafficModeLoadshare,
		}
	}
	config := m3ua.NewAssociationConfig()
	config.SetSCTPNoDelay(sctpNoDelay).
		SetSCTPSACK(sctpSACKDelay, sctpSACKFrequency).
		SetApplicationServers(applicationServers...)
	config.HeartbeatInfo = &m3ua.HeartbeatInfo{Enabled: false}
	config.DataQueueSize = 1024
	return config
}

// newRunReceiverControl builds the SGP receiver's control from its own
// configuration. Every field the run depends on is set here, in one place:
// notably reverseControl, the single control endpoint a bidirectional run may
// drive, which must come from this process's configuration and never from a
// control request.
func newRunReceiverControl(ctx context.Context, config commandConfig) *receiverControl {
	control := newReceiverControl(config.Associations, maxOutstanding)
	control.cpuStatPath = config.CPUStatPath
	control.driver = &reverseDriver{ctx: ctx, cpuStatPath: config.CPUStatPath}
	control.reverseControl = config.PeerControl
	return control
}

func runReceiver(ctx context.Context, config commandConfig) (runRecord, error) {
	control := newRunReceiverControl(ctx, config)
	httpListener, err := net.Listen("tcp", config.ControlAddress)
	if err != nil {
		return runRecord{}, fmt.Errorf("startup control-bind: %w", err)
	}
	defer func() { _ = httpListener.Close() }()
	httpServer := &http.Server{Handler: control.handler(), ReadHeaderTimeout: 5 * time.Second}
	httpFailure := make(chan error, 1)
	go func() {
		serveErr := httpServer.Serve(httpListener)
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			httpFailure <- serveErr
		}
	}()

	endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleSGP})
	if err != nil {
		return runRecord{}, fmt.Errorf("startup endpoint: %w", err)
	}
	defer func() { _ = endpoint.Close() }()

	fatal := make(chan error, 1)
	if config.Transport == "dial" {
		if err := dialAndRead(ctx, endpoint, config, control, fatal); err != nil {
			return runRecord{}, fmt.Errorf("startup dial: %w", err)
		}
	} else {
		localAddress, err := sctp.ResolveSCTPAddr("sctp", config.SCTPAddress)
		if err != nil {
			return runRecord{}, fmt.Errorf("startup resolve-listen-address: %w", err)
		}
		listener, err := endpoint.Listen("m3ua", localAddress, m3ua.NewListenerConfig(associationConfig("sgp")))
		if err != nil {
			return runRecord{}, fmt.Errorf("startup listen: %w", err)
		}
		defer func() { _ = listener.Close() }()
		go acceptAndRead(ctx, listener, config.Associations, control, fatal)
	}
	go sampleReceiver(ctx, control)
	select {
	case <-ctx.Done():
	case err = <-fatal:
		control.setFatal(err.Error())
	case err = <-httpFailure:
		control.setFatal("HTTP control server: " + err.Error())
	}
	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelShutdown()
	_ = httpServer.Shutdown(shutdownContext)
	record := control.result()
	record.Manifest = currentManifest(config.Outstanding, config.Initiation)
	if err != nil {
		return record, err
	}
	return record, nil
}

// dialAndRead initiates every association from the SGP side before the
// receiver reports ready, mirroring the accept path with the SCTP initiation
// direction reversed. A refused dial is retried within a bounded window
// while the ASP listener comes up; the library's Dial then waits for the
// association to reach AS-ACTIVE, and requireStateActive verifies that
// readiness contract explicitly before the association serves readiness or
// ReadData.
func dialAndRead(ctx context.Context, endpoint *m3ua.Endpoint, config commandConfig, control *receiverControl, fatal chan<- error) error {
	remoteAddress, err := sctp.ResolveSCTPAddr("sctp", config.SCTPAddress)
	if err != nil {
		return fmt.Errorf("resolve ASP address: %w", err)
	}
	var localAddress *sctp.SCTPAddr
	if config.LocalAddress != "" {
		localAddress, err = sctp.ResolveSCTPAddr("sctp", config.LocalAddress)
		if err != nil {
			return fmt.Errorf("resolve SGP local address: %w", err)
		}
	}
	for index := 0; index < config.Associations; index++ {
		association, err := dialWithRetry(ctx, dialRetryWindow, dialRetryInterval, func() (*m3ua.Association, error) {
			return endpoint.Dial(ctx, "m3ua", localAddress, remoteAddress, associationConfig("sgp"))
		})
		if err != nil {
			return fmt.Errorf("dial association %d: %w", index, err)
		}
		if err := requireStateActive(association.State()); err != nil {
			_ = association.Close()
			return fmt.Errorf("dial association %d: %w", index, err)
		}
		control.registerReverseAssociation(association)
		control.setAssociationReady(index, int(association.MaxMessageStreamID()))
		go readAssociation(ctx, index, association, control, fatal)
	}
	return nil
}

func acceptAndRead(ctx context.Context, listener *m3ua.Listener, associations int, control *receiverControl, fatal chan<- error) {
	for index := 0; index < associations; index++ {
		association, err := listener.Accept(ctx)
		if err != nil {
			nonblockingError(fatal, fmt.Errorf("accept association %d: %w", index, err))
			return
		}
		control.registerReverseAssociation(association)
		control.setAssociationReady(index, int(association.MaxMessageStreamID()))
		go readAssociation(ctx, index, association, control, fatal)
	}
}

func readAssociation(ctx context.Context, transportIndex int, association *m3ua.Association, control *receiverControl, fatal chan<- error) {
	// The echo reply writer is created lazily on the first echo request so
	// non-echo cohorts pay nothing. Its bounded queue is the entire coupling
	// between the read loop and reply-write backpressure: the read loop never
	// writes to the association itself.
	var replyQueue chan<- echoReplyJob
	defer func() {
		if replyQueue != nil {
			close(replyQueue)
		}
	}()
	for {
		message, err := association.ReadData(ctx)
		if err != nil {
			if ctx.Err() != nil || control.isStopped() && errors.Is(err, m3ua.ErrNotEstablished) {
				return
			}
			// One teardown closes every association, so all read loops fail
			// together and only the chan winner reaches the record. Emit one
			// diagnostic line per failure so the FIRST cause stays visible in
			// process output even when the record carries a cascade sibling.
			phase := control.phaseName()
			writeStartupDiagnostic("read-fatal", transportIndex, phase, err)
			nonblockingError(fatal, readFatalError(transportIndex, phase, err))
			return
		}
		received, outcome := control.record(transportIndex, receivedMessage{
			ProtocolData:         protocolDataFromM3UA(message.ProtocolData),
			NetworkAppearance:    message.Scope.NetworkAppearance,
			NetworkAppearanceSet: message.Scope.NetworkAppearanceSet,
			RoutingContext:       firstRoutingContext(message.Scope),
			RoutingContextSet:    message.Scope.RoutingContextSet,
		})
		if outcome != recordUnique || !control.echoMode() {
			continue
		}
		if replyQueue == nil {
			replyQueue = startEchoReplyWriter(association, control, maxOutstanding/control.expectedAssociations)
		}
		offerEchoReply(replyQueue, received.replyJob(len(message.ProtocolData.Data)), control)
	}
}

// writeEchoReply answers one validated echo request with a same-size,
// deterministic reply carrying the request identity with reversed point
// codes. The reply is RTT evidence for the sender only; it is not a useful
// delivery in the offered-load count.
func writeEchoReply(association *m3ua.Association, identity messageIdentity, size int) error {
	identity.Kind = kindEchoReply
	payload := buildPayload(identity, size)
	tuple := reverseTuple(tupleFor(identity.Flow, identity.Association))
	written, err := association.WriteData(tuple.dataRequest(payload))
	if err != nil {
		return err
	}
	if written != size {
		return fmt.Errorf("echo reply wrote %d bytes, want %d", written, size)
	}
	return nil
}

// firstRoutingContext is the single Routing Context a DATA may name. RFC 4666
// Section 3.3.1 defines exactly one for DATA, and the library refuses any other
// count before delivery.
func firstRoutingContext(scope m3ua.WireScope) uint32 {
	if len(scope.RoutingContexts) == 0 {
		return 0
	}
	return scope.RoutingContexts[0]
}

func protocolDataFromM3UA(payload *params.ProtocolDataPayload) protocolData {
	if payload == nil {
		return protocolData{}
	}
	return protocolData{
		OriginatingPointCode:    payload.OriginatingPointCode,
		DestinationPointCode:    payload.DestinationPointCode,
		ServiceIndicator:        payload.ServiceIndicator,
		NetworkIndicator:        payload.NetworkIndicator,
		MessagePriority:         payload.MessagePriority,
		SignallingLinkSelection: payload.SignallingLinkSelection,
		Data:                    payload.Data,
	}
}

func (control *receiverControl) isStopped() bool {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	return control.phase == receiverStopped
}

// phaseName names the receiver phase for diagnostics, distinguishing a
// startup read failure (idle/armed, before any cohort) from a mid-cohort
// one.
func (control *receiverControl) phaseName() string {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	return string(control.phase)
}

// readFatalError names the association, the receiver phase and the cause so
// the record distinguishes a startup failure (idle/armed phase) from a
// mid-cohort one.
func readFatalError(transportIndex int, phase string, err error) error {
	return fmt.Errorf("association %d ReadData in receiver phase %s: %w", transportIndex, phase, err)
}

// startupDiagnosticWriter receives one structured line per startup-relevant
// failure. It is a variable so tests can capture it; production writes to
// stderr because a process that dies during startup may have no cohort
// record worth reading.
var startupDiagnosticWriter io.Writer = os.Stderr

// writeStartupDiagnostic emits one JSON line per startup-relevant failure.
// The record's fatal_error keeps only the first failure reported to the
// control endpoint; these lines preserve every failure in order, which is
// what separates a first cause from a teardown cascade.
func writeStartupDiagnostic(event string, association int, phase string, err error) {
	_ = json.NewEncoder(startupDiagnosticWriter).Encode(map[string]any{
		"startup_diagnostic": event,
		"association":        association,
		"receiver_phase":     phase,
		"error":              err.Error(),
	})
}

func nonblockingError(destination chan<- error, err error) {
	select {
	case destination <- err:
	default:
	}
}
