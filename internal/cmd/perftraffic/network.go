package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
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
	routingContexts := make([]uint32, flowCount)
	for index := range routingContexts {
		routingContexts[index] = 100 + uint32(index)
	}
	config := m3ua.NewAssociationConfig(0, 0, 0, 0, 0, 0)
	config.SetSCTPNoDelay(sctpNoDelay).
		SetSCTPSACK(sctpSACKDelay, sctpSACKFrequency).
		SetTrafficModeType(params.TrafficModeLoadshare).
		SetNetworkAppearance(testNetworkAppearance).
		SetRoutingContexts(routingContexts...)
	config.HeartbeatInfo = &m3ua.HeartbeatInfo{Enabled: false}
	config.DataQueueSize = 1024
	return config
}

func runReceiver(ctx context.Context, config commandConfig) (runRecord, error) {
	control := newReceiverControl(config.Associations, maxOutstanding)
	control.cpuStatPath = config.CPUStatPath
	httpListener, err := net.Listen("tcp", config.ControlAddress)
	if err != nil {
		return runRecord{}, fmt.Errorf("listen for receiver control: %w", err)
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

	localAddress, err := sctp.ResolveSCTPAddr("sctp", config.SCTPAddress)
	if err != nil {
		return runRecord{}, fmt.Errorf("resolve SGP listen address: %w", err)
	}
	endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleSGP})
	if err != nil {
		return runRecord{}, fmt.Errorf("create SGP endpoint: %w", err)
	}
	defer func() { _ = endpoint.Close() }()
	listener, err := endpoint.Listen("m3ua", localAddress, m3ua.NewListenerConfig(associationConfig("sgp")))
	if err != nil {
		return runRecord{}, fmt.Errorf("listen for M3UA associations: %w", err)
	}
	defer func() { _ = listener.Close() }()

	fatal := make(chan error, 1)
	go acceptAndRead(ctx, listener, config.Associations, control, fatal)
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
	record.Manifest = currentManifest(config.Outstanding)
	if err != nil {
		return record, err
	}
	return record, nil
}

func acceptAndRead(ctx context.Context, listener *m3ua.Listener, associations int, control *receiverControl, fatal chan<- error) {
	for index := 0; index < associations; index++ {
		association, err := listener.Accept(ctx)
		if err != nil {
			nonblockingError(fatal, fmt.Errorf("accept association %d: %w", index, err))
			return
		}
		control.setAssociationReady(index, int(association.MaxMessageStreamID()))
		go readAssociation(ctx, index, association, control, fatal)
	}
}

func readAssociation(ctx context.Context, transportIndex int, association *m3ua.Association, control *receiverControl, fatal chan<- error) {
	for {
		message, err := association.ReadData()
		if err != nil {
			if ctx.Err() != nil || control.isStopped() && errors.Is(err, m3ua.ErrNotEstablished) {
				return
			}
			nonblockingError(fatal, fmt.Errorf("association %d ReadData: %w", transportIndex, err))
			return
		}
		control.record(transportIndex, receivedMessage{
			ProtocolData:         protocolDataFromM3UA(message.ProtocolData),
			NetworkAppearance:    message.NetworkAppearance,
			NetworkAppearanceSet: message.NetworkAppearanceSet,
			RoutingContext:       message.RoutingContext,
			RoutingContextSet:    message.RoutingContextSet,
		})
	}
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

func nonblockingError(destination chan<- error, err error) {
	select {
	case destination <- err:
	default:
	}
}
