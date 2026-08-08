// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"fmt"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

const (
	initialRecoveryDrainRetry = 10 * time.Millisecond
	maximumRecoveryDrainRetry = time.Second
)

// TrafficDistribution reports what DistributeData did with one MTP3 message.
// Queued and Delivered are mutually exclusive for a successful call.
type TrafficDistribution struct {
	Delivered int
	Queued    bool
}

type distributionPolicy struct {
	messageLimit                 int
	byteLimit                    int
	broadcastFlowCacheEntries    int
	broadcastFlowIdentifierBytes int
	routingContexts              []uint32
	networkAppearance            *params.Param
	broadcastFlowIdentifier      BroadcastFlowIdentifier
}

// broadcastFlowKey uses the MTP3 routing label plus the M3UA network and AS
// selectors. RFC 4666 does not define one exhaustive flow classifier, but it
// identifies Routing Context as the DATA traffic flow and uses SLS for ordered
// distribution; including the remaining routing-label fields prevents two
// independent routes that happen to share an SLS from suppressing each other's
// mandatory Broadcast synchronization marker.
type broadcastFlowKey struct {
	networkAppearance    uint32
	networkAppearanceSet bool
	routingContext       uint32
	originatingPointCode uint32
	destinationPointCode uint32
	serviceIndicator     uint8
	networkIndicator     uint8
	signalingLink        uint8
	applicationFlow      string
}

// DistributeData sends one DATA message to the ASPs serving its Application
// Server. It applies Override, Loadshare, and Broadcast traffic modes, retains
// traffic while the AS is AS-PENDING, and adds the Broadcast synchronization
// marker RFC 4666 Section 4.3.4.3 requires after an ASP becomes active.
//
// The message is copied before the call returns. The caller may reuse or mutate
// it immediately, including when the result says it was queued.
func (l *Listener) DistributeData(data *messages.Data) (TrafficDistribution, error) {
	if l == nil {
		return TrafficDistribution{}, errors.New("cannot distribute DATA through a nil Listener")
	}

	l.muConns.Lock()
	if l.closed {
		l.muConns.Unlock()
		return TrafficDistribution{}, ErrConnClosed
	}
	registry := l.as
	l.muConns.Unlock()
	if registry == nil {
		return TrafficDistribution{}, ErrNoActiveASP
	}
	policy := registry.distribution
	owned, protocolData, encodedSize, key, err := l.prepareDistributionData(registry, policy, data)
	if err != nil {
		return TrafficDistribution{}, err
	}
	applicationServer, ok := registry.lookup(key)
	if !ok {
		return TrafficDistribution{}, ErrNoActiveASP
	}
	flow := dataBroadcastFlowKey(owned, protocolData)
	if applicationServer.TrafficMode() == params.TrafficModeBroadcast {
		flow, err = classifyBroadcastFlow(policy, owned, protocolData)
		if err != nil {
			return TrafficDistribution{}, err
		}
	}

	return applicationServer.distribute(owned, protocolData, flow, encodedSize, policy.messageLimit, policy.byteLimit)
}

func (l *Listener) prepareDistributionData(registry *applicationServers, policy distributionPolicy, data *messages.Data) (*messages.Data, *params.ProtocolDataPayload, int, ASKey, error) {
	if data == nil {
		return nil, nil, 0, ASKey{}, errors.New("cannot distribute nil DATA")
	}
	if data.Header == nil {
		return nil, nil, 0, ASKey{}, errors.New("cannot distribute DATA without a common header")
	}
	if data.Version() != 1 {
		return nil, nil, 0, ASKey{}, NewInvalidVersionError(data.Version())
	}
	if data.Reserved != 0 {
		return nil, nil, 0, ASKey{}, fmt.Errorf("%w: DATA reserved byte is %#x", ErrInvalidParameterValue, data.Reserved)
	}
	if data.Class != messages.MsgClassTransfer || data.Type != messages.MsgTypePayloadData {
		return nil, nil, 0, ASKey{}, fmt.Errorf("%w: DATA header has class %d type %d", messages.ErrUnexpectedMessageType, data.Class, data.Type)
	}

	owned := copyData(data)
	if owned.ProtocolData == nil {
		return nil, nil, 0, ASKey{}, ErrMissingProtocolData
	}
	if owned.ProtocolData.Tag != params.ProtocolData {
		return nil, nil, 0, ASKey{}, fmt.Errorf("invalid DATA Protocol Data: %w", params.ErrInvalidType)
	}

	key, err := resolveDistributionRoutingContext(registry, policy, owned)
	if err != nil {
		return nil, nil, 0, ASKey{}, err
	}
	if err := validateDistributionNetworkAppearance(policy, owned.NetworkAppearance); err != nil {
		return nil, nil, 0, ASKey{}, err
	}

	raw, err := owned.MarshalBinary()
	if err != nil {
		if errors.Is(err, messages.ErrMissingParameter) {
			return nil, nil, 0, ASKey{}, ErrMissingProtocolData
		}
		return nil, nil, 0, ASKey{}, fmt.Errorf("invalid DATA: %w", err)
	}
	validated, err := messages.ParseData(raw)
	if err != nil {
		return nil, nil, 0, ASKey{}, fmt.Errorf("invalid DATA: %w", err)
	}
	validatedProtocolData, err := validated.ProtocolData.ProtocolData()
	if err != nil {
		return nil, nil, 0, ASKey{}, fmt.Errorf("invalid DATA Protocol Data: %w", err)
	}
	return validated, validatedProtocolData, len(raw), key, nil
}

func resolveDistributionRoutingContext(registry *applicationServers, policy distributionPolicy, data *messages.Data) (ASKey, error) {
	configured := policy.routingContexts

	if data.RoutingContext == nil {
		switch len(configured) {
		case 0:
			if key, _, ok := registry.sole(); ok {
				if key.RoutingContextSet {
					data.RoutingContext = params.NewRoutingContext(key.RoutingContext)
				}
				return key, nil
			}
			return ASKey{}, ErrNoActiveASP
		case 1:
			data.RoutingContext = params.NewRoutingContext(configured[0])
			return asKeyFromDistributionScope(policy, data.NetworkAppearance, configured[0]), nil
		default:
			return ASKey{}, ErrMissingRoutingContext
		}
	}
	if data.RoutingContext.Tag != params.RoutingContext {
		return ASKey{}, fmt.Errorf("invalid DATA Routing Context: %w", params.ErrInvalidType)
	}
	if len(data.RoutingContext.Data) != 4 {
		return ASKey{}, fmt.Errorf("invalid DATA Routing Context: %w", params.ErrInvalidLength)
	}
	routingContexts := data.RoutingContext.RoutingContexts()
	rtCtx := routingContexts[0]
	key := asKeyFromDistributionScope(policy, data.NetworkAppearance, rtCtx)
	if len(configured) > 0 {
		for _, candidate := range configured {
			if candidate == rtCtx {
				return key, nil
			}
		}
		return ASKey{}, NewInvalidRoutingContextError(rtCtx)
	}
	if _, ok := registry.lookup(key); !ok {
		return ASKey{}, NewInvalidRoutingContextError(rtCtx)
	}
	return key, nil
}

func validateDistributionNetworkAppearance(policy distributionPolicy, networkAppearance *params.Param) error {
	if networkAppearance == nil {
		return nil
	}
	if networkAppearance.Tag != params.NetworkAppearance {
		return fmt.Errorf("invalid DATA Network Appearance: %w", params.ErrInvalidType)
	}
	if len(networkAppearance.Data) != 4 {
		return fmt.Errorf("invalid DATA Network Appearance: %w", params.ErrInvalidLength)
	}
	if policy.networkAppearance == nil {
		return nil
	}
	configured := policy.networkAppearance
	if configured.Tag != params.NetworkAppearance || len(configured.Data) != 4 ||
		configured.NetworkAppearance() != networkAppearance.NetworkAppearance() {
		return NewInvalidNetworkAppearanceError(networkAppearance.NetworkAppearance())
	}
	return nil
}

func asKeyFromDistributionScope(policy distributionPolicy, networkAppearance *params.Param, routingContext uint32) ASKey {
	key := routingContextASKey(routingContext)
	if networkAppearance != nil {
		key.NetworkAppearance, key.NetworkAppearanceSet = appearanceOf(networkAppearance)
		return key
	}
	key.NetworkAppearance, key.NetworkAppearanceSet = appearanceOf(policy.networkAppearance)
	return key
}

func newDistributionPolicy(config *Config) distributionPolicy {
	policy := distributionPolicy{
		messageLimit:                 DefaultRecoveryQueueMessages,
		byteLimit:                    DefaultRecoveryQueueBytes,
		broadcastFlowCacheEntries:    DefaultBroadcastFlowCacheEntries,
		broadcastFlowIdentifierBytes: DefaultBroadcastFlowIdentifierBytes,
	}
	if config == nil {
		return policy
	}
	if config.RecoveryQueueMessages > 0 {
		policy.messageLimit = config.RecoveryQueueMessages
	}
	if config.RecoveryQueueBytes > 0 {
		policy.byteLimit = config.RecoveryQueueBytes
	}
	if config.BroadcastFlowCacheEntries > 0 {
		policy.broadcastFlowCacheEntries = config.BroadcastFlowCacheEntries
	}
	if config.BroadcastFlowIdentifierBytes > 0 {
		policy.broadcastFlowIdentifierBytes = config.BroadcastFlowIdentifierBytes
	}
	if config.RoutingContexts != nil {
		policy.routingContexts = append([]uint32(nil), config.RoutingContexts.RoutingContexts()...)
	}
	policy.networkAppearance = config.NetworkAppearance.Copy()
	policy.broadcastFlowIdentifier = config.BroadcastFlowIdentifier
	return policy
}

func copyData(data *messages.Data) *messages.Data {
	copy := messages.NewData(
		data.NetworkAppearance.Copy(),
		data.RoutingContext.Copy(),
		data.ProtocolData.Copy(),
		data.CorrelationID.Copy(),
	)
	copy.Others = make([]*params.Param, 0, len(data.Others))
	for _, parameter := range data.Others {
		copy.Others = append(copy.Others, parameter.Copy())
	}
	copy.SetLength()
	return copy
}

func (as *applicationServer) distribute(data *messages.Data, protocolData *params.ProtocolDataPayload, flow broadcastFlowKey, encodedSize, messageLimit, byteLimit int) (TrafficDistribution, error) {
	// Queuing needs only the state lock. In particular, do not wait for
	// deliveryMu while a recovery write is blocked: draining is precisely the
	// condition under which later DATA must be accepted behind that write.
	as.mu.Lock()
	if as.closed {
		as.mu.Unlock()
		return TrafficDistribution{}, ErrConnClosed
	}
	if as.state == ASPending || as.draining || as.activeSending || (as.state == ASActive && len(as.recoveryQueue) > 0) {
		startDrain, err := as.enqueueRecoveryLocked(data, flow, encodedSize, messageLimit, byteLimit)
		as.mu.Unlock()
		if err != nil {
			return TrafficDistribution{}, err
		}
		if startDrain {
			go as.drainRecoveryQueue()
		}
		return TrafficDistribution{Queued: true}, nil
	}
	if as.state != ASActive {
		as.mu.Unlock()
		return TrafficDistribution{}, ErrNoActiveASP
	}
	if encodedSize > byteLimit || !as.recoveryBudget.claim(encodedSize) {
		as.mu.Unlock()
		return TrafficDistribution{}, ErrRecoveryQueueFull
	}
	as.activeSending = true
	as.deliveryInFlightBytes = encodedSize
	as.recoveryQueueBytes += encodedSize
	as.mu.Unlock()

	// This caller owns the active-send slot before waiting for deliveryMu, so every
	// later caller either joins the bounded FIFO or is refused immediately.
	as.deliveryMu.Lock()
	as.mu.Lock()
	if as.closed {
		as.finishDeliveryItemLocked(encodedSize)
		as.activeSending = false
		as.mu.Unlock()
		as.deliveryMu.Unlock()
		return TrafficDistribution{}, ErrConnClosed
	}
	if as.state == ASPending || as.draining || (as.state == ASActive && len(as.recoveryQueue) > 0) {
		as.activeSending = false
		as.deliveryInFlightBytes = 0
		as.recoveryQueue = append([]queuedData{{
			data:        data,
			flow:        flow,
			size:        encodedSize,
			recoveryGen: as.queuedDataRecoveryGenerationLocked(),
		}}, as.recoveryQueue...)
		startDrain := as.state == ASActive && !as.draining
		if startDrain {
			as.stopDrainRetryLocked()
			as.draining = true
		}
		as.mu.Unlock()
		as.deliveryMu.Unlock()
		if startDrain {
			go as.drainRecoveryQueue()
		}
		return TrafficDistribution{Queued: true}, nil
	}
	if as.state != ASActive {
		as.finishDeliveryItemLocked(encodedSize)
		as.activeSending = false
		as.mu.Unlock()
		as.deliveryMu.Unlock()
		return TrafficDistribution{}, ErrNoActiveASP
	}
	as.mu.Unlock()
	result, err := as.deliverLocked(data, protocolData, flow)
	as.mu.Lock()
	as.finishDeliveryItemLocked(encodedSize)
	as.activeSending = false
	startDrain := as.state == ASActive && len(as.recoveryQueue) > 0 && !as.draining
	if startDrain {
		as.stopDrainRetryLocked()
		as.draining = true
	}
	as.mu.Unlock()
	as.deliveryMu.Unlock()
	if startDrain {
		go as.drainRecoveryQueue()
	}
	return result, err
}

// enqueueRecoveryLocked appends atomically against both configured limits and
// reports whether the caller must start the drain. It requires mu.
func (as *applicationServer) enqueueRecoveryLocked(data *messages.Data, flow broadcastFlowKey, encodedSize, messageLimit, byteLimit int) (bool, error) {
	retainedMessages := len(as.recoveryQueue)
	if as.deliveryInFlightBytes > 0 {
		retainedMessages++
	}
	if retainedMessages >= messageLimit || encodedSize > byteLimit-as.recoveryQueueBytes {
		return false, ErrRecoveryQueueFull
	}
	if !as.recoveryBudget.claim(encodedSize) {
		return false, ErrRecoveryQueueFull
	}
	as.recoveryQueue = append(as.recoveryQueue, queuedData{
		data:        data,
		flow:        flow,
		size:        encodedSize,
		recoveryGen: as.queuedDataRecoveryGenerationLocked(),
	})
	as.recoveryQueueBytes += encodedSize
	startDrain := as.state == ASActive && !as.draining && !as.activeSending
	if startDrain {
		as.stopDrainRetryLocked()
		as.draining = true
	}
	return startDrain, nil
}

// queuedDataRecoveryGenerationLocked assigns active FIFO traffic a generation
// newer than the last expired T(r) epoch. startRecoveryLocked restamps queued
// traffic when the AS later enters AS-PENDING; an item already on the wire
// keeps this earlier fresh generation so a failure before expiry is retained
// and a failure after expiry is discarded. It requires mu.
func (as *applicationServer) queuedDataRecoveryGenerationLocked() uint64 {
	if as.state == ASActive && as.recoveryGen <= as.recoveryExpiredGen {
		as.recoveryGen++
	}
	return as.recoveryGen
}

// deliverLocked requires deliveryMu. It snapshots targets and Broadcast state
// under mu, performs no socket writes under mu, then commits the flow marker
// only if every selected copy was accepted for delivery. ASP state may change
// during a write; the next message observes the new membership and epoch.
func (as *applicationServer) deliverLocked(data *messages.Data, protocolData *params.ProtocolDataPayload, flow broadcastFlowKey) (TrafficDistribution, error) {
	result, _, _, _, _, err := as.deliverAttemptLocked(data, protocolData, flow, nil, false, 0, false)
	return result, err
}

// deliverAttemptLocked retains enough state for a queued Broadcast retry to
// address only recipients whose earlier copy failed. Re-sending to every ASP
// would duplicate an MSU at recipients that already accepted it; dropping the
// item after one success silently loses it at the failed recipient.
func (as *applicationServer) deliverAttemptLocked(
	data *messages.Data,
	protocolData *params.ProtocolDataPayload,
	flow broadcastFlowKey,
	broadcastTargets []*Conn,
	broadcastTargetsSet bool,
	epoch uint64,
	tagged bool,
) (TrafficDistribution, []*Conn, bool, uint64, bool, error) {
	as.mu.Lock()
	broadcast := as.trafficMode == params.TrafficModeBroadcast
	var (
		targets []*Conn
		err     error
	)
	if broadcastTargetsSet {
		targets = make([]*Conn, 0, len(broadcastTargets))
		for _, target := range broadcastTargets {
			if as.asps[target] == StateAspActive {
				targets = append(targets, target)
			}
		}
	} else {
		targets, err = as.targetsLocked(protocolData.SignalingLinkSelection)
		if err == nil {
			epoch = as.broadcastEpoch
			if broadcast && epoch != 0 && as.broadcastTagged[flow] != epoch {
				data.CorrelationID = params.NewCorrelationID(as.allocateCorrelationIDLocked())
				data.SetLength()
				tagged = true
			}
		}
	}
	as.mu.Unlock()
	if err != nil {
		return TrafficDistribution{}, nil, broadcastTargetsSet, epoch, tagged, err
	}

	delivered := 0
	failed := make([]*Conn, 0, len(targets))
	var deliveryErrors []error
	for _, target := range targets {
		message := copyData(data)
		if _, err := target.writeDistributedSignal(message); err != nil {
			failed = append(failed, target)
			deliveryErrors = append(deliveryErrors, err)
			continue
		}
		delivered++
	}

	if tagged && len(failed) == 0 {
		as.mu.Lock()
		if as.broadcastEpoch == epoch {
			if as.broadcastTagged == nil {
				as.broadcastTagged = make(map[broadcastFlowKey]uint64)
			}
			flowLimit := as.broadcastFlowLimit
			if flowLimit <= 0 {
				flowLimit = DefaultBroadcastFlowCacheEntries
			}
			if _, known := as.broadcastTagged[flow]; !known && len(as.broadcastTagged) >= flowLimit {
				clear(as.broadcastTagged)
			}
			as.broadcastTagged[flow] = epoch
		}
		as.mu.Unlock()
	}
	return TrafficDistribution{Delivered: delivered}, failed, broadcast || broadcastTargetsSet, epoch, tagged, errors.Join(deliveryErrors...)
}

func dataBroadcastFlowKey(data *messages.Data, protocolData *params.ProtocolDataPayload) broadcastFlowKey {
	key := broadcastFlowKey{
		routingContext:       data.RoutingContext.RoutingContext(),
		originatingPointCode: protocolData.OriginatingPointCode,
		destinationPointCode: protocolData.DestinationPointCode,
		serviceIndicator:     protocolData.ServiceIndicator,
		networkIndicator:     protocolData.NetworkIndicator,
		signalingLink:        protocolData.SignalingLinkSelection,
	}
	if data.NetworkAppearance != nil {
		key.networkAppearance = data.NetworkAppearance.NetworkAppearance()
		key.networkAppearanceSet = true
	}
	return key
}

func classifyBroadcastFlow(policy distributionPolicy, data *messages.Data, protocolData *params.ProtocolDataPayload) (key broadcastFlowKey, err error) {
	key = dataBroadcastFlowKey(data, protocolData)
	if policy.broadcastFlowIdentifier == nil {
		return key, nil
	}
	payloadCopy := *protocolData
	payloadCopy.Data = append([]byte(nil), protocolData.Data...)
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("BroadcastFlowIdentifier panicked: %v", recovered)
		}
	}()
	key.applicationFlow, err = policy.broadcastFlowIdentifier(&payloadCopy)
	if err != nil {
		return broadcastFlowKey{}, fmt.Errorf("BroadcastFlowIdentifier: %w", err)
	}
	if len(key.applicationFlow) > policy.broadcastFlowIdentifierBytes {
		return broadcastFlowKey{}, fmt.Errorf("%w: got %d bytes, limit %d",
			ErrBroadcastFlowIdentifierTooLong, len(key.applicationFlow), policy.broadcastFlowIdentifierBytes)
	}
	return key, nil
}

func (as *applicationServer) targetsLocked(sls uint8) ([]*Conn, error) {
	active := as.active
	if len(active) == 0 {
		return nil, ErrNoActiveASP
	}
	switch as.trafficMode {
	case params.TrafficModeBroadcast:
		return append([]*Conn(nil), active...), nil
	case params.TrafficModeOverride:
		return active[:1], nil
	case 0, params.TrafficModeLoadshare:
		return []*Conn{active[int(sls)%len(active)]}, nil
	default:
		return nil, ErrUnsupportedTrafficMode
	}
}

func (as *applicationServer) activeASPsLocked() []*Conn {
	return append([]*Conn(nil), as.active...)
}

func (as *applicationServer) hasActiveASPLocked() bool {
	for _, state := range as.asps {
		if state == StateAspActive {
			return true
		}
	}
	return false
}

func (as *applicationServer) noteBroadcastActivationLocked() {
	if as.trafficMode != params.TrafficModeBroadcast {
		return
	}
	as.broadcastEpoch++
	as.broadcastTagged = nil
	if as.broadcastEpoch == 0 {
		as.broadcastEpoch = 1
	}
}

func (as *applicationServer) allocateCorrelationIDLocked() uint32 {
	as.nextCorrelationID++
	if as.nextCorrelationID == 0 {
		as.nextCorrelationID++
	}
	return as.nextCorrelationID
}

// drainRecoveryQueue serializes each potentially blocking write with state
// changes but releases deliveryMu between messages. New DATA sees draining and
// joins the tail, so recovery traffic cannot be overtaken and the ASP state
// machine never waits for the entire backlog.
func (as *applicationServer) drainRecoveryQueue() {
	for {
		as.deliveryMu.Lock()
		as.mu.Lock()
		if as.state != ASActive {
			as.draining = false
			as.mu.Unlock()
			as.deliveryMu.Unlock()
			return
		}
		if len(as.recoveryQueue) == 0 {
			as.draining = false
			as.stopDrainRetryLocked()
			as.drainRetryDelay = 0
			as.mu.Unlock()
			as.deliveryMu.Unlock()
			return
		}
		item := as.recoveryQueue[0]
		as.recoveryQueue[0] = queuedData{}
		as.recoveryQueue = as.recoveryQueue[1:]
		as.deliveryInFlightBytes = item.size
		as.mu.Unlock()

		protocolData, err := item.data.ProtocolData.ProtocolData()
		if err != nil {
			as.mu.Lock()
			as.finishRecoveryItemLocked(item)
			as.mu.Unlock()
			as.deliveryMu.Unlock()
			continue
		}
		_, failedTargets, broadcastTargetsSet, epoch, marker, err := as.deliverAttemptLocked(
			item.data,
			protocolData,
			item.flow,
			item.broadcastTargets,
			item.broadcastTargetsSet,
			item.broadcastEpoch,
			item.broadcastMarker,
		)
		item.broadcastTargets = failedTargets
		item.broadcastTargetsSet = broadcastTargetsSet
		item.broadcastEpoch = epoch
		item.broadcastMarker = marker
		if err == nil {
			as.mu.Lock()
			as.finishRecoveryItemLocked(item)
			as.drainRetryDelay = 0
			as.mu.Unlock()
			as.deliveryMu.Unlock()
			continue
		}

		as.mu.Lock()
		restored := !as.closed &&
			(as.state == ASPending || as.state == ASActive) &&
			item.recoveryGen > as.recoveryExpiredGen
		if restored {
			if as.state == ASPending {
				item.recoveryGen = as.recoveryGen
			}
			as.recoveryQueue = append([]queuedData{item}, as.recoveryQueue...)
			as.deliveryInFlightBytes = 0
		} else {
			as.finishRecoveryItemLocked(item)
		}
		as.draining = false
		if restored && as.state == ASActive {
			as.scheduleDrainRetryLocked()
		}
		as.mu.Unlock()
		as.deliveryMu.Unlock()
		if restored {
			logf("m3ua: retained recovery DATA for AS %v after delivery failed: %v", as.key, err)
		}
		return
	}
}

func (as *applicationServer) scheduleDrainRetryLocked() {
	if as.drainRetry != nil || as.closed || as.state != ASActive || as.activeSending || len(as.recoveryQueue) == 0 {
		return
	}
	delay := as.drainRetryDelay
	if delay <= 0 {
		delay = initialRecoveryDrainRetry
	} else {
		delay *= 2
		if delay > maximumRecoveryDrainRetry {
			delay = maximumRecoveryDrainRetry
		}
	}
	as.drainRetryDelay = delay
	as.drainRetry = time.AfterFunc(delay, func() {
		as.mu.Lock()
		as.drainRetry = nil
		if as.closed || as.state != ASActive || as.draining || as.activeSending || len(as.recoveryQueue) == 0 {
			as.mu.Unlock()
			return
		}
		as.draining = true
		as.mu.Unlock()
		go as.drainRecoveryQueue()
	})
}

func (as *applicationServer) stopDrainRetryLocked() {
	if as.drainRetry != nil {
		as.drainRetry.Stop()
		as.drainRetry = nil
	}
}

func (as *applicationServer) finishRecoveryItemLocked(item queuedData) {
	as.finishDeliveryItemLocked(item.size)

}

func (as *applicationServer) finishDeliveryItemLocked(size int) {
	as.deliveryInFlightBytes = 0
	as.recoveryQueueBytes -= size
	if as.recoveryQueueBytes < 0 {
		panic("m3ua: AS recovery byte accounting underflow")
	}
	as.recoveryBudget.release(1, size)
}
