package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

// gate pauses the goroutines that pass through it.
type gate struct {
	mutex sync.Mutex
	wait  chan struct{}
}

func (level *gate) pause() {
	level.mutex.Lock()
	defer level.mutex.Unlock()
	if level.wait == nil {
		level.wait = make(chan struct{})
	}
}

func (level *gate) resume() {
	level.mutex.Lock()
	defer level.mutex.Unlock()
	if level.wait != nil {
		close(level.wait)
		level.wait = nil
	}
}

// pass returns once the gate is open, reporting whether it had to wait.
func (level *gate) pass(ctx context.Context) bool {
	level.mutex.Lock()
	wait := level.wait
	level.mutex.Unlock()
	if wait == nil {
		return false
	}
	select {
	case <-wait:
	case <-ctx.Done():
	}
	return true
}

// subscriber is one healthy SSNM subscription consumer. During the overload
// phase it is paused so its 256-event queue overflows; the events it delivers
// after it resumes and before the continuity-loss marker are the retained
// queue, which the event cap bounds.
type subscriber struct {
	index        int
	subscription *m3ua.SSNMSubscription

	mutex     sync.Mutex
	summary   subscriberSummary
	lastEvent time.Time
	armed     bool
	counting  bool
	sinceArm  int
	overload  subscriberOverload
}

func (consumer *subscriber) arm() {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	consumer.armed, consumer.counting, consumer.sinceArm = true, false, 0
	consumer.overload = subscriberOverload{Subscriber: consumer.index}
}

func (consumer *subscriber) run(ctx context.Context, pause *gate, phase func() string) {
	for {
		waited := pause.pass(ctx)
		consumer.mutex.Lock()
		if waited && consumer.armed {
			consumer.counting, consumer.sinceArm = true, 0
		}
		consumer.mutex.Unlock()
		event, err := consumer.subscription.Next(ctx)
		consumer.mutex.Lock()
		if err != nil {
			consumer.summary.TerminalError = err.Error()
			consumer.mutex.Unlock()
			return
		}
		consumer.summary.Events++
		consumer.summary.ByKind[event.Kind.String()]++
		consumer.lastEvent = time.Now()
		consumer.summary.LastRevision = max(consumer.summary.LastRevision, event.Revision)
		lost := event.ContinuityLost
		if lost {
			consumer.summary.ContinuityLoss++
			consumer.summary.LossByPhase[phase()]++
			if consumer.counting {
				consumer.overload.LossObserved = true
				consumer.overload.DeliveredBeforeLoss = consumer.sinceArm
			}
		} else if consumer.counting {
			consumer.sinceArm++
		}
		consumer.mutex.Unlock()
		if !lost {
			continue
		}
		snapshot, err := consumer.subscription.Resync()
		consumer.mutex.Lock()
		if err != nil {
			consumer.summary.ResyncErrorText = err.Error()
		} else {
			consumer.summary.Resyncs++
			consumer.summary.LastRevision = max(consumer.summary.LastRevision, snapshot.Revision)
			if consumer.counting {
				consumer.overload.Resynced = true
				consumer.armed, consumer.counting = false, false
			}
		}
		consumer.lastEvent = time.Now()
		consumer.mutex.Unlock()
	}
}

type churnTracker struct {
	mutex sync.Mutex
	stats aspChurnStats
	open  int
}

func (tracker *churnTracker) reset() {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	tracker.stats = aspChurnStats{FailureReasons: map[string]int{}, ByMode: map[string]int{}}
}

func (tracker *churnTracker) accept() {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	tracker.stats.Accepted++
	tracker.open++
}

func (tracker *churnTracker) reject() {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	tracker.stats.Rejected++
}

func (tracker *churnTracker) release(mode closeMode, reason string, err error) {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	tracker.open--
	if err != nil {
		tracker.stats.Failed++
		tracker.stats.FailureReasons[reason]++
		if tracker.stats.FirstFailure == "" {
			tracker.stats.FirstFailure = reason + ": " + err.Error()
		}
		return
	}
	tracker.stats.Released++
	tracker.stats.ByMode[mode.String()]++
}

func (tracker *churnTracker) snapshot() aspChurnStats {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	stats := tracker.stats
	stats.StillOpen = tracker.open
	return stats
}

type aspRun struct {
	config   commandConfig
	ctx      context.Context
	endpoint *m3ua.Endpoint
	listener *m3ua.Listener
	source   procSource
	recorder *sampler
	client   *controlClient

	readers        gate
	subscriberGate gate
	subscribers    []*subscriber
	ledger         receiveLedger
	churn          churnTracker
	holdNanos      atomic.Int64
	shuttingDown   atomic.Bool
	indications    atomic.Uint64
	resyncMarkers  atomic.Uint64

	mutex       sync.Mutex
	stable      []*m3ua.Association
	stableIDs   map[m3ua.AssociationID]int
	registered  int
	stableReady chan struct{}
	stableEnded []string
}

func runASP(ctx context.Context, config commandConfig) (record aspRecord) {
	record = aspRecord{Kind: "perfchurn-asp", Label: config.Label, Config: config, Manifest: currentManifest(roleASP, config)}
	asp := &aspRun{config: config, ctx: ctx, source: defaultProcSource(), client: newControlClient(config.PeerControl),
		stable: make([]*m3ua.Association, stableAssociations), stableIDs: map[m3ua.AssociationID]int{},
		stableReady: make(chan struct{})}
	asp.churn.reset()
	asp.recorder = newSampler(asp.source, rssInterval, heapInterval)
	samplerContext, stopSampler := context.WithCancel(context.Background())
	samplerDone := make(chan struct{})
	go func() {
		asp.recorder.run(samplerContext)
		close(samplerDone)
	}()
	defer func() {
		stopSampler()
		<-samplerDone
		record.Phases, record.RSSSeries, record.HeapSeries = asp.recorder.snapshot()
		evaluateRun(&record)
	}()
	if err := asp.start(&record); err != nil {
		record.Error = err.Error()
		asp.stop(&record)
		return record
	}
	if err := asp.phases(&record); err != nil {
		record.Error = err.Error()
	}
	asp.stop(&record)
	return record
}

func (asp *aspRun) start(record *aspRecord) error {
	endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleASP, ASP: aspInventory(asp.config.Routes), SSNMState: ssnmStateConfig()})
	if err != nil {
		return fmt.Errorf("ASP endpoint: %w", err)
	}
	asp.endpoint = endpoint
	record.MTPIndication.Capacity = cap(endpoint.MTPIndications())
	go func() {
		for indication := range endpoint.MTPIndications() {
			asp.indications.Add(1)
			if indication.ResyncRequired {
				asp.resyncMarkers.Add(1)
			}
		}
	}()
	for index := 0; index < subscriberCount; index++ {
		_, subscription, err := endpoint.SubscribeSSNM()
		if err != nil {
			return fmt.Errorf("subscription %d: %w", index, err)
		}
		consumer := &subscriber{index: index, subscription: subscription,
			summary: subscriberSummary{Subscriber: index, ByKind: map[string]int{}, LossByPhase: map[string]int{}}}
		asp.subscribers = append(asp.subscribers, consumer)
		go consumer.run(asp.ctx, &asp.subscriberGate, asp.recorder.currentPhase)
	}
	address, err := sctp.ResolveSCTPAddr("sctp", asp.config.SCTPAddress)
	if err != nil {
		return fmt.Errorf("resolve listen address: %w", err)
	}
	listener, err := endpoint.Listen("m3ua", address, &m3ua.ListenerConfig{
		SelectAssociationConfig: func(info m3ua.AcceptInfo) (*m3ua.AssociationConfig, error) {
			if info.RemoteAddr == nil {
				return nil, errUnknownPeerPort
			}
			role, err := classifyPort(info.RemoteAddr.Port)
			if err != nil {
				return nil, err
			}
			return aspAssociationConfig(role), nil
		},
	})
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	asp.listener = listener
	for index := 0; index < asp.config.AcceptConcurrency; index++ {
		go asp.acceptLoop()
	}
	return nil
}

func (asp *aspRun) acceptLoop() {
	for {
		association, err := asp.listener.Accept(asp.ctx)
		if err != nil {
			var establishment *m3ua.AssociationEstablishmentError
			if errors.As(err, &establishment) {
				asp.churn.reject()
				continue
			}
			return
		}
		remote, ok := association.RemoteAddr().(*sctp.SCTPAddr)
		if !ok {
			_ = association.Close()
			asp.churn.reject()
			continue
		}
		role, err := classifyPort(remote.Port)
		if err != nil {
			_ = association.Close()
			asp.churn.reject()
			continue
		}
		if role.Stable {
			asp.registerStable(role.StableIndex, association)
			continue
		}
		go asp.serveChurn(association, role)
	}
}

func (asp *aspRun) registerStable(index int, association *m3ua.Association) {
	asp.mutex.Lock()
	if asp.stable[index] != nil {
		asp.stableEnded = append(asp.stableEnded, fmt.Sprintf("stable association %d accepted twice", index))
		asp.mutex.Unlock()
		_ = association.Close()
		return
	}
	asp.stable[index] = association
	asp.stableIDs[association.ID()] = index
	asp.registered++
	if asp.registered == stableAssociations {
		close(asp.stableReady)
	}
	asp.mutex.Unlock()
	drainChannels(association, func() {
		if asp.ctx.Err() == nil && !asp.shuttingDown.Load() {
			asp.mutex.Lock()
			asp.stableEnded = append(asp.stableEnded, fmt.Sprintf("stable association %d (%d) ended in phase %s: %v",
				index, association.ID(), asp.recorder.currentPhase(), association.Err()))
			asp.mutex.Unlock()
		}
	})
	go func() {
		for {
			asp.readers.pass(asp.ctx)
			message, err := association.ReadData(asp.ctx)
			if err != nil {
				return
			}
			if message.ProtocolData != nil {
				asp.ledger.record(index, message.ProtocolData.Data)
			}
		}
	}()
}

// serveChurn is the independent child close of one accepted churn
// association: the listener and every sibling stay as they are.
func (asp *aspRun) serveChurn(association *m3ua.Association, role portRole) {
	asp.churn.accept()
	drained := drainChannels(association, nil)
	hold := time.Duration(asp.holdNanos.Load())
	var (
		err    error
		reason string
	)
	waitHold := func() {
		select {
		case <-time.After(hold):
		case <-association.Done():
		case <-asp.ctx.Done():
		}
	}
	switch role.CloseMode {
	case closeASPGraceful:
		waitHold()
		shutdown, cancel := context.WithTimeout(asp.ctx, 10*time.Second)
		err, reason = association.ShutdownContext(shutdown), "graceful-shutdown"
		cancel()
	case closeASPAbrupt:
		waitHold()
		err, reason = association.Close(), "close"
	default:
		select {
		case <-association.Done():
		case <-time.After(hold + 20*time.Second):
			err, reason = errors.New("peer close not observed"), "peer-close-not-observed"
		}
		_ = association.Close()
	}
	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		if err == nil {
			err, reason = errors.New("indication channels still open after close"), "channels-open"
		}
	}
	asp.churn.release(role.CloseMode, reason, err)
}

func (asp *aspRun) stableSet() []*m3ua.Association {
	asp.mutex.Lock()
	defer asp.mutex.Unlock()
	return append([]*m3ua.Association(nil), asp.stable...)
}

func (asp *aspRun) phases(record *aspRecord) error {
	if err := asp.warm(record); err != nil {
		return err
	}
	asp.recorder.setPhase(phaseSteady)
	ledgers, err := asp.traffic(1, func() error {
		return sleepContext(asp.ctx, asp.config.Steady)
	})
	record.Steady = ledgers
	if err != nil {
		return fmt.Errorf("steady: %w", err)
	}
	asp.recorder.setPhase(phaseBaseline)
	drained, err := asp.drain()
	if err != nil {
		return fmt.Errorf("baseline drain: %w", err)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		if attempt > 1 {
			if err := sleepContext(asp.ctx, 2*time.Second); err != nil {
				return err
			}
		}
		record.Baseline = append(record.Baseline, asp.retainedSample(attempt, drained))
	}
	ordered := append([]retainedSample(nil), record.Baseline...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].LiveHeapBytes < ordered[j].LiveHeapBytes })
	record.BaselineFinal = ordered[len(ordered)/2]
	if err := asp.overload(record); err != nil {
		return fmt.Errorf("overload: %w", err)
	}
	for block := 1; block <= asp.config.Blocks; block++ {
		result, err := asp.churnBlock(record, block)
		record.Blocks = append(record.Blocks, result)
		if err != nil {
			return fmt.Errorf("churn block %d: %w", block, err)
		}
	}
	return nil
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (asp *aspRun) warm(record *aspRecord) error {
	asp.recorder.setPhase(phaseWarm)
	if err := asp.client.waitReady(asp.ctx, 60*time.Second); err != nil {
		return err
	}
	started := time.Now()
	var established establishResponse
	if err := asp.client.call(asp.ctx, operationEstablish, 2*time.Minute, struct{}{}, &established); err != nil {
		return err
	}
	select {
	case <-asp.stableReady:
	case <-time.After(60 * time.Second):
		return fmt.Errorf("only %d of %d stable associations accepted", asp.registeredCount(), stableAssociations)
	case <-asp.ctx.Done():
		return asp.ctx.Err()
	}
	record.Warm.StableAccepted = asp.registeredCount()
	record.Warm.EstablishMillis = time.Since(started).Milliseconds()
	for _, association := range asp.stableSet() {
		record.Manifest.Limits.ObservedChannelCapacities = map[string]int{
			"state_changes":          cap(association.StateChanges()),
			"management_indications": cap(association.ManagementIndications()),
			"signalling_status":      cap(association.SignallingStatus()),
			"data_queue":             association.DataQueueStats().Capacity,
			"mtp_indications":        record.MTPIndication.Capacity,
		}
		break
	}
	started = time.Now()
	var populated populateResponse
	if err := asp.client.call(asp.ctx, operationPopulate, 5*time.Minute, struct{}{}, &populated); err != nil {
		return err
	}
	record.Warm.PeerMessages, record.Warm.PeerErrors = populated.Messages, populated.Errors
	deadline := time.Now().Add(60 * time.Second)
	for {
		record.Warm.Store = asp.storeSummary()
		if record.Warm.Store.AvailabilityRecords >= stateRecords || time.Now().After(deadline) {
			break
		}
		if err := sleepContext(asp.ctx, 500*time.Millisecond); err != nil {
			return err
		}
	}
	record.Warm.PopulateMillis = time.Since(started).Milliseconds()
	if record.Warm.Store.records() != stateRecords || record.Warm.Store.Partitions != partitionCount || len(populated.Errors) != 0 {
		record.Warm.Error = fmt.Sprintf("reference store not reached: %d records in %d partitions, %d refused, peer errors %v",
			record.Warm.Store.records(), record.Warm.Store.Partitions, record.Warm.Store.RecordsRefused, populated.Errors)
		return errors.New(record.Warm.Error)
	}
	if !asp.waitSubscribersQuiet(30 * time.Second) {
		return errors.New("subscribers did not drain the population")
	}
	return nil
}

func (asp *aspRun) registeredCount() int {
	asp.mutex.Lock()
	defer asp.mutex.Unlock()
	return asp.registered
}

func (asp *aspRun) storeSummary() storeSummary {
	snapshot := asp.endpoint.SSNMKnowledge()
	summary := storeSummary{Revision: snapshot.Revision, Partitions: len(snapshot.Partitions),
		RecordsRefused: snapshot.RecordsRefused, ReportsRefused: snapshot.ReportsRefused,
		PartitionsInvalidated: snapshot.PartitionsInvalidated, LastResourceLoss: snapshot.LastResourceLoss}
	for _, partition := range snapshot.Partitions {
		for _, destination := range partition.Destinations {
			if destination.AvailabilitySet {
				summary.AvailabilityRecords++
			}
			if destination.CongestionSet {
				summary.CongestionRecords++
			}
		}
	}
	return summary
}

// traffic runs one ledgered epoch in both directions on the stable
// associations while during runs, then stops both senders and judges both
// ledgers.
func (asp *aspRun) traffic(epoch uint32, during func() error) ([]ledgerResult, error) {
	perAssociation := asp.config.DataRate / stableAssociations
	asp.ledger.reset(epoch)
	if err := asp.client.call(asp.ctx, operationTrafficStart, 30*time.Second,
		trafficStartRequest{Epoch: epoch, PerAssociation: perAssociation}, &struct{}{}); err != nil {
		return nil, err
	}
	sender := startLedgerSender(asp.ctx, asp.stableSet(), true, epoch, perAssociation)
	duringErr := during()
	written := sender.stop()
	var stopped trafficStopResponse
	err := asp.client.call(asp.ctx, operationTrafficStop, time.Minute, trafficStopRequest{Epoch: epoch, ASPSent: written.Sent,
		ASPWriteErrors: written.Errors, ASPFirstError: written.FirstError, ASPRefused: written.Refused, DrainWaitMillis: 10000}, &stopped)
	if err != nil {
		return nil, errors.Join(duringErr, err)
	}
	waitLedger(asp.ctx, &asp.ledger, stopped.PeerSent, 10*time.Second)
	received := asp.ledger.result("sgp-to-asp", stopped.PeerSent, stopped.PeerWriteErrors, stopped.PeerFirstError)
	received.Refused = stopped.PeerRefused
	return []ledgerResult{stopped.Ledger, received}, duringErr
}

func (asp *aspRun) queueSnapshot() (maxQueued, full int, discarded uint64) {
	for _, association := range asp.stableSet() {
		stats := association.DataQueueStats()
		maxQueued = max(maxQueued, stats.Queued)
		if stats.Capacity > 0 && stats.Queued == stats.Capacity {
			full++
		}
		discarded += stats.Discarded
	}
	return maxQueued, full, discarded
}

// drain waits for empty DATA queues and quiet subscribers and returns when
// the drain completed.
func (asp *aspRun) drain() (time.Time, error) {
	deadline := time.Now().Add(60 * time.Second)
	for {
		queued, _, _ := asp.queueSnapshot()
		if queued == 0 {
			break
		}
		if time.Now().After(deadline) {
			return time.Time{}, fmt.Errorf("DATA queues still hold %d messages", queued)
		}
		if err := sleepContext(asp.ctx, 50*time.Millisecond); err != nil {
			return time.Time{}, err
		}
	}
	if !asp.waitSubscribersQuiet(time.Until(deadline)) {
		return time.Time{}, errors.New("subscribers did not drain")
	}
	return time.Now(), nil
}

// waitSubscribersQuiet waits until no subscriber has received anything for
// half a second. It is called only once the peer has finished reporting, so
// quiet subscribers have drained. Last revisions are not compared: a resync
// snapshot may carry a revision no event does.
func (asp *aspRun) waitSubscribersQuiet(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) && asp.ctx.Err() == nil {
		quiet := true
		for _, consumer := range asp.subscribers {
			consumer.mutex.Lock()
			if time.Since(consumer.lastEvent) < 500*time.Millisecond {
				quiet = false
			}
			consumer.mutex.Unlock()
		}
		if quiet {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func (asp *aspRun) retainedSample(attempt int, drained time.Time) retainedSample {
	snapshot := forcedRuntimeSnapshot()
	sample := retainedSample{Attempt: attempt, AtMillis: asp.recorder.millis(), SinceDrainMillis: time.Since(drained).Milliseconds(),
		LiveHeapBytes: snapshot.LiveHeapBytes, HeapObjectsBytes: snapshot.HeapObjectsBytes, Goroutines: runtime.NumGoroutine(),
		Classes: snapshot.Classes}
	if status, err := asp.source.status(); err != nil {
		sample.RSSError = err.Error()
	} else {
		sample.RSSBytes, sample.RSSAnonBytes, sample.Threads = status.RSS, status.RSSAnon, status.Threads
	}
	if huge, err := asp.source.anonHugePages(); err == nil {
		sample.AnonHugePages = huge
	}
	descriptors, inodes := asp.source.fds()
	sample.FDs = descriptors
	sample.Kernel = asp.source.kernel(inodes)
	statuses := asp.endpoint.AssociationStatuses()
	sample.Associations = len(statuses)
	asp.mutex.Lock()
	seen := make(map[m3ua.AssociationID]bool, len(statuses))
	for _, status := range statuses {
		if _, stable := asp.stableIDs[status.Association]; stable {
			seen[status.Association] = true
		} else {
			sample.Unexpected = append(sample.Unexpected, uint64(status.Association))
		}
	}
	for id := range asp.stableIDs {
		if !seen[id] {
			sample.MissingStable = append(sample.MissingStable, uint64(id))
		}
	}
	asp.mutex.Unlock()
	return sample
}

// retain takes post-drain samples, each after two forced collections, until
// one meets every retention condition or the 60-second window would close.
// Every attempt is kept; the last is the block's result.
func (asp *aspRun) retain(drained time.Time, baseline retainedSample) ([]retainedSample, retainedSample) {
	var attempts []retainedSample
	for attempt := 1; ; attempt++ {
		sample := asp.retainedSample(attempt, drained)
		sample.Pass, sample.Failures = evaluateRetention(baseline, sample, stableAssociations)
		attempts = append(attempts, sample)
		if sample.Pass || time.Since(drained)+5*time.Second > retainWindow || asp.ctx.Err() != nil {
			break
		}
		_ = sleepContext(asp.ctx, 5*time.Second)
	}
	final := attempts[len(attempts)-1]
	if !final.Pass {
		var profile bytes.Buffer
		_ = pprof.Lookup("goroutine").WriteTo(&profile, 1)
		if profile.Len() > 256<<10 {
			profile.Truncate(256 << 10)
		}
		final.GoroutineProfile = profile.String()
		attempts[len(attempts)-1] = final
	}
	return attempts, final
}

func (asp *aspRun) overload(record *aspRecord) error {
	asp.recorder.setPhase(phaseOverload)
	result := &record.Overload
	result.QueueCapacity = dataQueueSize
	before := asp.storeSummary()
	oomBefore, oomErr := asp.source.oomKills()
	_, _, discardedBefore := asp.queueSnapshot()
	asp.readers.pause()
	for _, consumer := range asp.subscribers {
		consumer.arm()
	}
	asp.subscriberGate.pause()
	resumed := false
	resume := func() {
		if !resumed {
			asp.readers.resume()
			asp.subscriberGate.resume()
			resumed = true
		}
	}
	defer resume()

	type peerResult struct {
		response overloadResponse
		err      error
	}
	peerDone := make(chan peerResult, 1)
	go func() {
		var response overloadResponse
		err := asp.client.call(asp.ctx, operationOverload, 5*time.Minute,
			overloadRequest{PerAssociation: dataQueueSize + asp.config.OverloadExtra, Toggles: asp.config.SSNMToggles}, &response)
		peerDone <- peerResult{response, err}
	}()
	observe := func() {
		queued, full, _ := asp.queueSnapshot()
		result.QueuedSamples++
		result.MaxQueued = max(result.MaxQueued, queued)
		result.FullAssociations = max(result.FullAssociations, full)
		result.MaxMTPIndicationQueue = max(result.MaxMTPIndicationQueue, len(asp.endpoint.MTPIndications()))
	}
	var peer peerResult
	for waiting := true; waiting; {
		select {
		case peer = <-peerDone:
			waiting = false
		case <-time.After(250 * time.Millisecond):
			observe()
		case <-asp.ctx.Done():
			return asp.ctx.Err()
		}
	}
	if peer.err != nil {
		return peer.err
	}
	result.PeerSent, result.PeerWriteErrors, result.SSNMReports = peer.response.Sent, peer.response.WriteErrors, peer.response.Reports
	result.PeerFirstWriteError, result.PeerRefused = peer.response.FirstWriteError, peer.response.Refused
	if len(peer.response.ReportErrors) != 0 {
		result.Error = fmt.Sprintf("peer SSNM report errors: %v", peer.response.ReportErrors)
	}
	// Hold the queues full and keep sampling: bounded queues must stay at
	// their caps rather than grow.
	holdEnd := time.Now().Add(asp.config.OverloadHold)
	for time.Now().Before(holdEnd) {
		observe()
		if err := sleepContext(asp.ctx, time.Second); err != nil {
			return err
		}
	}
	observe()
	_, _, discardedAfter := asp.queueSnapshot()
	result.Discarded = discardedAfter - discardedBefore

	asp.recorder.setPhase(phaseOverloadDrain)
	started := time.Now()
	resume()
	drained, err := asp.drain()
	if err != nil {
		return err
	}
	result.DrainMillis = time.Since(started).Milliseconds()
	result.Received = asp.ledger.overloadTotal()
	for _, consumer := range asp.subscribers {
		consumer.mutex.Lock()
		result.Subscribers = append(result.Subscribers, consumer.overload)
		consumer.armed, consumer.counting = false, false
		consumer.mutex.Unlock()
	}
	after := asp.storeSummary()
	result.StateRecords = after.records()
	result.RecordsRefusedDelta = after.RecordsRefused - before.RecordsRefused
	if oomAfter, err := asp.source.oomKills(); err != nil || oomErr != nil {
		result.OOMError = errors.Join(oomErr, err).Error()
	} else {
		result.OOMKills = oomAfter - oomBefore
	}
	asp.recorder.setPhase(phasePostOverload)
	_, record.PostOverload = asp.retain(drained, record.BaselineFinal)
	return nil
}

func (asp *aspRun) churnBlock(record *aspRecord, block int) (blockResult, error) {
	firstCycle := (block - 1) * asp.config.BlockCycles
	result := blockResult{Block: block, FirstCycle: firstCycle, Cycles: asp.config.BlockCycles}
	asp.recorder.setPhase(fmt.Sprintf("%s%d", phaseChurnPrefix, block))
	asp.holdNanos.Store(int64(asp.config.ChurnHold))
	asp.churn.reset()
	endedBefore := asp.stableEndedCount()
	started := time.Now()
	var peerErr error
	ledgers, err := asp.traffic(uint32(100+block), func() error {
		var response churnResponse
		plan := churnPlan{FirstCycle: firstCycle, Cycles: asp.config.BlockCycles, Rate: asp.config.ChurnRate, Group: asp.config.ChurnGroup}
		timeout := time.Duration(float64(plan.Cycles)/plan.Rate*float64(time.Second)) + asp.config.ChurnHold + 2*time.Minute
		peerErr = asp.client.call(asp.ctx, operationChurn, timeout, churnRequest{Plan: plan, HoldMillis: asp.config.ChurnHold.Milliseconds()}, &response)
		result.Peer = response.Stats
		// Every accepted child must be released before the block ends.
		deadline := time.Now().Add(30 * time.Second)
		for asp.churn.snapshot().StillOpen > 0 && time.Now().Before(deadline) {
			time.Sleep(50 * time.Millisecond)
		}
		return peerErr
	})
	result.TrafficRunning = time.Since(started).Seconds()
	result.Ledgers = ledgers
	result.ASP = asp.churn.snapshot()
	if peerErr != nil {
		result.PeerError = peerErr.Error()
	}
	if err != nil {
		return result, err
	}
	asp.recorder.setPhase(fmt.Sprintf("%s%d", phaseDrainPrefix, block))
	drainStarted := time.Now()
	drained, err := asp.drain()
	if err != nil {
		return result, err
	}
	result.DrainMillis = time.Since(drainStarted).Milliseconds()
	result.StableEnded = asp.stableEndedCount() - endedBefore
	asp.recorder.setPhase(fmt.Sprintf("%s%d", phaseRetainPrefix, block))
	result.Retained, result.Final = asp.retain(drained, record.BaselineFinal)
	return result, nil
}

func (asp *aspRun) stableEndedCount() int {
	asp.mutex.Lock()
	defer asp.mutex.Unlock()
	return len(asp.stableEnded)
}

func (asp *aspRun) stop(record *aspRecord) {
	asp.recorder.setPhase(phaseShutdown)
	if asp.endpoint != nil {
		record.Store = asp.storeSummary()
	}
	// The peer ends its associations once it has answered, so stable ends
	// from here on are the teardown, not a failure.
	asp.shuttingDown.Store(true)
	var peer peerRecord
	if err := asp.client.call(context.Background(), operationFinish, 30*time.Second, struct{}{}, &peer); err == nil {
		record.Peer = &peer
	} else if record.Error == "" {
		record.Error = err.Error()
	}
	for _, consumer := range asp.subscribers {
		consumer.mutex.Lock()
		summary := consumer.summary
		summary.ByKind, summary.LossByPhase = maps.Clone(summary.ByKind), maps.Clone(summary.LossByPhase)
		record.Subscribers = append(record.Subscribers, summary)
		consumer.mutex.Unlock()
	}
	record.MTPIndication.Received, record.MTPIndication.ResyncRequired = asp.indications.Load(), asp.resyncMarkers.Load()
	asp.mutex.Lock()
	record.StableEnded = append([]string(nil), asp.stableEnded...)
	asp.mutex.Unlock()
	if asp.endpoint != nil {
		_ = asp.endpoint.Close()
	}
}
