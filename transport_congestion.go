package m3ua

import (
	"cmp"
	"slices"
	"sync"
	"time"

	"github.com/gomaja/go-sctp"
)

type transportCongestionJob struct {
	association *Association
	id          AssociationID
	epoch       uint64
}

type transportCongestionMonitor struct {
	config     TransportCongestionConfig
	read       func(*Association) (*sctp.SendBacklog, error)
	readStatus func(*Association) (*sctp.Status, error)
	now        func() time.Time
	started    bool // Endpoint.mu guards startup against Close.
	workers    sync.WaitGroup
	projection chan struct{}
}

// TransportCongestion returns the latest local outbound transport-pressure
// observation. Known=false means unavailable or stale, never measured clear.
// Level is retained through query failure for diagnosis. This snapshot is
// independent of received SCON and local inbound overload (RFC 4666 §1.4.6).
func (c *Association) TransportCongestion() TransportCongestionState {
	if c == nil {
		return TransportCongestionState{}
	}
	if p := c.transportSnapshot.Load(); p != nil && p.Epoch == c.Epoch() {
		return *p
	}
	return TransportCongestionState{Association: c.ID(), Epoch: c.Epoch()}
}

func (m *transportCongestionMonitor) startLocked(e *Endpoint) {
	if m == nil || m.config.Disabled || m.started {
		return
	}
	m.started = true
	m.projection = make(chan struct{}, 1)
	jobs := make(chan transportCongestionJob, m.config.Workers*16)
	m.workers.Add(m.config.Workers + 2)
	go func() { defer m.workers.Done(); m.projectChanges(e) }()
	for range m.config.Workers {
		go func() {
			defer m.workers.Done()
			for job := range jobs {
				m.sample(e, job)
				job.association.transportBusy.Store(false)
			}
		}()
	}
	go func() { defer m.workers.Done(); defer close(jobs); m.run(e, jobs) }()
}

// The subscription callback owns initial channel setup. Neither a new sampler
// nor an existing Endpoint sampler may query this association before it returns.
func (c *Association) enableTransportSampling() {
	c.transportReady.Store(true)
	if c.endpoint == nil || c.sctpConn == nil {
		return
	}
	e := c.endpoint
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.closed && e.associationsByID[c.ID()] == c && !associationDone(c) {
		e.transport.startLocked(e)
	}
}

func (m *transportCongestionMonitor) snapshot(e *Endpoint) []transportCongestionJob {
	e.mu.Lock()
	jobs := make([]transportCongestionJob, 0, len(e.associations))
	for a := range e.associations {
		jobs = append(jobs, transportCongestionJob{a, a.ID(), a.Epoch()})
	}
	e.mu.Unlock()
	kept := jobs[:0]
	for _, job := range jobs {
		a := job.association
		if a.sctpConn == nil || !a.transportReady.Load() || a.terminating.Load() {
			continue
		}
		kept = append(kept, job)
	}
	slices.SortFunc(kept, func(a, b transportCongestionJob) int { return cmp.Compare(a.id, b.id) })
	return kept
}

func (m *transportCongestionMonitor) run(e *Endpoint, jobs chan<- transportCongestionJob) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-timer.C:
		}
		start := time.Now()
		batch := m.snapshot(e)
		for index, job := range batch {
			due := start.Add(time.Duration(index) * m.config.Interval / time.Duration(len(batch)))
			timer.Reset(time.Until(due))
			select {
			case <-e.ctx.Done():
				return
			case <-timer.C:
			}
			if !job.association.transportBusy.CompareAndSwap(false, true) {
				m.missed(e, job)
				continue
			}
			select {
			case jobs <- job:
			default:
				job.association.transportBusy.Store(false)
				m.missed(e, job)
			}
		}
		timer.Reset(time.Until(start.Add(m.config.Interval)))
	}
}

func associationDone(c *Association) bool {
	select {
	case <-c.done:
		return true
	default:
		return c.terminating.Load()
	}
}

// Socket queries run without Endpoint, routing or association locks. A busy
// bit prevents overlapping queries, including across a restart. Epoch reset
// and commit share transportMu, so an old result cannot cross the boundary.
func (m *transportCongestionMonitor) sample(e *Endpoint, job transportCongestionJob) {
	a := job.association
	// A control write can hold muState for seconds. Missing this association's
	// slot must neither occupy a worker nor delay another association's query.
	if !a.muState.TryRLock() {
		m.missed(e, job)
		return
	}
	state := a.state
	a.muState.RUnlock()
	if state == StateSCTPCDI || state == StateSCTPRI {
		m.missed(e, job)
		return
	}
	backlog, err := m.read(job.association)
	now := m.now()
	e.mu.Lock()
	a.transportMu.Lock()
	if e.closed || e.associationsByID[job.id] != a || job.id != a.ID() || job.epoch != a.Epoch() || associationDone(a) {
		a.transportMu.Unlock()
		e.mu.Unlock()
		return
	}
	initial := a.transportSnapshot.Load() == nil
	before := a.TransportCongestion()
	detector := a.transportDetectorLocked()
	detector.state.Association = job.id
	detector.state.Epoch = job.epoch
	after := detector.observe(m.config, now, backlog, err)
	diagnosticDue := after.Level != before.Level || after.Level > 0 && (before.DiagnosticAt.IsZero() || now.Sub(before.DiagnosticAt) >= time.Second)

	a.transportSnapshot.Store(&after)
	changed := m.publishLocked(a, before, after, initial)
	queued := changed && m.queueProjectionLocked(a)
	a.transportMu.Unlock()
	e.mu.Unlock()
	if changed && !queued {
		m.project(e, a)
	}
	if diagnosticDue && m.readStatus != nil {
		m.diagnose(e, job)
	}

}

func (m *transportCongestionMonitor) missed(e *Endpoint, job transportCongestionJob) {
	e.mu.Lock()
	a := job.association
	a.transportMu.Lock()
	changed := false
	queued := false
	if !e.closed && e.associationsByID[job.id] == a && job.epoch == a.Epoch() && !associationDone(a) {
		initial := a.transportSnapshot.Load() == nil
		before := a.TransportCongestion()
		after := before
		after.MissedSamples++
		after.SampleValid = false
		detector := a.transportDetectorLocked()
		detector.resetDwell()
		if after.LastValidAt.IsZero() || m.now().Sub(after.LastValidAt) >= m.config.StaleAfter {
			after.Known = false
		}
		detector.state = after
		a.transportSnapshot.Store(&after)
		changed = m.publishLocked(a, before, after, initial)
		queued = changed && m.queueProjectionLocked(a)
	}
	a.transportMu.Unlock()
	e.mu.Unlock()
	if changed && !queued {
		m.project(e, a)
	}
}

// Endpoint.mu guards startup. One pending bit per association and one wake-up
// coalesce transitions without a growing publication queue. Routing may wait
// for an AS state commit; it must not occupy the scheduler or sampling workers.
func (m *transportCongestionMonitor) queueProjectionLocked(a *Association) bool {
	if !m.started {
		return false
	}
	a.transportProjectionPending.Store(true)
	select {
	case m.projection <- struct{}{}:
	default:
	}
	return true
}

func (m *transportCongestionMonitor) projectChanges(e *Endpoint) {
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-m.projection:
		}
		e.mu.Lock()
		pending := make([]*Association, 0)
		for a := range e.associations {
			if a.transportProjectionPending.Swap(false) {
				pending = append(pending, a)
			}
		}
		e.mu.Unlock()
		for _, a := range pending {
			if e.ctx.Err() != nil {
				return
			}
			m.project(e, a)
		}
	}
}

func (m *transportCongestionMonitor) publishLocked(a *Association, before, after TransportCongestionState, initial bool) bool {
	if !initial && before.Level == after.Level && before.Known == after.Known && before.SampleValid == after.SampleValid && before.Error == after.Error && !after.LimitChanged {
		return false
	}
	a.notifyManagement(&ManagementIndication{Kind: ManagementTransportCongestion, TransportCongestion: &after, ASKeys: endpointASPStatusKeys(a), Description: "outbound SCTP transport pressure changed"})
	return initial || before.Level != after.Level || before.Known != after.Known || before.SampledAt.IsZero()
}

func (m *transportCongestionMonitor) project(e *Endpoint, a *Association) {
	// Projection reads the latest immutable snapshot after releasing all
	// Endpoint and detector locks. A delayed projection cannot replay old state.
	if e.aspRoutes != nil {
		e.aspRoutes.transportChanged(a)
	}
	e.refreshIPSPDestinations()
}

// transportMu guards the lazily allocated dwell state. Keeping it out of the
// association allocation preserves the existing DATA fields and their layout.
func (c *Association) transportDetectorLocked() *transportCongestionDetector {
	if c.transportDetector == nil {
		c.transportDetector = &transportCongestionDetector{state: c.TransportCongestion()}
	}
	return c.transportDetector
}

func (c *Association) resetTransportCongestionEpoch() {
	c.transportMu.Lock()
	before := c.transportSnapshot.Load()
	c.epoch.Add(1)
	if before == nil {
		// Absence means this association has no detector contribution. Restart
		// must not invent one for disabled or otherwise unsampled associations
		// (RFC 4666 Sections 1.4.6 and 4.3.3).
		c.transportDetector = nil
	} else {
		state := TransportCongestionState{Association: c.ID(), Epoch: c.Epoch(), Generation: before.Generation + 1}
		detector := c.transportDetectorLocked()
		*detector = transportCongestionDetector{state: state}
		c.transportSnapshot.Store(&state)
	}
	c.transportMu.Unlock()
	if c.endpoint != nil {
		c.endpoint.transport.project(c.endpoint, c)
	}
}

func readTransportSCTPStatus(a *Association) (*sctp.Status, error) {
	if a.sctpConn == nil {
		return nil, ErrAssociationClosed
	}
	return a.sctpConn.Status()
}

// Diagnostics never delay committing a backlog measurement. A late status
// result adds only diagnostics to the latest snapshot, preserving any missed
// deadline, error or stale transition recorded while the query was in flight.
func (m *transportCongestionMonitor) diagnose(e *Endpoint, job transportCongestionJob) {
	diagnostic, err := m.readStatus(job.association)
	now := m.now()
	e.mu.Lock()
	defer e.mu.Unlock()
	a := job.association
	a.transportMu.Lock()
	defer a.transportMu.Unlock()
	if e.closed || e.associationsByID[job.id] != a || job.epoch != a.Epoch() || associationDone(a) {
		return
	}
	after := a.TransportCongestion()
	after.DiagnosticAt = now
	after.DiagnosticValid = err == nil && diagnostic != nil
	after.DiagnosticError = ""
	if err != nil {
		after.DiagnosticError = err.Error()
	}
	if diagnostic != nil {
		after.ReceiverWindow = diagnostic.PeerRwnd
		after.UnackedDataChunks = diagnostic.Unacked
		after.PrimaryCwnd = diagnostic.Primary.Cwnd
	}
	a.transportDetectorLocked().state = after
	a.transportSnapshot.Store(&after)
}
