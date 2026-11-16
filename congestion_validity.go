package m3ua

import (
	"fmt"
	"slices"
	"time"
)

// CongestionValidityConfig enables local received-SCON validity. RFC 4666
// §3.4.4 says a node may maintain this timer. No validity is negotiated.
type CongestionValidityConfig struct {
	// After defaults to 30 seconds; negative values are invalid.
	After time.Duration
}

// DestinationAuditConfig enables optional ASP DAUD traffic (RFC 4666 §4.5.3).
// Nil disables it. Defaults are engineering choices, not protocol timer values.
type DestinationAuditConfig struct {
	// Period defaults to 30 seconds; negative values are invalid.
	Period time.Duration
	// OnActivation lists isolation audits to start after committed activation.
	// These scopes continue to be audited during extended isolation until DAVA.
	OnActivation []DestinationStateAuditRequest
	// MaxPending defaults to 256 exact source/scope/range jobs.
	MaxPending int
	// MaxMessagesPerSecond defaults to 10, with evenly spaced submissions.
	MaxMessagesPerSecond int
}

func resolveCongestionAutomation(role Role, input *CongestionConfig) (time.Duration, *DestinationAuditConfig, error) {
	var after time.Duration
	if input.Validity != nil {
		after = input.Validity.After
		if after < 0 {
			return 0, nil, fmt.Errorf("%w: negative congestion validity", ErrInvalidRoleConfiguration)
		}
		if after == 0 {
			after = 30 * time.Second
		}
	}
	if input.Auditing == nil {
		return after, nil, nil
	}
	if role != RoleASP {
		return 0, nil, fmt.Errorf("%w: destination auditing is ASP-only", ErrInvalidRoleConfiguration)
	}
	cfg := *input.Auditing
	if cfg.Period < 0 || cfg.MaxPending < 0 || cfg.MaxMessagesPerSecond < 0 {
		return 0, nil, fmt.Errorf("%w: negative destination audit configuration", ErrInvalidRoleConfiguration)
	}
	if cfg.Period == 0 {
		cfg.Period = 30 * time.Second
	}
	if cfg.MaxPending == 0 {
		cfg.MaxPending = 256
	}
	if cfg.MaxMessagesPerSecond == 0 {
		cfg.MaxMessagesPerSecond = 10
	}
	cfg.OnActivation = append([]DestinationStateAuditRequest(nil), cfg.OnActivation...)
	for i := range cfg.OnActivation {
		r := &cfg.OnActivation[i]
		if _, err := buildSSNMParameters(r.Scope, r.Destinations, r.Info); err != nil {
			return 0, nil, err
		}
		r.Scope = r.Scope.clone()
		r.Destinations = append([]PointCodeRange(nil), r.Destinations...)
	}
	return after, &cfg, nil
}

type receivedCongestionKey struct {
	association AssociationID
	as          ASKey
	scope       string
	destination ssnmDestinationKey
}

func receivedCongestionBytes(record SSNMCongestion) int {
	return ssnmScopeBytes(record.Scope, []ASKey{record.AS})
}

// applyReceivedCongestionLocked keeps independently bounded source history.
// Newest covering statements are selected before freshness, so expired narrow
// reports cannot resurrect superseded broad reports from the same source.
func (s *ssnmState) applyReceivedCongestionLocked(state *ssnmPartitionState, report SSNMReport, resolvedAS []ASKey) error {
	keys := make([]ssnmDestinationKey, 0, len(report.Destinations))
	for _, d := range report.Destinations {
		keys = append(keys, ssnmDestinationKeyFor(d))
	}
	slices.SortFunc(keys, compareSSNMDestinationKeys)
	keys = slices.Compact(keys)
	resolved := append([]ASKey(nil), resolvedAS...)
	state.work.copyAS(len(resolved))
	if len(resolved) == 0 {
		key := ASKey{NetworkAppearance: report.Scope.NetworkAppearance, NetworkAppearanceSet: report.Scope.NetworkAppearanceSet}
		if report.Scope.RoutingContextSet && len(report.Scope.RoutingContexts) == 1 {
			key.RoutingContext = report.Scope.RoutingContexts[0]
			key.RoutingContextSet = true
		}
		resolved = append(resolved, key)
	}
	slices.SortFunc(resolved, compareASKey)
	resolved = slices.Compact(resolved)
	if len(keys) > 0 && len(resolved) > s.limits.MaxRecordsPerPartition/len(keys) {
		maximumInt := int(^uint(0) >> 1)
		refused := maximumInt
		if len(resolved) <= maximumInt/len(keys) {
			refused = len(resolved) * len(keys)
		}
		return s.refuseReportLocked(refused, "resolved congestion source report exceeds partition record limit")
	}
	scopeKey := peerCongestionScopeKey(report.Scope)
	now := s.automationNow()
	record := SSNMCongestion{Congested: !report.CongestionLevelSet || report.CongestionLevel != 0, Level: report.CongestionLevel, LevelSet: report.CongestionLevelSet, Source: report.Source, Scope: report.Scope.clone(), Association: report.Association, Epoch: report.Epoch, ReceivedAt: now}
	if record.Congested && s.validity > 0 {
		record.ExpiresAt = now.Add(s.validity)
	}
	added, bytes := 0, 0
	for _, d := range keys {
		if old, held := state.congestion[d]; !held {
			added++
			bytes += ssnmScopeBytes(record.Scope, resolved)
		} else {
			bytes += ssnmScopeBytes(record.Scope, resolved) - ssnmScopeBytes(old.Scope, old.resolved)
		}
		for _, as := range resolved {
			old, held := state.receivedCongestion[receivedCongestionKey{association: report.Association, as: as, scope: scopeKey, destination: d}]
			if !held {
				added++
			} else {
				bytes -= receivedCongestionBytes(old)
			}
			bytes += receivedCongestionBytes(record)
		}
	}
	if err := s.admitLocked(state, report.Partition.peerKey(), added, bytes, report); err != nil {
		return err
	}
	if state.receivedCongestion == nil {
		state.receivedCongestion = make(map[receivedCongestionKey]SSNMCongestion)
	}
	record.Revision = s.nextRevisionLocked()
	report.Revision = record.Revision
	for _, d := range keys {
		for _, as := range resolved {
			record.AS = as
			state.setReceivedCongestion(receivedCongestionKey{association: report.Association, as: as, scope: scopeKey, destination: d}, record)

		}
		state.congestion[d] = ssnmCongestionRecord{SSNMCongestion: record, resolved: resolved}
	}
	s.records += added
	s.peerRecords[report.Partition.peerKey()] += added
	s.bytes += bytes
	state.bytes += bytes
	event := SSNMEvent{Kind: SSNMReportEvent, Revision: record.Revision, Partition: report.Partition, Epoch: report.Epoch, Report: report, ReportSet: true}
	if len(s.subscribers) > 0 {
		for _, d := range keys {
			event.Updated = append(event.Updated, state.retainedKnowledge(d))
		}
	}
	s.publishLocked(event)
	return nil
}

func (s *ssnmState) automationNow() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// A candidate carries the report version and partition epoch, not permission
// to expire it. A fresh SCON or lifecycle change can invalidate it after the
// deadline scan and before the short commit (RFC 4666 §3.4.4).
type congestionExpiry struct {
	partition       SSNMPartition
	epoch, revision uint64
	received        receivedCongestionKey
	peer            peerCongestionKey
	peerRecord      bool
}

func (s *ssnmState) congestionExpiries(now time.Time) []congestionExpiry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.validity == 0 {
		return nil
	}
	var due []congestionExpiry
	for partition, state := range s.partitions {
		for key, record := range state.receivedCongestion {
			if record.Freshness == CongestionFresh && !record.ExpiresAt.IsZero() && !now.Before(record.ExpiresAt) {
				due = append(due, congestionExpiry{partition: partition, epoch: state.epoch, revision: record.Revision, received: key})
			}
		}
		for key, record := range state.peerCongestion {
			if record.Freshness == CongestionFresh && !record.ExpiresAt.IsZero() && !now.Before(record.ExpiresAt) {
				due = append(due, congestionExpiry{partition: partition, epoch: state.epoch, revision: record.Revision, peer: key, peerRecord: true})
			}
		}
	}
	return due
}

// Keep the single-candidate version fence available to callers that captured a
// statement before a newer report or partition generation was committed.
func (s *ssnmState) expireCongestionStatement(expiry congestionExpiry, now time.Time) bool {
	changed := make(map[SSNMPartition]bool)
	s.expireCongestionBatch([]congestionExpiry{expiry}, now, changed)
	return changed[expiry.partition]
}

// A chunk commits freshness and publishes its identities under the same store
// lock. Snapshots can never observe an unpublished freshness change. The lock
// is released between chunks, without joining callbacks or transport work.
func (s *ssnmState) expireCongestionBatch(expiries []congestionExpiry, now time.Time, changed map[SSNMPartition]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	event := SSNMEvent{Kind: SSNMCongestionExpiredEvent}
	for _, expiry := range expiries {
		identity, accepted := s.expireCongestionLocked(expiry, now)
		if !accepted {
			continue
		}
		event.Expired = append(event.Expired, identity)
		if !expiry.peerRecord {
			changed[expiry.partition] = true
		}
	}
	if len(event.Expired) > 0 {
		event.Revision = s.nextRevisionLocked()
		s.publishLocked(event)
	}
}

func (s *ssnmState) expireCongestionLocked(expiry congestionExpiry, now time.Time) (SSNMCongestionExpiry, bool) {
	state := s.partitions[expiry.partition]
	if state == nil || state.epoch != expiry.epoch {
		return SSNMCongestionExpiry{}, false
	}
	if expiry.peerRecord {
		record, held := state.peerCongestion[expiry.peer]
		if !held || record.Revision != expiry.revision || record.Freshness != CongestionFresh || record.ExpiresAt.IsZero() || now.Before(record.ExpiresAt) {
			return SSNMCongestionExpiry{}, false
		}
		record.Freshness = CongestionExpired
		state.peerCongestion[expiry.peer] = record
		return SSNMCongestionExpiry{Epoch: state.epoch, Destination: record.Destination, AS: record.AS, Revision: record.Revision, Peer: true}, true
	}
	record, held := state.receivedCongestion[expiry.received]
	if !held || record.Revision != expiry.revision || record.Freshness != CongestionFresh || record.ExpiresAt.IsZero() || now.Before(record.ExpiresAt) {
		return SSNMCongestionExpiry{}, false
	}
	record.Freshness = CongestionExpired
	state.setReceivedCongestion(expiry.received, record)
	if current := state.congestion[expiry.received.destination]; current.Revision == record.Revision && current.AS == record.AS {
		state.congestion[expiry.received.destination] = ssnmCongestionRecord{SSNMCongestion: record, resolved: current.resolved}
	}
	return SSNMCongestionExpiry{Epoch: state.epoch, Destination: expiry.received.destination.pointCodeRange(), AS: record.AS, Revision: record.Revision}, true
}

func (s *ssnmState) expireCongestion(now time.Time, stop ...<-chan struct{}) []SSNMPartition {
	changed := make(map[SSNMPartition]bool)
	due := s.congestionExpiries(now)
	for start := 0; start < len(due); start += ssnmExpiryBatchSize {
		if len(stop) > 0 {
			select {
			case <-stop[0]:
				goto finished
			default:
			}
		}
		s.expireCongestionBatch(due[start:min(start+ssnmExpiryBatchSize, len(due))], now, changed)
	}
finished:
	var partitions []SSNMPartition
	for partition := range changed {
		partitions = append(partitions, partition)
	}
	sortSSNMPartitions(partitions)
	return partitions
}

// A standalone partition shares storage, never received-congestion authority,
// between an association's ASes. Canonical partitions already identify the AS;
// its SGPs may legitimately use different RC labels (RFC 4666 §§1.2, 1.4.2.1).
func receivedCongestionApplies(record SSNMCongestion, key ASKey, standalone bool) bool {
	if standalone && (record.AS.RoutingContextSet != key.RoutingContextSet || record.AS.RoutingContext != key.RoutingContext) {
		return false
	}
	appearance, set := record.AS.NetworkAppearance, record.AS.NetworkAppearanceSet
	if record.Scope.NetworkAppearanceSet {
		appearance, set = record.Scope.NetworkAppearance, true
	}
	if key.NetworkAppearanceSet {
		return set && appearance == key.NetworkAppearance
	}
	return !standalone || !set
}
