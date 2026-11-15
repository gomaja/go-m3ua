package m3ua

import (
	"slices"

	"github.com/gomaja/go-m3ua/messages/params"
)

// Received destination knowledge uses the peer registration direction even
// on an IPSP Double Exchange association. Capture under the existing tracker
// prevents a half-read scope/version pair; an open change gate is discarded,
// never joined by this receive path (RFC 4666 §§3.4.1, 4.4.2).
type receivedSSNMAdmission struct {
	forget              receivedForgetFence
	forgetOnly          bool
	active              bool
	epoch               uint64
	knowledgeGeneration uint64
	valid               bool
	resolution          ssnmDestinationRefusal
	keys                []ASKey
	bindings            []queueCongestionBinding
	bindingPartitions   []SSNMPartition
	partitions          []SSNMPartition
	nationalByBinding   []bool
}

func (c *Association) captureReceivedSSNMAdmission(scope WireScope) receivedSSNMAdmission {
	admission := receivedSSNMAdmission{epoch: c.Epoch(), active: true}
	tracker := &c.sconSubmissions
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	admission.knowledgeGeneration = tracker.knowledgeGeneration.Load()
	admission.forget = receivedForgetFence{generation: &tracker.knowledgeGeneration, version: admission.knowledgeGeneration}
	if tracker.optionalChanging != nil {
		return admission
	}
	for id := range tracker.changing {
		if id.local {
			continue
		}
		if !scope.RoutingContextSet || slices.Contains(scope.RoutingContexts, id.routingContext) {
			return admission
		}
	}
	admission.resolution = c.captureSSNMDestinationRefusal(scope, false)
	admission.keys = append([]ASKey(nil), admission.resolution.keys...)
	if len(admission.keys) == 0 {
		admission.keys = c.ssnmASKeys(scope)
	}
	slices.SortFunc(admission.keys, compareASKey)
	admission.keys = slices.Compact(admission.keys)
	admission.partitions = c.ssnmPartitionsForResolution(scope, admission.resolution)
	for _, key := range admission.keys {
		admission.bindingPartitions = append(admission.bindingPartitions, c.ssnmPartitionFor(key))
		admission.nationalByBinding = append(admission.nationalByBinding, c.congestionProfile(key).Method == CongestionNationalWithPriorities)
	}
	var work *ssnmWorkCount
	if store := c.ssnmStore(); store != nil {
		work = store.work
	}
	static := newReceivedStaticASIndex(c.staticallyConfiguredASKeys(), work)
	contextless := !scope.RoutingContextSet && c.hasStaticallyConfiguredContextlessAS()
	c.muDynamicASKeys.RLock()
	defer c.muDynamicASKeys.RUnlock()
	for _, key := range admission.keys {
		binding := queueCongestionBinding{key: normalizeOutboundASKey(key), epoch: admission.epoch, local: false}
		if dynamic, held := c.dynamicPeerASKeys[key.RoutingContext]; key.RoutingContextSet && held {
			if key.NetworkAppearanceSet && dynamic.NetworkAppearanceSet && key.NetworkAppearance != dynamic.NetworkAppearance {
				return admission
			}
			binding.key, binding.version = dynamic, c.dynamicPeerASKeyVersions[key.RoutingContext]
		} else if key.RoutingContextSet {
			candidate, found := static.find(key, work)
			if found {
				binding.key = candidate
			}
			if !found {
				return admission
			}
		} else if !contextless {
			return admission
		} else {
			binding.key = c.contextlessASKey(false)
		}
		admission.bindings = append(admission.bindings, binding)
	}
	admission.valid = len(admission.bindings) > 0 && c.Epoch() == admission.epoch
	return admission
}

// Final lock order: tracker -> peer dynamic bindings -> legacy destination
// store or canonical SSNM store (inside callback). Neither RKM mutex is acquired.
// Callers release this fence before projections, callbacks or transport.
// Registration changes release tracker/dynamic locks before joining cancelled
// submissions; caller-held RKM correlation/lifecycle locks may remain held.
func (c *Association) withReceivedSSNMAdmission(admission receivedSSNMAdmission, commit func() error) error {
	if !admission.valid {
		return nil
	}
	tracker := &c.sconSubmissions
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.optionalChanging != nil || tracker.knowledgeGeneration.Load() != admission.knowledgeGeneration || c.Epoch() != admission.epoch || c.terminating.Load() {
		return nil
	}
	select {
	case <-c.done:
		return nil
	default:
	}
	c.muDynamicASKeys.RLock()
	defer c.muDynamicASKeys.RUnlock()
	for _, binding := range admission.bindings {
		if tracker.changing[sconBindingID{binding.key.RoutingContext, false}] != nil || !c.queueCongestionBindingCurrent(binding) {
			return nil
		}
	}
	return commit()
}

func receivedStaticASMatches(static, key ASKey) bool {
	return static.RoutingContextSet == key.RoutingContextSet && static.RoutingContext == key.RoutingContext && (!static.NetworkAppearanceSet || key.NetworkAppearanceSet && static.NetworkAppearance == key.NetworkAppearance)
}
func receivedStaticASPresent(static receivedStaticASIndex, key ASKey, probes ...*ssnmWorkCount) bool {
	var work *ssnmWorkCount
	if len(probes) > 0 {
		work = probes[0]
	}
	_, found := static.find(key, work)
	return found
}

// Capture before legacy route resolution can yield to a lifetime change.
// Every received destination report carries the forget generation. Enabled
// policies also freeze source epoch and registration identity, so those reports
// cannot be relabelled with a replacement registration (RFC 4666 §§3.4.1, 4.4.2).
func (c *Association) receivedReportAdmission(report SSNMReport) receivedSSNMAdmission {
	if c.role == RoleSGP || report.Source != SSNMPeerReport || (!report.retainsAvailability() && !report.retainsCongestion()) {
		return receivedSSNMAdmission{}
	}
	store := c.ssnmStore()
	if store != nil && (store.validity > 0 || store.auditing != nil) {
		return c.captureReceivedSSNMAdmission(report.Scope)
	}
	// The disabled-policy path captures only one atomic generation, without
	// resolving bindings, allocating history or acquiring the submission tracker.
	generation := &c.sconSubmissions.knowledgeGeneration
	return receivedSSNMAdmission{active: true, forgetOnly: true, forget: receivedForgetFence{generation: generation, version: generation.Load()}}
}

func (c *Association) receivedAffectedPointCodes(apc *params.Param, scope WireScope, kind SSNMReportKind, admission receivedSSNMAdmission, peerRefusal ...peerCongestionRefusal) ([]uint32, []uint8, error) {
	if admission.active && apc != nil {
		if count, limit := apc.AffectedPointCodeCount(), c.ssnmAffectedPointCodeLimit(); count > limit {
			return nil, nil, c.refuseOversizedReceivedSSNM(scope, count, limit, kind, admission)
		}
	}
	return c.ssnmAffectedPointCodes(apc, scope, kind, peerRefusal...)
}

func (c *Association) refuseOversizedReceivedSSNM(scope WireScope, count, limit int, _ SSNMReportKind, admission receivedSSNMAdmission) error {
	store := c.ssnmStore()
	if store == nil {
		return ErrSSNMOversizedReport
	}
	if admission.forgetOnly {
		resolution := c.captureSSNMDestinationRefusal(scope, false)
		partitions := c.ssnmPartitionsForResolution(scope, resolution)
		resolution.forget = admission.forget
		err := store.oversizedPrepared(partitions, count, limit, c.ID(), resolution, nil)
		c.projectReceivedChanges(partitions)
		return err
	}
	applied := false
	err := c.withReceivedSSNMAdmission(admission, func() error {
		applied = true
		return store.oversizedPrepared(admission.partitions, count, limit, c.ID(), admission.resolution, &admission.epoch)
	})
	if applied {
		c.projectReceivedChanges(admission.partitions)
	}
	if err == nil {
		return ErrSSNMOversizedReport
	}
	return err
}
