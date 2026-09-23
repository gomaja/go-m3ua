package main

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// retainedHeapTolerance is the section 4 retained-heap allowance over the
// warmed baseline: max(8 MiB, 5% of the baseline). Five percent is rounded
// down, so the allowance is never larger than the contract states.
func retainedHeapTolerance(baseline uint64) uint64 {
	return max(retainedAbsoluteSlack, baseline*retainedPercentSlack/100)
}

func retainedHeapLimit(baseline uint64) uint64 {
	return baseline + retainedHeapTolerance(baseline)
}

func formatMiB(bytes uint64) string {
	return fmt.Sprintf("%.2f MiB (%d B)", float64(bytes)/float64(mebibyte), bytes)
}

// evaluateCeiling holds every sample to one limit. No samples is no evidence,
// never a pass.
func evaluateCeiling(id, contract, requirement string, values []uint64, limit uint64) criterion {
	result := criterion{ID: id, Contract: contract, Requirement: requirement}
	if len(values) == 0 {
		result.Status = statusNotEvaluated
		result.Observed = "no samples"
		return result
	}
	peak := values[0]
	for _, value := range values[1:] {
		peak = max(peak, value)
	}
	result.Status = statusPass
	if peak > limit {
		result.Status = statusFail
	}
	result.Observed = fmt.Sprintf("max %s over %d samples; limit %s", formatMiB(peak), len(values), formatMiB(limit))
	return result
}

// evaluateFinalBlocks applies the rule that the final checked post-drain
// samples must each meet the retained-heap limit. Earlier blocks are recorded
// but not gated. With fewer than required blocks the rule cannot pass, but a
// final block that exceeds the limit is still a failure.
func evaluateFinalBlocks(baseline uint64, finals []uint64, required, checked int) criterion {
	limit := retainedHeapLimit(baseline)
	result := criterion{ID: "f8.retained-heap", Contract: "F8",
		Requirement: fmt.Sprintf("after churn and drain, two forced GCs: live heap within max(8 MiB, 5%%) of the warmed baseline in each of the final %d of %d blocks", checked, required)}
	start := max(len(finals)-checked, 0)
	var observed []string
	failed := false
	for index := start; index < len(finals); index++ {
		within := finals[index] <= limit
		failed = failed || !within
		observed = append(observed, fmt.Sprintf("block %d %s", index+1, formatMiB(finals[index])))
	}
	result.Observed = fmt.Sprintf("baseline %s, limit %s; %s", formatMiB(baseline), formatMiB(limit), strings.Join(observed, ", "))
	switch {
	case failed:
		result.Status = statusFail
	case len(finals) < required:
		result.Status = statusNotEvaluated
		result.Observed += fmt.Sprintf("; only %d of %d blocks run", len(finals), required)
	default:
		result.Status = statusPass
	}
	return result
}

// evaluateRetention compares one post-drain sample with the baseline: live
// heap within tolerance, identical goroutine and descriptor counts, an
// endpoint registry holding exactly the stable associations, and no owned or
// live kernel association beyond the baseline. Protocol-required transient
// kernel states are reported by the sample but are not live resources.
func evaluateRetention(baseline, sample retainedSample, stable int) (bool, []string) {
	var failures []string
	if limit := retainedHeapLimit(baseline.LiveHeapBytes); sample.LiveHeapBytes > limit {
		failures = append(failures, fmt.Sprintf("live heap %s above %s", formatMiB(sample.LiveHeapBytes), formatMiB(limit)))
	}
	failures = append(failures, resourceFailures(baseline, sample, stable)...)
	return len(failures) == 0, failures
}

func resourceFailures(baseline, sample retainedSample, stable int) []string {
	var failures []string
	if sample.LiveSubscriptions != subscriberCount {
		failures = append(failures, fmt.Sprintf("live subscriptions %d, want %d", sample.LiveSubscriptions, subscriberCount))
	}
	if sample.Goroutines != baseline.Goroutines {
		failures = append(failures, fmt.Sprintf("goroutines %d, baseline %d", sample.Goroutines, baseline.Goroutines))
	}
	switch {
	case sample.FDs.Error != "" || baseline.FDs.Error != "":
		failures = append(failures, fmt.Sprintf("descriptors unavailable: %s%s", sample.FDs.Error, baseline.FDs.Error))
	case sample.FDs.Total != baseline.FDs.Total:
		failures = append(failures, fmt.Sprintf("descriptors %d, baseline %d", sample.FDs.Total, baseline.FDs.Total))
	}
	failures = append(failures, registryFailures(sample, stable)...)
	failures = append(failures, kernelFailures(baseline, sample)...)
	return failures
}

func registryFailures(sample retainedSample, stable int) []string {
	if sample.Associations != stable || len(sample.Unexpected) != 0 || len(sample.MissingStable) != 0 {
		return []string{fmt.Sprintf("endpoint registry holds %d associations (want %d), unexpected %v, missing stable %v",
			sample.Associations, stable, sample.Unexpected, sample.MissingStable)}
	}
	return nil
}

func kernelFailures(baseline, sample retainedSample) []string {
	switch {
	case sample.Kernel.Error != "" || baseline.Kernel.Error != "":
		return []string{fmt.Sprintf("kernel associations unavailable: %s%s", sample.Kernel.Error, baseline.Kernel.Error)}
	case sample.Kernel.OwnedEstablished != baseline.Kernel.OwnedEstablished || len(sample.Kernel.OwnedOther) != 0 ||
		sample.Kernel.unownedEstablished() != 0:
		return []string{fmt.Sprintf("kernel associations owned established %d (baseline %d), owned other %v, unowned established %d",
			sample.Kernel.OwnedEstablished, baseline.Kernel.OwnedEstablished, sample.Kernel.OwnedOther, sample.Kernel.unownedEstablished())}
	}
	return nil
}

// evaluateRun derives every criterion and the verdict from the record. A
// fatal error makes the run invalid whatever the criteria say.
func evaluateRun(record *aspRecord) {
	record.Criteria = []criterion{
		cyclesCriterion(record),
		rateCriterion(record),
		concurrentAcceptsCriterion(record),
		childCloseCriterion(record),
		survivingCriterion(record),
	}
	steadyHeap, steadyRSS, overloadHeap, overloadRSS := splitSeries(record)
	record.Criteria = append(record.Criteria,
		contractLoadCriterion(record),
		evaluateWindow("f8.steady-heap", "F8", "steady state: Go live heap after GC at most 256 MiB (10 s samples, every non-overload phase)", steadyHeap, steadyLiveHeapLimit),
		evaluateWindow("f8.steady-rss", "F8", "steady state: library-process RSS at most 512 MiB (1 s samples, every non-overload phase)", steadyRSS, steadyRSSLimit),
		evaluateWindow("f8.overload-heap", "F8", "bounded overload/full queues: live heap at most 512 MiB", overloadHeap, overloadLiveHeapLimit),
		evaluateWindow("f8.overload-rss", "F8", "bounded overload/full queues: RSS at most 1 GiB", overloadRSS, overloadRSSLimit),
		overloadBoundsCriterion(record),
		postOverloadCriterion(record),
		subscriptionsCriterion(record),
	)
	finals := make([]uint64, len(record.Blocks))
	for index, block := range record.Blocks {
		finals[index] = block.Final.LiveHeapBytes
	}
	record.Criteria = append(record.Criteria,
		evaluateFinalBlocks(record.BaselineFinal.LiveHeapBytes, finals, requiredBlocks, finalBlocksChecked),
		perBlockCriterion(record, "f8.retained-resources", "library-owned descriptors and goroutines return to baseline counts after every block, with all 8 subscriptions live",
			func(sample retainedSample) []string {
				var failures []string
				for _, failure := range resourceFailures(record.BaselineFinal, sample, stableAssociations) {
					if strings.HasPrefix(failure, "goroutines") || strings.HasPrefix(failure, "descriptors") ||
						strings.HasPrefix(failure, "live subscriptions") {
						failures = append(failures, failure)
					}
				}
				return failures
			}),
		perBlockCriterion(record, "f8.retained-registry", "endpoint registries retain no removed associations after every block",
			func(sample retainedSample) []string { return registryFailures(sample, stableAssociations) }),
		perBlockCriterion(record, "f8.retained-kernel", "no extra owned kernel association after teardown; transient kernel states reported separately",
			func(sample retainedSample) []string { return kernelFailures(record.BaselineFinal, sample) }),
		perBlockCriterion(record, "f8.retain-window", "retained samples taken within 60 s of complete drain, after two forced GCs",
			func(sample retainedSample) []string {
				if time.Duration(sample.SinceDrainMillis)*time.Millisecond > retainWindow {
					return []string{fmt.Sprintf("sample %d ms after drain", sample.SinceDrainMillis)}
				}
				return nil
			}),
		blocksCriterion(record),
	)
	record.Verdict = verdictFor(record)
}

func verdictFor(record *aspRecord) string {
	if record.Error != "" {
		return verdictInvalid
	}
	verdict := verdictPass
	for _, item := range record.Criteria {
		switch item.Status {
		case statusFail:
			return verdictFail
		case statusNotEvaluated:
			verdict = verdictIncomplete
		}
	}
	return verdict
}

// seriesWindow is one gated window of a sampled series: the values of every
// sample that could be taken, how many could not, and every phase whose
// coverage fell short of its sampling interval.
type seriesWindow struct {
	Values  []uint64
	Errored int
	Gaps    []string
}

// splitSeries divides the RSS and live-heap series into the steady window
// (every non-overload phase) and the overload window. An errored sample is
// counted, not dropped silently, and each phase must hold at least one sample
// per sampling interval, less one for the phase boundaries: an unsampled
// stretch could hide the peak a ceiling exists to catch.
func splitSeries(record *aspRecord) (steadyHeap, steadyRSS, overloadHeap, overloadRSS seriesWindow) {
	rssCounts, heapCounts := map[string]int{}, map[string]int{}
	for _, sample := range record.RSSSeries {
		window := &steadyRSS
		if overloadPhase(sample.Phase) {
			window = &overloadRSS
		}
		if sample.Error != "" {
			window.Errored++
			continue
		}
		window.Values = append(window.Values, sample.RSSBytes)
		rssCounts[sample.Phase]++
	}
	for _, sample := range record.HeapSeries {
		if sample.BeforeFirstGC {
			// No collection has run, so there is no post-GC live heap yet.
			continue
		}
		window := &steadyHeap
		if overloadPhase(sample.Phase) {
			window = &overloadHeap
		}
		if sample.Error != "" || sample.LiveHeapBytes == 0 {
			window.Errored++
			continue
		}
		window.Values = append(window.Values, sample.LiveHeapBytes)
		heapCounts[sample.Phase]++
	}
	for _, mark := range record.Phases {
		duration := time.Duration(mark.EndMillis-mark.StartMillis) * time.Millisecond
		rssWindow, heapWindow := &steadyRSS, &steadyHeap
		if overloadPhase(mark.Name) {
			rssWindow, heapWindow = &overloadRSS, &overloadHeap
		}
		if want := int(duration/rssInterval) - 1; rssCounts[mark.Name] < want {
			rssWindow.Gaps = append(rssWindow.Gaps, fmt.Sprintf("phase %s: %d RSS samples in %s, expected at least %d",
				mark.Name, rssCounts[mark.Name], duration, want))
		}
		if want := int(duration/heapInterval) - 1; heapCounts[mark.Name] < want {
			heapWindow.Gaps = append(heapWindow.Gaps, fmt.Sprintf("phase %s: %d heap samples in %s, expected at least %d",
				mark.Name, heapCounts[mark.Name], duration, want))
		}
	}
	return steadyHeap, steadyRSS, overloadHeap, overloadRSS
}

// evaluateWindow holds a window to a ceiling. A sample above the limit fails
// the window whatever else is missing; otherwise an errored sample or a
// coverage gap leaves it not evaluated, because what was not measured cannot
// be shown to be within the limit.
func evaluateWindow(id, contract, requirement string, window seriesWindow, limit uint64) criterion {
	result := evaluateCeiling(id, contract, requirement, window.Values, limit)
	if result.Status == statusFail {
		return result
	}
	var missing []string
	if window.Errored != 0 {
		missing = append(missing, fmt.Sprintf("%d samples could not be taken", window.Errored))
	}
	missing = append(missing, window.Gaps...)
	if len(missing) != 0 {
		result.Status = statusNotEvaluated
		result.Observed += "; " + strings.Join(missing, "; ")
	}
	return result
}

// scheduleTolerance is how far below its open-loop schedule one epoch's sent
// volume may end: 0.5% of the schedule plus 10 ms of offered traffic, which
// covers the sender's wake granularity and the instant it is stopped. A
// sender that falls further behind did not offer the load.
func scheduleTolerance(scheduled uint64, rate float64) uint64 {
	return scheduled/200 + uint64(math.Ceil(rate*0.010))
}

func cyclesCriterion(record *aspRecord) criterion {
	result := criterion{ID: "f7.cycles", Contract: "F7",
		Requirement: fmt.Sprintf("%d complete establish/activate/close cycles, none failed", requiredChurnCycles)}
	attempted, completed, failed := 0, 0, 0
	for _, block := range record.Blocks {
		attempted += block.Peer.Attempted
		completed += block.Peer.Completed
		failed += block.Peer.Failed
	}
	result.Observed = fmt.Sprintf("attempted %d, completed %d, failed %d over %d blocks", attempted, completed, failed, len(record.Blocks))
	switch {
	case failed != 0 || completed != attempted || attempted != record.Config.totalCycles() || len(record.Blocks) != record.Config.Blocks:
		result.Status = statusFail
	case completed < requiredChurnCycles:
		result.Status = statusNotEvaluated
		result.Observed += fmt.Sprintf("; the contract requires %d", requiredChurnCycles)
	default:
		result.Status = statusPass
	}
	return result
}

func rateCriterion(record *aspRecord) criterion {
	result := criterion{ID: "f7.rate", Contract: "F7",
		Requirement: "open-loop start rate of 4 cycles/s; every release within one cycle interval (1/rate) of its plan"}
	period := 1000 / record.Config.ChurnRate
	lateness, lowest, highest := 0.0, 0.0, 0.0
	for index, block := range record.Blocks {
		lateness = max(lateness, block.Peer.MaxStartLatenessMillis)
		if index == 0 || block.Peer.AchievedRate < lowest {
			lowest = block.Peer.AchievedRate
		}
		highest = max(highest, block.Peer.AchievedRate)
	}
	result.Observed = fmt.Sprintf("configured %.3g cycles/s in groups of %d; achieved %.3f–%.3f cycles/s; max start lateness %.1f ms (cycle interval %.0f ms)",
		record.Config.ChurnRate, record.Config.ChurnGroup, lowest, highest, lateness, period)
	switch {
	case len(record.Blocks) == 0:
		result.Status = statusNotEvaluated
	case lateness >= period:
		result.Status = statusFail
	case record.Config.ChurnRate != requiredChurnRate:
		result.Status = statusNotEvaluated
	default:
		result.Status = statusPass
	}
	return result
}

func concurrentAcceptsCriterion(record *aspRecord) criterion {
	result := criterion{ID: "f7.concurrent-accepts", Contract: "F7",
		Requirement: "establishments overlap at both ends, so the listener serves concurrent accepts"}
	peak, listener := 0, 0
	for _, block := range record.Blocks {
		peak = max(peak, block.Peer.MaxEstablishing)
		listener = max(listener, block.ASP.MaxEstablishing)
	}
	result.Observed = fmt.Sprintf("max concurrent establishing %d at the dialing peer and %d in the ASP listener (SCTP accepted, M3UA handshake not yet complete) with %d concurrent Accept calls",
		peak, listener, record.Config.AcceptConcurrency)
	result.Status = statusPass
	if peak < 2 || listener < 2 {
		result.Status = statusFail
	}
	if len(record.Blocks) == 0 {
		result.Status = statusNotEvaluated
	}
	return result
}

func childCloseCriterion(record *aspRecord) criterion {
	result := criterion{ID: "f7.child-close", Contract: "F7",
		Requirement: "every accepted churn child released independently (graceful, abrupt and peer-initiated closes), none left open"}
	modes := map[string]int{}
	accepted, released, failed, open, peerCompleted := 0, 0, 0, 0, 0
	for _, block := range record.Blocks {
		accepted += block.ASP.Accepted
		released += block.ASP.Released
		failed += block.ASP.Failed
		open += block.ASP.StillOpen
		peerCompleted += block.Peer.Completed
		for mode, count := range block.ASP.ByMode {
			modes[mode] += count
		}
	}
	result.Observed = fmt.Sprintf("accepted %d, released %d, failed %d, still open %d, peer-completed %d, by mode %v",
		accepted, released, failed, open, peerCompleted, modes)
	switch {
	case len(record.Blocks) == 0:
		result.Status = statusNotEvaluated
	case failed != 0 || open != 0 || released != accepted || accepted != peerCompleted ||
		modes[closeASPGraceful.String()] == 0 || modes[closeASPAbrupt.String()] == 0 || modes[closePeer.String()] == 0:
		result.Status = statusFail
	default:
		result.Status = statusPass
	}
	return result
}

func survivingCriterion(record *aspRecord) criterion {
	result := criterion{ID: "f7.surviving-no-loss", Contract: "F7",
		Requirement: "no DATA loss, duplication, reordering or write failure on the surviving stable associations, which never end"}
	var problems []string
	var delivered uint64
	check := func(name string, ledger ledgerResult) {
		delivered += ledger.UniqueTotal
		if !ledger.lossFree() {
			problems = append(problems, fmt.Sprintf("%s %s: sent %d unique %d missing %d gaps %d late %d excess %d invalid %d write errors %d",
				name, ledger.Direction, ledger.SentTotal, ledger.UniqueTotal, ledger.Missing, ledger.Gaps, ledger.Late,
				ledger.Excess, ledger.Invalid, ledger.WriteErrors))
		}
	}
	for _, ledger := range record.Steady {
		check("steady", ledger)
	}
	// record.StableEnded lists every end in the run; the per-block counts are
	// the subset during churn, so the larger of the two is the total.
	blockEnded := 0
	for _, block := range record.Blocks {
		if len(block.Ledgers) == 0 {
			problems = append(problems, fmt.Sprintf("block %d has no ledger", block.Block))
		}
		for _, ledger := range block.Ledgers {
			check(fmt.Sprintf("block %d", block.Block), ledger)
		}
		blockEnded += block.StableEnded
	}
	ended := max(len(record.StableEnded), blockEnded)
	if ended != 0 {
		problems = append(problems, fmt.Sprintf("%d stable association ends", ended))
	}
	result.Observed = fmt.Sprintf("%d ledgered deliveries", delivered)
	switch {
	case len(problems) != 0:
		result.Status = statusFail
		result.Observed += "; " + strings.Join(problems, "; ")
	case len(record.Blocks) == 0:
		result.Status = statusNotEvaluated
	default:
		result.Status = statusPass
	}
	return result
}

func overloadBoundsCriterion(record *aspRecord) criterion {
	overload := record.Overload
	result := criterion{ID: "f8.overload-bounds", Contract: "F8",
		Requirement: "bounded queues filled with 4,096-byte payloads and never beyond their caps: 1,024-message DATA queues, 256-event subscriptions (continuity loss, then resync), 16,384 records; after drain every flood message is received or counted discarded; no OOM"}
	var problems []string
	if overload.Error != "" {
		problems = append(problems, overload.Error)
	}
	if overload.PeerWriteErrors != 0 {
		problems = append(problems, fmt.Sprintf("the peer flood failed %d writes: %s", overload.PeerWriteErrors, overload.PeerFirstWriteError))
	}
	if overload.QueuedSamples == 0 || overload.FullAssociations != stableAssociations || overload.MaxQueued != dataQueueSize ||
		overload.QueueCapacity != dataQueueSize || overload.Discarded == 0 {
		problems = append(problems, fmt.Sprintf("DATA queues: %d of %d full, max queued %d of capacity %d over %d samples, discarded %d",
			overload.FullAssociations, stableAssociations, overload.MaxQueued, overload.QueueCapacity, overload.QueuedSamples, overload.Discarded))
	}
	if overload.Received+overload.Discarded != overload.PeerSent {
		problems = append(problems, fmt.Sprintf("delivery not reconciled: received %d + discarded %d != peer sent %d",
			overload.Received, overload.Discarded, overload.PeerSent))
	}
	if len(overload.Subscribers) != subscriberCount {
		problems = append(problems, fmt.Sprintf("%d subscribers observed", len(overload.Subscribers)))
	}
	for _, subscriber := range overload.Subscribers {
		if !subscriber.LossObserved || subscriber.DeliveredBeforeLoss > subscriptionQueueSize || !subscriber.Resynced {
			problems = append(problems, fmt.Sprintf("subscriber %d: loss %t after %d events, resynced %t",
				subscriber.Subscriber, subscriber.LossObserved, subscriber.DeliveredBeforeLoss, subscriber.Resynced))
		}
	}
	if overload.StateRecords > stateRecords {
		problems = append(problems, fmt.Sprintf("%d state records", overload.StateRecords))
	}
	if overload.MaxMTPIndicationQueue > mtpIndicationQueueSize {
		problems = append(problems, fmt.Sprintf("MTP indication queue %d", overload.MaxMTPIndicationQueue))
	}
	if overload.OOMKills != 0 {
		problems = append(problems, fmt.Sprintf("%d OOM kills", overload.OOMKills))
	}
	result.Observed = fmt.Sprintf("%d/%d DATA queues full at %d, %d discarded, %d state records, OOM kills %d",
		overload.FullAssociations, stableAssociations, overload.MaxQueued, overload.Discarded, overload.StateRecords, overload.OOMKills)
	if overload.OOMError != "" {
		result.Observed += " (OOM counter: " + overload.OOMError + ")"
	}
	result.Status = statusPass
	if len(problems) != 0 {
		result.Status = statusFail
		result.Observed += "; " + strings.Join(problems, "; ")
	}
	return result
}

func perBlockCriterion(record *aspRecord, id, requirement string, failures func(retainedSample) []string) criterion {
	result := criterion{ID: id, Contract: "F8", Requirement: requirement}
	var problems []string
	for _, block := range record.Blocks {
		for _, failure := range failures(block.Final) {
			problems = append(problems, fmt.Sprintf("block %d: %s", block.Block, failure))
		}
	}
	switch {
	case len(problems) != 0:
		result.Status = statusFail
		result.Observed = strings.Join(problems, "; ")
	case len(record.Blocks) == 0:
		result.Status = statusNotEvaluated
		result.Observed = "no blocks"
	default:
		result.Status = statusPass
		result.Observed = fmt.Sprintf("%d blocks at baseline: %d goroutines, %d descriptors, %d owned kernel associations",
			len(record.Blocks), record.BaselineFinal.Goroutines, record.BaselineFinal.FDs.Total, record.BaselineFinal.Kernel.OwnedEstablished)
	}
	return result
}

func blocksCriterion(record *aspRecord) criterion {
	result := criterion{ID: "f8.blocks", Contract: "F8", Requirement: fmt.Sprintf("at least %d churn blocks with full time series", requiredBlocks),
		Observed: fmt.Sprintf("%d blocks, %d RSS and %d heap samples", len(record.Blocks), len(record.RSSSeries), len(record.HeapSeries))}
	result.Status = statusPass
	if len(record.Blocks) < requiredBlocks {
		result.Status = statusNotEvaluated
	}
	return result
}

// contractLoadCriterion holds the stable associations to section 4's load:
// "Use mixed traffic at 50% of its target except where a row specifies
// otherwise." The churn row sets churn, not traffic, so the section 2
// deterministic mix runs at half its 40,000/s target through steady and every
// churn block. Each epoch must deliver its open-loop schedule within
// scheduleTolerance, with no write failure. A transport refusal fails it too:
// the resend keeps the ledger whole, but section 4 counts "concealed upstream
// throttling" against a fixed-load trial, and a refused send is exactly that.
func contractLoadCriterion(record *aspRecord) criterion {
	result := criterion{ID: "f7.contract-load", Contract: "F7/F8",
		Requirement: "section 2 deterministic mix at 50% of 40,000/s (20,000 msg/s aggregate) on the stable associations through steady and every churn block; each epoch delivers its schedule within 0.5% + 10 ms of offered traffic with no write failure and no transport refusal"}
	var problems []string
	var scheduled, sent, refused uint64
	epochs := 0
	check := func(name string, ledgers []ledgerResult) {
		epochs++
		if len(ledgers) == 0 {
			problems = append(problems, name+": no ledger")
		}
		for _, ledger := range ledgers {
			scheduled += ledger.Scheduled
			sent += ledger.SentTotal
			refused += ledger.Refused
			tolerance := scheduleTolerance(ledger.Scheduled, ledger.Rate)
			switch {
			case ledger.Scheduled == 0:
				problems = append(problems, fmt.Sprintf("%s %s: nothing scheduled", name, ledger.Direction))
			case ledger.SentTotal+tolerance < ledger.Scheduled:
				problems = append(problems, fmt.Sprintf("%s %s: sent %d of %d scheduled at %.0f/s (tolerance %d)",
					name, ledger.Direction, ledger.SentTotal, ledger.Scheduled, ledger.Rate, tolerance))
			}
			if ledger.WriteErrors != 0 || ledger.Refused != 0 {
				problems = append(problems, fmt.Sprintf("%s %s: %d write errors, %d transport refusals",
					name, ledger.Direction, ledger.WriteErrors, ledger.Refused))
			}
		}
	}
	check("steady", record.Steady)
	for _, block := range record.Blocks {
		check(fmt.Sprintf("block %d", block.Block), block.Ledgers)
	}
	result.Observed = fmt.Sprintf("configured %s at %d msg/s forward and %d msg/s reverse; %d epochs, %d of %d scheduled messages sent, %d refusals",
		record.Config.Payload, record.Config.DataRate, record.Config.ReverseRate, epochs, sent, scheduled, refused)
	switch {
	case len(problems) != 0:
		result.Status = statusFail
		result.Observed += "; " + strings.Join(problems, "; ")
	case record.Config.DataRate != contractDataRate || record.Config.Payload != string(workloadMix):
		result.Status = statusNotEvaluated
		result.Observed += "; not the contract load"
	case len(record.Blocks) == 0:
		result.Status = statusNotEvaluated
	default:
		result.Status = statusPass
	}
	return result
}

// postOverloadCriterion gates the sample taken after the overload has
// drained exactly like a block's retained sample, so retention the overload
// leaves behind is attributed to it rather than to the first churn block.
func postOverloadCriterion(record *aspRecord) criterion {
	result := criterion{ID: "f8.post-overload-retained", Contract: "F8",
		Requirement: "after the overload drains, two forced GCs: live heap within max(8 MiB, 5%) of the baseline, goroutines, descriptors, registry, kernel associations and subscriptions at baseline"}
	sample := record.PostOverload
	if sample.Attempt == 0 {
		result.Status, result.Observed = statusNotEvaluated, "no post-overload sample"
		return result
	}
	pass, failures := evaluateRetention(record.BaselineFinal, sample, stableAssociations)
	result.Observed = fmt.Sprintf("live heap %s (baseline %s), %d goroutines, %d descriptors, %d ms after drain",
		formatMiB(sample.LiveHeapBytes), formatMiB(record.BaselineFinal.LiveHeapBytes), sample.Goroutines, sample.FDs.Total, sample.SinceDrainMillis)
	result.Status = statusPass
	if !pass {
		result.Status = statusFail
		result.Observed += "; " + strings.Join(failures, "; ")
	}
	return result
}

// subscriptionsCriterion requires the 8 subscriptions to stay healthy for the
// whole run: no terminal error, no failed resync, continuity loss only in the
// overload phases that cause it on purpose, a resync for every loss, and all
// 8 live at the baseline.
func subscriptionsCriterion(record *aspRecord) criterion {
	result := criterion{ID: "f8.subscriptions", Contract: "F8",
		Requirement: "8 subscriptions healthy throughout: continuity loss only during the overload, each followed by a successful resync, no terminal error, all 8 live at the baseline"}
	var problems []string
	if len(record.Subscribers) != subscriberCount {
		problems = append(problems, fmt.Sprintf("%d subscribers recorded", len(record.Subscribers)))
	}
	var events uint64
	for _, subscriber := range record.Subscribers {
		events += subscriber.Events
		if subscriber.TerminalError != "" {
			problems = append(problems, fmt.Sprintf("subscriber %d ended: %s", subscriber.Subscriber, subscriber.TerminalError))
		}
		if subscriber.ResyncErrorText != "" {
			problems = append(problems, fmt.Sprintf("subscriber %d resync failed: %s", subscriber.Subscriber, subscriber.ResyncErrorText))
		}
		for phase, losses := range subscriber.LossByPhase {
			if losses != 0 && !overloadPhase(phase) {
				problems = append(problems, fmt.Sprintf("subscriber %d lost continuity %d times in phase %s", subscriber.Subscriber, losses, phase))
			}
		}
		if subscriber.Resyncs != subscriber.ContinuityLoss {
			problems = append(problems, fmt.Sprintf("subscriber %d: %d continuity losses, %d resyncs",
				subscriber.Subscriber, subscriber.ContinuityLoss, subscriber.Resyncs))
		}
	}
	baselines := append([]retainedSample{record.BaselineFinal}, record.Baseline...)
	for _, sample := range baselines {
		if sample.LiveSubscriptions != subscriberCount {
			problems = append(problems, fmt.Sprintf("baseline sample %d: %d live subscriptions", sample.Attempt, sample.LiveSubscriptions))
			break
		}
	}
	result.Observed = fmt.Sprintf("%d subscribers, %d events", len(record.Subscribers), events)
	result.Status = statusPass
	if len(problems) != 0 {
		sort.Strings(problems)
		result.Status = statusFail
		result.Observed += "; " + strings.Join(problems, "; ")
	}
	return result
}
