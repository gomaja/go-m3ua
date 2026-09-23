package main

// Phases of the ASP process. Overload ceilings apply to the two overload
// phases; every other phase is held to the steady-state ceilings.
const (
	phaseStartup       = "startup"
	phaseWarm          = "warm"
	phaseSteady        = "steady"
	phaseBaseline      = "baseline"
	phaseOverload      = "overload"
	phaseOverloadDrain = "overload-drain"
	phasePostOverload  = "post-overload"
	phaseChurnPrefix   = "churn-"
	phaseDrainPrefix   = "drain-"
	phaseRetainPrefix  = "retain-"
	phaseShutdown      = "shutdown"
)

func overloadPhase(phase string) bool {
	return phase == phaseOverload || phase == phaseOverloadDrain
}

type phaseMark struct {
	Name        string `json:"name"`
	StartMillis int64  `json:"start_ms"`
	EndMillis   int64  `json:"end_ms"`
}

// rssSample is the one-second series: process RSS and, for peak tracking,
// the heap object bytes at that instant (not a post-GC value).
type rssSample struct {
	AtMillis         int64  `json:"at_ms"`
	Phase            string `json:"phase"`
	RSSBytes         uint64 `json:"rss_bytes"`
	RSSAnonBytes     uint64 `json:"rss_anon_bytes"`
	RSSFileBytes     uint64 `json:"rss_file_bytes"`
	Threads          int    `json:"threads"`
	HeapObjectsBytes uint64 `json:"heap_objects_bytes"`
	Error            string `json:"error,omitempty"`
}

// heapSample is the ten-second series. LiveHeapBytes is the heap marked
// live by the most recent garbage collection, read without forcing one.
type heapSample struct {
	AtMillis           int64         `json:"at_ms"`
	Phase              string        `json:"phase"`
	LiveHeapBytes      uint64        `json:"live_heap_bytes"`
	HeapObjectsBytes   uint64        `json:"heap_objects_bytes"`
	HeapGoalBytes      uint64        `json:"heap_goal_bytes"`
	GCCycles           uint64        `json:"gc_cycles"`
	Goroutines         int           `json:"goroutines"`
	FDs                int           `json:"fds"`
	Classes            memoryClasses `json:"memory_classes"`
	AnonHugePagesBytes uint64        `json:"anon_huge_pages_bytes"`
	Error              string        `json:"error,omitempty"`
}

type fdSnapshot struct {
	Total                  int    `json:"total"`
	Sockets                int    `json:"sockets"`
	SCTPAssociationSockets int    `json:"sctp_association_sockets"`
	SCTPEndpointSockets    int    `json:"sctp_endpoint_sockets"`
	Other                  int    `json:"other"`
	Error                  string `json:"error,omitempty"`
}

// kernelAssociations classifies /proc/net/sctp/assocs of the library
// process's network namespace. An association whose socket is one of this
// process's descriptors is owned; one whose socket is gone is a
// protocol-required transient state (for example SHUTDOWN-ACK-SENT) and is
// reported separately rather than as a live library resource.
type kernelAssociations struct {
	Total            int            `json:"total"`
	OwnedEstablished int            `json:"owned_established"`
	OwnedOther       map[string]int `json:"owned_other,omitempty"`
	UnownedByState   map[string]int `json:"unowned_by_state,omitempty"`
	Error            string         `json:"error,omitempty"`
}

func (kernel kernelAssociations) unownedEstablished() int {
	return kernel.UnownedByState[sctpStateName(sctpStateEstablished)]
}

type retainedSample struct {
	Attempt          int                `json:"attempt"`
	AtMillis         int64              `json:"at_ms"`
	SinceDrainMillis int64              `json:"since_drain_ms"`
	LiveHeapBytes    uint64             `json:"live_heap_bytes"`
	HeapObjectsBytes uint64             `json:"heap_objects_bytes"`
	RSSBytes         uint64             `json:"rss_bytes"`
	RSSAnonBytes     uint64             `json:"rss_anon_bytes"`
	AnonHugePages    uint64             `json:"anon_huge_pages_bytes"`
	Threads          int                `json:"threads"`
	RSSError         string             `json:"rss_error,omitempty"`
	Classes          memoryClasses      `json:"memory_classes"`
	Goroutines       int                `json:"goroutines"`
	FDs              fdSnapshot         `json:"fds"`
	Associations     int                `json:"endpoint_associations"`
	Unexpected       []uint64           `json:"unexpected_association_ids,omitempty"`
	MissingStable    []uint64           `json:"missing_stable_association_ids,omitempty"`
	Kernel           kernelAssociations `json:"kernel_associations"`
	Pass             bool               `json:"meets_retention"`
	Failures         []string           `json:"retention_failures,omitempty"`
	GoroutineProfile string             `json:"goroutine_profile,omitempty"`
}

type ledgerResult struct {
	Direction       string   `json:"direction"`
	Epoch           uint32   `json:"epoch"`
	Sent            []uint64 `json:"sent"`
	Unique          []uint64 `json:"unique"`
	SentTotal       uint64   `json:"sent_total"`
	UniqueTotal     uint64   `json:"unique_total"`
	Missing         uint64   `json:"missing"`
	Gaps            uint64   `json:"gaps"`
	Late            uint64   `json:"duplicate_or_late"`
	Excess          uint64   `json:"excess"`
	Invalid         uint64   `json:"invalid"`
	Stale           uint64   `json:"stale_epoch"`
	WriteErrors     uint64   `json:"write_errors"`
	FirstWriteError string   `json:"first_write_error,omitempty"`
	// Refused counts sends the transport refused for a full send buffer and
	// that were offered again: backpressure, not loss.
	Refused uint64 `json:"refused_then_resent"`
}

func (result ledgerResult) lossFree() bool {
	return result.SentTotal > 0 && result.Missing == 0 && result.Gaps == 0 && result.Late == 0 &&
		result.Excess == 0 && result.Invalid == 0 && result.WriteErrors == 0
}

type churnStats struct {
	Attempted              int            `json:"attempted"`
	Completed              int            `json:"completed"`
	Failed                 int            `json:"failed"`
	FailureReasons         map[string]int `json:"failure_reasons,omitempty"`
	FirstFailure           string         `json:"first_failure,omitempty"`
	ByMode                 map[string]int `json:"completed_by_close_mode,omitempty"`
	MaxInFlight            int            `json:"max_in_flight"`
	MaxEstablishing        int            `json:"max_concurrent_establishing"`
	MaxStartLatenessMillis float64        `json:"max_start_lateness_ms"`
	StartSpanMillis        float64        `json:"start_span_ms"`
	AchievedRate           float64        `json:"achieved_cycles_per_second"`
	EstablishMillis        []float64      `json:"establish_ms,omitempty"`
}

type aspChurnStats struct {
	Accepted       int            `json:"accepted"`
	Released       int            `json:"released"`
	Failed         int            `json:"failed"`
	FailureReasons map[string]int `json:"failure_reasons,omitempty"`
	FirstFailure   string         `json:"first_failure,omitempty"`
	ByMode         map[string]int `json:"released_by_close_mode,omitempty"`
	StillOpen      int            `json:"still_open_after_block"`
	Rejected       int            `json:"establishment_errors"`
}

type blockResult struct {
	Block          int              `json:"block"`
	FirstCycle     int              `json:"first_cycle"`
	Cycles         int              `json:"cycles"`
	Peer           churnStats       `json:"peer"`
	ASP            aspChurnStats    `json:"asp"`
	Ledgers        []ledgerResult   `json:"ledgers"`
	StableEnded    int              `json:"stable_associations_ended"`
	DrainMillis    int64            `json:"drain_ms"`
	Retained       []retainedSample `json:"retained_attempts"`
	Final          retainedSample   `json:"retained_final"`
	PeerError      string           `json:"peer_error,omitempty"`
	TrafficRunning float64          `json:"traffic_seconds"`
}

type subscriberOverload struct {
	Subscriber          int  `json:"subscriber"`
	DeliveredBeforeLoss int  `json:"delivered_before_continuity_loss"`
	LossObserved        bool `json:"continuity_loss_observed"`
	Resynced            bool `json:"resynced"`
}

type overloadResult struct {
	QueueCapacity         int                  `json:"data_queue_capacity"`
	FullAssociations      int                  `json:"associations_full"`
	MaxQueued             int                  `json:"max_queued_observed"`
	QueuedSamples         int                  `json:"queue_samples"`
	Discarded             uint64               `json:"discarded"`
	PeerSent              uint64               `json:"peer_sent_4096"`
	PeerWriteErrors       uint64               `json:"peer_write_errors"`
	PeerFirstWriteError   string               `json:"peer_first_write_error,omitempty"`
	PeerRefused           uint64               `json:"peer_refused_then_resent"`
	Received              uint64               `json:"received_4096_after_resume"`
	SSNMReports           int                  `json:"ssnm_reports_sent"`
	Subscribers           []subscriberOverload `json:"subscribers"`
	StateRecords          int                  `json:"state_records_after"`
	RecordsRefusedDelta   uint64               `json:"records_refused_delta"`
	MaxMTPIndicationQueue int                  `json:"max_mtp_indication_queued"`
	OOMKills              uint64               `json:"oom_kills_delta"`
	OOMError              string               `json:"oom_error,omitempty"`
	DrainMillis           int64                `json:"drain_ms"`
	Error                 string               `json:"error,omitempty"`
}

type subscriberSummary struct {
	Subscriber      int            `json:"subscriber"`
	Events          uint64         `json:"events"`
	ByKind          map[string]int `json:"by_kind"`
	ContinuityLoss  int            `json:"continuity_losses"`
	Resyncs         int            `json:"resyncs"`
	LastRevision    uint64         `json:"last_revision"`
	TerminalError   string         `json:"terminal_error,omitempty"`
	LossByPhase     map[string]int `json:"continuity_losses_by_phase,omitempty"`
	ResyncErrorText string         `json:"resync_error,omitempty"`
}

type storeSummary struct {
	Revision              uint64 `json:"revision"`
	Partitions            int    `json:"partitions"`
	AvailabilityRecords   int    `json:"availability_records"`
	CongestionRecords     int    `json:"congestion_records"`
	RecordsRefused        uint64 `json:"records_refused"`
	ReportsRefused        uint64 `json:"reports_refused"`
	PartitionsInvalidated uint64 `json:"partitions_invalidated"`
	LastResourceLoss      string `json:"last_resource_loss,omitempty"`
}

func (summary storeSummary) records() int {
	return summary.AvailabilityRecords + summary.CongestionRecords
}

type criterion struct {
	ID          string `json:"id"`
	Contract    string `json:"contract"`
	Requirement string `json:"requirement"`
	Status      string `json:"status"`
	Observed    string `json:"observed"`
}

const (
	statusPass         = "pass"
	statusFail         = "fail"
	statusNotEvaluated = "not-evaluated"

	verdictPass       = "pass"
	verdictFail       = "fail"
	verdictIncomplete = "incomplete"
	verdictInvalid    = "invalid"
)

type aspRecord struct {
	Kind          string              `json:"kind"`
	Label         string              `json:"label,omitempty"`
	Verdict       string              `json:"verdict"`
	Error         string              `json:"error,omitempty"`
	Config        commandConfig       `json:"config"`
	Manifest      manifest            `json:"manifest"`
	Phases        []phaseMark         `json:"phases"`
	RSSSeries     []rssSample         `json:"rss_series"`
	HeapSeries    []heapSample        `json:"heap_series"`
	Warm          warmResult          `json:"warm"`
	Steady        []ledgerResult      `json:"steady_ledgers"`
	Baseline      []retainedSample    `json:"baseline_samples"`
	BaselineFinal retainedSample      `json:"baseline"`
	Overload      overloadResult      `json:"overload"`
	PostOverload  retainedSample      `json:"post_overload_sample"`
	Blocks        []blockResult       `json:"blocks"`
	Subscribers   []subscriberSummary `json:"subscribers"`
	MTPIndication indicationSummary   `json:"mtp_indications"`
	StableEnded   []string            `json:"stable_association_ends,omitempty"`
	Store         storeSummary        `json:"store_final"`
	Criteria      []criterion         `json:"criteria"`
	Peer          *peerRecord         `json:"peer,omitempty"`
}

type warmResult struct {
	StableAccepted  int          `json:"stable_accepted"`
	EstablishMillis int64        `json:"establish_ms"`
	PopulateMillis  int64        `json:"populate_ms"`
	PeerMessages    int          `json:"peer_ssnm_messages"`
	PeerErrors      []string     `json:"peer_errors,omitempty"`
	Store           storeSummary `json:"store"`
	Error           string       `json:"error,omitempty"`
}

type indicationSummary struct {
	Received       uint64 `json:"received"`
	ResyncRequired uint64 `json:"resync_required"`
	Capacity       int    `json:"capacity"`
}

type peerRecord struct {
	Kind     string         `json:"kind"`
	Error    string         `json:"error,omitempty"`
	Manifest manifest       `json:"manifest"`
	Stable   int            `json:"stable_associations"`
	Blocks   []churnStats   `json:"blocks"`
	Ledgers  []ledgerResult `json:"ledgers"`
	Events   []string       `json:"events,omitempty"`
}
