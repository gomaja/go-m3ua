package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/gomaja/go-m3ua"
)

// assessedBaselineRevision is the baseline commit of the performance
// campaign; the build stamp records the candidate revision separately.
const assessedBaselineRevision = "d097e191d879efc95e36c0254814933f01aa9aee"

type manifest struct {
	Role                     string            `json:"role"`
	AssessedBaselineRevision string            `json:"assessed_baseline_revision"`
	VCSRevision              string            `json:"vcs_revision"`
	VCSModified              bool              `json:"vcs_modified"`
	VCSTime                  string            `json:"vcs_time,omitempty"`
	GoVersion                string            `json:"go_version"`
	GoOS                     string            `json:"goos"`
	GoArch                   string            `json:"goarch"`
	GOMAXPROCS               int               `json:"gomaxprocs"`
	GOGC                     string            `json:"gogc_env"`
	GOMEMLIMIT               string            `json:"gomemlimit_env"`
	GODEBUG                  string            `json:"godebug_env"`
	MemoryLimitBytes         int64             `json:"runtime_memory_limit_bytes"`
	BuildSettings            map[string]string `json:"build_settings,omitempty"`
	Dependencies             map[string]string `json:"dependencies,omitempty"`
	ImageDigest              string            `json:"image_digest,omitempty"`
	Kernel                   map[string]string `json:"kernel,omitempty"`
	Cgroup                   map[string]string `json:"cgroup,omitempty"`
	// TransparentHugePages is recorded because khugepaged can make memory the
	// runtime has released resident again, which moves RSS without any change
	// in the process (runtime GODEBUG disablethp; go.dev/issue/64332).
	TransparentHugePages map[string]string `json:"transparent_huge_pages,omitempty"`
	Topology             topologyManifest  `json:"topology"`
	Limits               pinnedLimits      `json:"limits"`
	Workload             map[string]string `json:"workload,omitempty"`
	// Transport is recorded once the stable associations are up.
	Transport *transportEvidence `json:"transport,omitempty"`
}

type topologyManifest struct {
	SignallingGateways       int    `json:"signalling_gateways"`
	SGPs                     int    `json:"sgps"`
	ApplicationServersPerSG  int    `json:"application_servers_per_gateway"`
	StableAssociations       int    `json:"stable_associations"`
	ASPerStableAssociation   int    `json:"application_servers_per_stable_association"`
	Partitions               int    `json:"partitions"`
	DestinationsPerPartition int    `json:"destinations_per_partition"`
	StateRecords             int    `json:"state_records"`
	RouteIntersectingRecords int    `json:"route_intersecting_records"`
	Subscribers              int    `json:"subscribers"`
	Routes                   int    `json:"routes"`
	NetworkAppearance        uint32 `json:"network_appearance"`
	Partitioning             string `json:"partitioning"`
}

// pinnedLimits records every bounded queue and accounting cap of the run.
type pinnedLimits struct {
	SSNMState                    m3ua.SSNMStateConfig `json:"ssnm_state"`
	SubscriptionEventCap         int                  `json:"subscription_event_cap"`
	SubscriptionByteCap          int                  `json:"subscription_byte_cap_contract"`
	SubscriptionByteCapNote      string               `json:"subscription_byte_cap_note"`
	DataQueueMessages            int                  `json:"data_queue_messages"`
	PendingRecoveryTotalBytes    int                  `json:"pending_recovery_total_bytes"`
	PendingRecoveryNote          string               `json:"pending_recovery_note"`
	SGPRecovery                  sgpRecoveryLimits    `json:"peer_recovery"`
	MTPIndicationQueue           int                  `json:"mtp_indication_queue"`
	ASPAffectedPointCodes        int                  `json:"asp_max_affected_point_codes_per_ssnm"`
	ASPRouteRecordsPerRoute      int                  `json:"asp_max_ssnm_state_records_per_route"`
	ASPRouteRecordsPerSG         int                  `json:"asp_max_ssnm_state_records_per_signalling_gateway"`
	ASPRouteRecords              int                  `json:"asp_max_ssnm_state_records"`
	ASPDestinationRecords        int                  `json:"asp_max_ssnm_destination_records_per_association"`
	TransferFlowCacheEntries     int                  `json:"transfer_flow_cache_entries,omitempty"`
	EstablishTimeoutMillis       int64                `json:"establish_timeout_ms"`
	ObservedChannelCapacities    map[string]int       `json:"observed_channel_capacities,omitempty"`
	LiveHeapSteadyBytes          uint64               `json:"steady_live_heap_limit_bytes"`
	RSSSteadyBytes               uint64               `json:"steady_rss_limit_bytes"`
	LiveHeapOverloadBytes        uint64               `json:"overload_live_heap_limit_bytes"`
	RSSOverloadBytes             uint64               `json:"overload_rss_limit_bytes"`
	RetainedAbsoluteBytes        uint64               `json:"retained_absolute_slack_bytes"`
	RetainedPercent              uint64               `json:"retained_percent_slack"`
	RetainWindowMillis           int64                `json:"retain_window_ms"`
	RSSIntervalMillis            int64                `json:"rss_interval_ms"`
	HeapIntervalMillis           int64                `json:"heap_interval_ms"`
	OverloadPayloadBytes         int                  `json:"overload_payload_bytes"`
	LedgerPayloadBytes           int                  `json:"ledger_payload_bytes"`
	RequiredBlocks               int                  `json:"required_blocks"`
	FinalBlocksChecked           int                  `json:"final_blocks_checked"`
	RequiredChurnCycles          int                  `json:"required_churn_cycles"`
	RequiredChurnCyclesPerSecond float64              `json:"required_churn_cycles_per_second"`
}

// sgpRecoveryLimits is the numeric part of the peer SGPConfig; the config
// itself carries a function field and cannot be encoded.
type sgpRecoveryLimits struct {
	RecoveryQueueMessages      int `json:"recovery_queue_messages"`
	RecoveryQueueBytes         int `json:"recovery_queue_bytes"`
	RecoveryQueueTotalMessages int `json:"recovery_queue_total_messages"`
	RecoveryQueueTotalBytes    int `json:"recovery_queue_total_bytes"`
	MaxSSNMDestinationRecords  int `json:"max_ssnm_destination_records"`
}

func recoveryLimits(config *m3ua.SGPConfig) sgpRecoveryLimits {
	return sgpRecoveryLimits{RecoveryQueueMessages: config.RecoveryQueueMessages, RecoveryQueueBytes: config.RecoveryQueueBytes,
		RecoveryQueueTotalMessages: config.RecoveryQueueTotalMessages, RecoveryQueueTotalBytes: config.RecoveryQueueTotalBytes,
		MaxSSNMDestinationRecords: config.MaxSSNMDestinationRecords}
}

func currentManifest(role string, config commandConfig) manifest {
	inventory := aspInventory(config.Routes)
	limits := pinnedLimits{
		SSNMState:                    *ssnmStateConfig(),
		SubscriptionEventCap:         subscriptionQueueSize,
		SubscriptionByteCap:          subscriptionByteCap,
		SubscriptionByteCapNote:      "SSNMStateConfig at this revision has no per-subscription byte cap; only the 256-event cap is configured and enforced",
		DataQueueMessages:            dataQueueSize,
		PendingRecoveryTotalBytes:    pendingRecoveryTotalBytes,
		PendingRecoveryNote:          "pinned on each peer SGP Endpoint; an ASP Endpoint has no pending-recovery queue",
		SGPRecovery:                  recoveryLimits(sgpEndpointConfig()),
		MTPIndicationQueue:           inventory.MTPIndicationQueueSize,
		ASPAffectedPointCodes:        inventory.MaxAffectedPointCodesPerSSNM,
		ASPRouteRecordsPerRoute:      inventory.MaxSSNMStateRecordsPerRoute,
		ASPRouteRecordsPerSG:         inventory.MaxSSNMStateRecordsPerSignallingGateway,
		ASPRouteRecords:              inventory.MaxSSNMStateRecords,
		ASPDestinationRecords:        inventory.MaxSSNMDestinationRecords,
		EstablishTimeoutMillis:       establishTimeout.Milliseconds(),
		LiveHeapSteadyBytes:          steadyLiveHeapLimit,
		RSSSteadyBytes:               steadyRSSLimit,
		LiveHeapOverloadBytes:        overloadLiveHeapLimit,
		RSSOverloadBytes:             overloadRSSLimit,
		RetainedAbsoluteBytes:        retainedAbsoluteSlack,
		RetainedPercent:              retainedPercentSlack,
		RetainWindowMillis:           retainWindow.Milliseconds(),
		RSSIntervalMillis:            rssInterval.Milliseconds(),
		HeapIntervalMillis:           heapInterval.Milliseconds(),
		OverloadPayloadBytes:         overloadPayloadSize,
		LedgerPayloadBytes:           ledgerPayloadSize,
		RequiredBlocks:               requiredBlocks,
		FinalBlocksChecked:           finalBlocksChecked,
		RequiredChurnCycles:          requiredChurnCycles,
		RequiredChurnCyclesPerSecond: requiredChurnRate,
	}
	if inventory.Routing != nil {
		limits.TransferFlowCacheEntries = inventory.Routing.TransferFlowCacheEntries
	}
	result := manifest{
		Role:                     role,
		AssessedBaselineRevision: assessedBaselineRevision,
		GoVersion:                runtime.Version(),
		GoOS:                     runtime.GOOS,
		GoArch:                   runtime.GOARCH,
		GOMAXPROCS:               runtime.GOMAXPROCS(0),
		GOGC:                     os.Getenv("GOGC"),
		GOMEMLIMIT:               os.Getenv("GOMEMLIMIT"),
		GODEBUG:                  os.Getenv("GODEBUG"),
		MemoryLimitBytes:         debug.SetMemoryLimit(-1),
		ImageDigest:              config.ImageDigest,
		Kernel:                   readValues("/proc/sys", "kernel/osrelease", "net/core/rmem_default", "net/core/wmem_default", "net/core/rmem_max", "net/core/wmem_max", "net/sctp/rto_min", "net/sctp/rto_max", "net/sctp/rto_initial", "net/sctp/hb_interval", "net/sctp/sndbuf_policy", "net/sctp/rcvbuf_policy"),
		Cgroup:                   readValues("/sys/fs/cgroup", "memory.max", "memory.swap.max", "cpu.max", "cpuset.cpus.effective"),
		TransparentHugePages: readValues("/sys/kernel/mm/transparent_hugepage", "enabled", "defrag", "khugepaged/defrag",
			"khugepaged/max_ptes_none", "khugepaged/pages_to_scan", "khugepaged/scan_sleep_millisecs"),
		Limits: limits,
		Workload: map[string]string{
			"payload":               config.Payload,
			"asp_to_peer_rate":      fmt.Sprint(config.DataRate),
			"peer_to_asp_rate":      fmt.Sprint(config.ReverseRate),
			"flows_per_association": fmt.Sprint(flowsPerAssociation),
			"ordering":              "strict per flow; each flow has its own SLS, (association mod 4) x 4 + flow",
			"schedule":              "open loop per association, slot k due at k/rate, flows interleaved slot by slot",
			"schedule_tolerance":    "0.5% of the schedule plus 10 ms of offered traffic",
			"contract":              "performance-budgets.md section 4: section 2 mix at 50% of 40,000 msg/s",
		},
		Topology: topologyManifest{
			SignallingGateways: gatewayCount, SGPs: sgpCount, ApplicationServersPerSG: asPerGateway,
			StableAssociations: stableAssociations, ASPerStableAssociation: asPerStableAssociation,
			Partitions: partitionCount, DestinationsPerPartition: destinationsPerPartition, StateRecords: stateRecords,
			RouteIntersectingRecords: gatewayCount * referenceRouteCount, Subscribers: subscriberCount, Routes: config.Routes,
			NetworkAppearance: networkAppearance,
			Partitioning:      "canonical (Signalling Gateway, Application Server) partitions; churn associations join an existing Application Server",
		},
	}
	if build, ok := debug.ReadBuildInfo(); ok {
		result.BuildSettings = map[string]string{}
		for _, setting := range build.Settings {
			switch setting.Key {
			case "vcs.revision":
				result.VCSRevision = setting.Value
			case "vcs.modified":
				result.VCSModified = setting.Value == "true"
			case "vcs.time":
				result.VCSTime = setting.Value
			default:
				result.BuildSettings[setting.Key] = setting.Value
			}
		}
		result.Dependencies = map[string]string{}
		for _, dependency := range build.Deps {
			result.Dependencies[dependency.Path] = dependency.Version
		}
	}
	return result
}

// readValues reads small single-value files under root; an unreadable one is
// recorded as such, never omitted.
func readValues(root string, names ...string) map[string]string {
	values := make(map[string]string, len(names))
	for _, name := range names {
		content, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			values[name] = "unavailable: " + err.Error()
			continue
		}
		values[name] = strings.TrimSpace(string(content))
	}
	return values
}
