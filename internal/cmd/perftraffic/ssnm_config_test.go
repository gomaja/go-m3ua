package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func ssnmSenderArguments(extra ...string) []string {
	return append([]string{
		"-role=asp", "-transport=dial", "-sctp-address=10.0.0.2:2905",
		"-peer-control=http://10.0.0.2:8080", "-associations=1", "-rate=10000",
		"-warmup=5s", "-duration=30s", "-drain=2s", "-cohort=ssnm", "-same-host-clock",
	}, extra...)
}

func ssnmReceiverArguments(extra ...string) []string {
	return append([]string{"-role=sgp", "-transport=listen", "-same-host-clock"}, extra...)
}

func TestSSNMFlagsDefaultOffLeaveConfigurationUnchanged(testContext *testing.T) {
	config, err := parseConfig(ssnmSenderArguments())
	if err != nil {
		testContext.Fatalf("parseConfig: %v", err)
	}
	if config.SSNM != (ssnmConfig{}) || config.SSNM.enabled() {
		testContext.Fatalf("SSNM config = %+v, want zero when -ssnm-rate is unset", config.SSNM)
	}
}

func TestSSNMFlagsApplyDefaults(testContext *testing.T) {
	config, err := parseConfig(ssnmSenderArguments("-ssnm-rate=1000"))
	if err != nil {
		testContext.Fatalf("parseConfig: %v", err)
	}
	want := ssnmConfig{Rate: 1000, APCs: 1, Records: 16384, Subscribers: 8, QueueBytes: 1 << 20,
		Budgets: ssnmBudgets{ApplyP99: 100 * time.Millisecond, Resync: 100 * time.Millisecond, Recovery: time.Second}}
	if config.SSNM != want {
		testContext.Fatalf("SSNM config = %+v, want %+v", config.SSNM, want)
	}
	receiver, err := parseConfig(ssnmReceiverArguments("-ssnm-rate=10", "-ssnm-apcs=1024"))
	if err != nil {
		testContext.Fatalf("parse receiver: %v", err)
	}
	if receiver.SSNM != (ssnmConfig{Rate: 10, APCs: 1024, Records: 16384}) {
		testContext.Fatalf("receiver SSNM config = %+v", receiver.SSNM)
	}
}

func TestSSNMBudgetFlagsParseAndReachTheManifest(testContext *testing.T) {
	config, err := parseConfig(ssnmSenderArguments("-ssnm-rate=10", "-ssnm-apcs=1024", "-ssnm-records=1024",
		"-ssnm-apply-p99-budget=250ms", "-ssnm-resync-budget=50ms", "-ssnm-recovery-budget=2s"))
	if err != nil {
		testContext.Fatalf("parseConfig: %v", err)
	}
	want := ssnmBudgets{ApplyP99: 250 * time.Millisecond, Resync: 50 * time.Millisecond, Recovery: 2 * time.Second}
	if config.SSNM.Budgets != want {
		testContext.Fatalf("budgets = %+v, want %+v", config.SSNM.Budgets, want)
	}
	record := config.SSNM.budgetsRecord()
	if record == nil || record.ApplyP99 != want.ApplyP99 || record.Resync != want.Resync || record.Recovery != want.Recovery || record.Scope == "" {
		testContext.Fatalf("manifest budgets = %+v", record)
	}
	encoded, err := json.Marshal(fixtureManifest{SSNMBudgets: record})
	if err != nil || !strings.Contains(string(encoded), `"ssnm_budgets":{"apply_p99_ns":250000000,"resync_ns":50000000,"recovery_ns":2000000000`) {
		testContext.Fatalf("manifest encoding %s, %v", encoded, err)
	}
	off, err := parseConfig(ssnmSenderArguments())
	if err != nil || off.SSNM.budgetsRecord() != nil {
		testContext.Fatalf("budgets recorded without SSNM load: %+v, %v", off.SSNM, err)
	}
	encoded, err = json.Marshal(fixtureManifest{})
	if err != nil || strings.Contains(string(encoded), "ssnm") {
		testContext.Fatalf("manifest without SSNM load encodes %s, %v", encoded, err)
	}
}

func TestSSNMPauseFlagParses(testContext *testing.T) {
	config, err := parseConfig(ssnmSenderArguments("-ssnm-rate=1000", "-pause-subscriber=5s/10s"))
	if err != nil {
		testContext.Fatalf("parseConfig: %v", err)
	}
	if config.SSNM.Pause != (ssnmPause{Offset: 5 * time.Second, Duration: 10 * time.Second}) || config.SSNM.healthySubscribers() != 7 {
		testContext.Fatalf("pause = %+v healthy = %d", config.SSNM.Pause, config.SSNM.healthySubscribers())
	}
}

func TestSSNMFlagsRejectInvalidCombinations(testContext *testing.T) {
	cases := map[string]struct {
		arguments []string
		want      string
	}{
		"apcs without rate":         {ssnmSenderArguments("-ssnm-apcs=1024"), "requires -ssnm-rate"},
		"subscribers without rate":  {ssnmSenderArguments("-subscribers=4"), "requires -ssnm-rate"},
		"pause without rate":        {ssnmSenderArguments("-pause-subscriber=1s/1s"), "requires -ssnm-rate"},
		"rate above bound":          {ssnmSenderArguments("-ssnm-rate=10001"), "must not exceed"},
		"zero apcs":                 {ssnmSenderArguments("-ssnm-rate=10", "-ssnm-apcs=0"), "ssnm-apcs"},
		"apcs above bound":          {ssnmSenderArguments("-ssnm-rate=10", "-ssnm-apcs=1025"), "ssnm-apcs"},
		"records above bound":       {ssnmSenderArguments("-ssnm-rate=10", "-ssnm-records=16385"), "ssnm-records"},
		"records not multiple":      {ssnmSenderArguments("-ssnm-rate=10", "-ssnm-apcs=1024", "-ssnm-records=1000"), "multiple"},
		"echo mode":                 {append(ssnmSenderArguments("-ssnm-rate=10"), "-mode=echo", "-same-host-clock=false"), "throughput"},
		"no shared clock":           {append(ssnmSenderArguments("-ssnm-rate=10"), "-same-host-clock=false"), "same-host-clock"},
		"receiver subscribers":      {ssnmReceiverArguments("-ssnm-rate=10", "-subscribers=8"), "not SGP flags"},
		"receiver pause":            {ssnmReceiverArguments("-ssnm-rate=10", "-pause-subscriber=1s/1s"), "not SGP flags"},
		"too many subscribers":      {ssnmSenderArguments("-ssnm-rate=10", "-subscribers=17"), "subscribers"},
		"pause with one subscriber": {ssnmSenderArguments("-ssnm-rate=10", "-subscribers=1", "-pause-subscriber=1s/1s"), "at least two"},
		"pause past window":         {ssnmSenderArguments("-ssnm-rate=10", "-pause-subscriber=25s/10s"), "inside the measurement window"},
		"pause malformed":           {ssnmSenderArguments("-ssnm-rate=10", "-pause-subscriber=10s"), "offset>/<duration"},
		"pause negative":            {ssnmSenderArguments("-ssnm-rate=10", "-pause-subscriber=-1s/1s"), "negative"},
		"receipt storage bound":     {append(ssnmSenderArguments("-ssnm-rate=10000", "-subscribers=16"), "-associations=32", "-duration=9m", "-warmup=0s"), "receipt storage"},
		"budget without rate":       {ssnmSenderArguments("-ssnm-resync-budget=1s"), "requires -ssnm-rate"},
		"receiver budget":           {ssnmReceiverArguments("-ssnm-rate=10", "-ssnm-apply-p99-budget=1s"), "not an SGP flag"},
		"zero budget":               {ssnmSenderArguments("-ssnm-rate=10", "-ssnm-recovery-budget=0s"), "budgets must be positive"},
		"negative budget":           {ssnmSenderArguments("-ssnm-rate=10", "-ssnm-apply-p99-budget=-1ms"), "budgets must be positive"},
		"queue bytes without rate":  {ssnmSenderArguments("-ssnm-subscription-queue-bytes=98304"), "requires -ssnm-rate"},
		"receiver queue bytes":      {ssnmReceiverArguments("-ssnm-rate=10", "-ssnm-subscription-queue-bytes=98304"), "not SGP flags"},
		"negative queue bytes":      {ssnmSenderArguments("-ssnm-rate=10", "-ssnm-subscription-queue-bytes=-1"), "must not be negative"},
		"queue bytes below library": {ssnmSenderArguments("-ssnm-rate=10", "-ssnm-subscription-queue-bytes=511"), "at least 512"},
		// The full store's preload message names 1,024 destinations:
		// 516 + 1,024 x 268 accounted bytes.
		"queue bytes below preload": {ssnmSenderArguments("-ssnm-rate=10", "-ssnm-subscription-queue-bytes=274947"), "cannot hold the 1024-destination preload message, 274948 accounted bytes"},
		"small store preload":       {ssnmSenderArguments("-ssnm-rate=10", "-ssnm-records=256", "-ssnm-subscription-queue-bytes=69123"), "cannot hold the 256-destination preload message, 69124"},
	}
	for name, testCase := range cases {
		testContext.Run(name, func(testContext *testing.T) {
			_, err := parseConfig(testCase.arguments)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				testContext.Fatalf("parseConfig error = %v, want containing %q", err, testCase.want)
			}
		})
	}
}

func TestSSNMRunSpecOmittedWhenOff(testContext *testing.T) {
	encoded, err := json.Marshal(runSpec{Cohort: "c", Associations: 1, Expected: 1, Duration: time.Second, Rate: 1})
	if err != nil {
		testContext.Fatal(err)
	}
	if strings.Contains(string(encoded), "ssnm") {
		testContext.Fatalf("runSpec without SSNM load serializes an ssnm field: %s", encoded)
	}
	encoded, err = json.Marshal(runRecord{Side: "sender"})
	if err != nil {
		testContext.Fatal(err)
	}
	if strings.Contains(string(encoded), "\"ssnm\"") {
		testContext.Fatalf("runRecord without SSNM load serializes an ssnm field: %s", encoded)
	}
}

func TestSSNMRunSpecRoundTripsAndCompares(testContext *testing.T) {
	window := &sharedClockWindow{Start: 10, End: 20}
	specification := runSpec{Cohort: "c", Clock: window, SSNM: workloadRef(ssnmConfig{Rate: 1000, APCs: 1, Records: 16384, Subscribers: 8}.workload(ssnmPhaseMeasurement, 10))}
	encoded, err := json.Marshal(specification)
	if err != nil {
		testContext.Fatal(err)
	}
	var decoded runSpec
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		testContext.Fatal(err)
	}
	if !sameRunSpec(specification, decoded) {
		testContext.Fatalf("decoded spec %+v differs from %+v", decoded, specification)
	}
	decoded.SSNM.Records = 8192
	if sameRunSpec(specification, decoded) {
		testContext.Fatal("specs with different SSNM workloads compare equal")
	}
}

// The byte limit resolves to the library default when unset and reaches the
// store configuration, the recorded limits and every cohort specification.
func TestSSNMSubscriptionQueueBytesReachStoreAndSpec(testContext *testing.T) {
	for _, testCase := range []struct {
		name      string
		arguments []string
		want      int
	}{
		{"library default", ssnmSenderArguments("-ssnm-rate=1000"), 1 << 20},
		{"byte-binding F3", ssnmSenderArguments("-ssnm-rate=1000", "-ssnm-records=256", "-pause-subscriber=5s/10s", "-ssnm-subscription-queue-bytes=98304"), 98_304},
		{"exactly one preload message", ssnmSenderArguments("-ssnm-rate=1000", "-ssnm-records=256", "-ssnm-subscription-queue-bytes=69124"), 69_124},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			config, err := parseConfig(testCase.arguments)
			if err != nil {
				testContext.Fatalf("parseConfig: %v", err)
			}
			limits := ssnmStoreLimits(config.SSNM, config.Associations)
			workload := config.SSNM.workload(ssnmPhaseMeasurement, 1)
			if config.SSNM.QueueBytes != testCase.want || limits.SubscriptionQueueBytes != testCase.want || limits.SubscriptionQueueSize != 256 ||
				limitsRecord(limits).SubscriptionQueueBytes != testCase.want || workload.SubscriptionQueueBytes != testCase.want {
				testContext.Fatalf("queue bytes %d, limits %+v, workload %+v, want %d", config.SSNM.QueueBytes, limits, workload, testCase.want)
			}
			encoded, err := json.Marshal(runSpec{SSNM: &workload})
			if err != nil || !strings.Contains(string(encoded), fmt.Sprintf(`"subscription_queue_bytes":%d`, testCase.want)) {
				testContext.Fatalf("spec encoding %s, %v", encoded, err)
			}
		})
	}
	receiver, err := parseConfig(ssnmReceiverArguments("-ssnm-rate=1000"))
	if err != nil || receiver.SSNM.QueueBytes != 0 {
		testContext.Fatalf("receiver queue bytes %d, %v", receiver.SSNM.QueueBytes, err)
	}
	specification := runSpec{SSNM: workloadRef(ssnmConfig{Rate: 1000, APCs: 1, Records: 256, Subscribers: 8, QueueBytes: 98_304}.workload(ssnmPhaseMeasurement, 1))}
	other := specification
	other.SSNM = workloadRef(*specification.SSNM)
	other.SSNM.SubscriptionQueueBytes = 1 << 20
	if sameRunSpec(specification, other) {
		testContext.Fatal("specs with different subscription byte limits compare equal")
	}
}

func TestSSNMStoreLimitsFitTheWorkload(testContext *testing.T) {
	limits := ssnmStoreLimits(ssnmConfig{Rate: 1000, APCs: 1, Records: 16384, Subscribers: 8}, 2)
	if limits.MaxRecords != 32768 || limits.MaxRecordsPerPartition != 16384 || limits.MaxRecordsPerPeer != 16384 ||
		limits.MaxSubscribers != 8 || limits.SubscriptionQueueSize != 256 || limits.MaxAffectedPointCodes != 1024 {
		testContext.Fatalf("limits = %+v", limits)
	}
	if need := 2 * (ssnmAccountedPartitionBytes + 16384*ssnmAccountedRecordBytes); limits.MaxBytes < need {
		testContext.Fatalf("MaxBytes %d cannot hold %d accounted bytes", limits.MaxBytes, need)
	}
	if config := senderEndpointConfig(commandConfig{Associations: 1}); config.SSNMState != nil || config.ASP != nil {
		testContext.Fatalf("endpoint configuration without SSNM load changed: %+v", config)
	}
}

// workloadRef returns a declaration as runSpec carries it.
func workloadRef(workload ssnmWorkload) *ssnmWorkload { return &workload }
