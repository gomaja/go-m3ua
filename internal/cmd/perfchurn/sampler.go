package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"time"
)

var errUnavailable = errors.New("unavailable on this platform")

// procSource reads process and kernel state from a proc tree rooted at root
// ("/proc" on Linux). An empty root is a platform without one: every reader
// reports errUnavailable and the run records that instead of a number.
type procSource struct {
	root   string
	cgroup string
}

// SCTP association states in the kernel's enum sctp_state order, which is
// what the ST column of /proc/net/sctp/assocs prints.
const sctpStateEstablished = 3

var sctpStateNames = []string{"CLOSED", "COOKIE_WAIT", "COOKIE_ECHOED", "ESTABLISHED",
	"SHUTDOWN_PENDING", "SHUTDOWN_SENT", "SHUTDOWN_RECEIVED", "SHUTDOWN_ACK_SENT"}

func sctpStateName(state int) string {
	if state >= 0 && state < len(sctpStateNames) {
		return sctpStateNames[state]
	}
	return fmt.Sprintf("STATE_%d", state)
}

type sctpAssoc struct {
	State      int
	Inode      uint64
	LocalPort  int
	RemotePort int
}

// procStatus is what /proc/<pid>/status says about resident memory and
// threads. RSS splits into anonymous memory, file-backed pages and shared
// memory; the thread count exposes operating-system threads the Go runtime
// keeps, whose stacks are resident but are not goroutines.
type procStatus struct {
	RSS     uint64
	RSSAnon uint64
	RSSFile uint64
	Threads int
}

// parseStatus reads VmRSS (required), RssAnon, RssFile (in kB) and Threads.
func parseStatus(content []byte) (procStatus, error) {
	var status procStatus
	found := false
	scanner := bufio.NewScanner(bytes.NewReader(content))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		var target *uint64
		switch fields[0] {
		case "VmRSS:":
			target, found = &status.RSS, true
		case "RssAnon:":
			target = &status.RSSAnon
		case "RssFile:":
			target = &status.RSSFile
		case "Threads:":
			threads, err := strconv.Atoi(fields[1])
			if err != nil {
				return procStatus{}, fmt.Errorf("parse Threads %q: %w", fields[1], err)
			}
			status.Threads = threads
			continue
		default:
			continue
		}
		kilobytes, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return procStatus{}, fmt.Errorf("parse %s %q: %w", fields[0], fields[1], err)
		}
		*target = kilobytes * 1024
	}
	if err := scanner.Err(); err != nil {
		return procStatus{}, err
	}
	if !found {
		return procStatus{}, errors.New("status has no VmRSS line")
	}
	return status, nil
}

// parseStatusRSS reads VmRSS, in kB, from /proc/<pid>/status.
func parseStatusRSS(content []byte) (uint64, error) {
	status, err := parseStatus(content)
	return status.RSS, err
}

// parseSCTPAssocs reads the columns this fixture needs from
// /proc/net/sctp/assocs: ST (4), INODE (10), LPORT (11) and RPORT (12), in
// the layout of the kernel's sctp_assocs_seq_show.
func parseSCTPAssocs(content []byte) ([]sctpAssoc, error) {
	var associations []sctpAssoc
	scanner := bufio.NewScanner(bytes.NewReader(content))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "ASSOC-ID") || strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 13 {
			return nil, fmt.Errorf("short /proc/net/sctp/assocs row %q", line)
		}
		values := make([]uint64, 0, 4)
		for _, index := range []int{4, 10, 11, 12} {
			value, err := strconv.ParseUint(fields[index], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("column %d of /proc/net/sctp/assocs row %q: %w", index, line, err)
			}
			values = append(values, value)
		}
		associations = append(associations, sctpAssoc{State: int(values[0]), Inode: values[1],
			LocalPort: int(values[2]), RemotePort: int(values[3])})
	}
	return associations, scanner.Err()
}

// parseSCTPEndpoints returns the socket inodes (column 7) of
// /proc/net/sctp/eps.
func parseSCTPEndpoints(content []byte) (map[uint64]bool, error) {
	inodes := make(map[uint64]bool)
	scanner := bufio.NewScanner(bytes.NewReader(content))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "ENDPT") || strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 8 {
			return nil, fmt.Errorf("short /proc/net/sctp/eps row %q", line)
		}
		inode, err := strconv.ParseUint(fields[7], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("inode of /proc/net/sctp/eps row %q: %w", line, err)
		}
		inodes[inode] = true
	}
	return inodes, scanner.Err()
}

func (source procSource) status() (procStatus, error) {
	if source.root == "" {
		return procStatus{}, fmt.Errorf("process RSS: %w", errUnavailable)
	}
	content, err := os.ReadFile(filepath.Join(source.root, "self", "status"))
	if err != nil {
		return procStatus{}, fmt.Errorf("read status: %w", err)
	}
	return parseStatus(content)
}

// parseAnonHugePages reads AnonHugePages, in kB, from /proc/<pid>/smaps_rollup:
// anonymous memory the kernel maps with transparent huge pages. khugepaged
// can collapse a region the Go runtime has released back into a resident huge
// page, which shows as RSS the runtime's memory classes count as released.
func parseAnonHugePages(content []byte) (uint64, error) {
	scanner := bufio.NewScanner(bytes.NewReader(content))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 || fields[0] != "AnonHugePages:" {
			continue
		}
		kilobytes, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse AnonHugePages %q: %w", fields[1], err)
		}
		return kilobytes * 1024, nil
	}
	return 0, errors.New("smaps_rollup has no AnonHugePages line")
}

func (source procSource) anonHugePages() (uint64, error) {
	if source.root == "" {
		return 0, fmt.Errorf("anonymous huge pages: %w", errUnavailable)
	}
	content, err := os.ReadFile(filepath.Join(source.root, "self", "smaps_rollup"))
	if err != nil {
		return 0, fmt.Errorf("read smaps_rollup: %w", err)
	}
	return parseAnonHugePages(content)
}

func (source procSource) rss() (uint64, error) {
	status, err := source.status()
	return status.RSS, err
}

func (source procSource) sctpInodes() (associations, endpoints map[uint64]bool) {
	associations, endpoints = map[uint64]bool{}, map[uint64]bool{}
	if content, err := os.ReadFile(filepath.Join(source.root, "net", "sctp", "assocs")); err == nil {
		if parsed, err := parseSCTPAssocs(content); err == nil {
			for _, association := range parsed {
				associations[association.Inode] = true
			}
		}
	}
	if content, err := os.ReadFile(filepath.Join(source.root, "net", "sctp", "eps")); err == nil {
		if parsed, err := parseSCTPEndpoints(content); err == nil {
			endpoints = parsed
		}
	}
	return associations, endpoints
}

// fds counts this process's descriptors and classifies its sockets. The
// listing itself holds one directory descriptor while it runs; that one is in
// every count alike, so comparisons are unaffected. It also returns the
// socket inodes, which is what identifies an owned kernel association.
func (source procSource) fds() (fdSnapshot, map[uint64]bool) {
	if source.root == "" {
		return fdSnapshot{Total: -1, Error: fmt.Sprintf("descriptors: %v", errUnavailable)}, nil
	}
	associations, endpoints := source.sctpInodes()
	directory := filepath.Join(source.root, "self", "fd")
	entries, err := os.ReadDir(directory)
	if err != nil {
		return fdSnapshot{Total: -1, Error: fmt.Sprintf("read descriptors: %v", err)}, nil
	}
	snapshot := fdSnapshot{}
	sockets := make(map[uint64]bool)
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join(directory, entry.Name()))
		if err != nil {
			// The directory descriptor of the listing may already be closed
			// when it is resolved; it still counted as one descriptor.
			snapshot.Total++
			continue
		}
		snapshot.Total++
		inodeText, isSocket := strings.CutPrefix(target, "socket:[")
		if !isSocket {
			continue
		}
		snapshot.Sockets++
		inode, err := strconv.ParseUint(strings.TrimSuffix(inodeText, "]"), 10, 64)
		if err != nil {
			continue
		}
		sockets[inode] = true
		switch {
		case associations[inode]:
			snapshot.SCTPAssociationSockets++
		case endpoints[inode]:
			snapshot.SCTPEndpointSockets++
		default:
			snapshot.Other++
		}
	}
	return snapshot, sockets
}

// kernel classifies every association of the network namespace by whether
// its socket is one of owned.
func (source procSource) kernel(owned map[uint64]bool) kernelAssociations {
	if source.root == "" {
		return kernelAssociations{Error: fmt.Sprintf("kernel associations: %v", errUnavailable)}
	}
	content, err := os.ReadFile(filepath.Join(source.root, "net", "sctp", "assocs"))
	if err != nil {
		return kernelAssociations{Error: fmt.Sprintf("read kernel associations: %v", err)}
	}
	associations, err := parseSCTPAssocs(content)
	if err != nil {
		return kernelAssociations{Error: err.Error()}
	}
	result := kernelAssociations{Total: len(associations)}
	for _, association := range associations {
		name := sctpStateName(association.State)
		switch {
		case association.Inode != 0 && owned[association.Inode] && association.State == sctpStateEstablished:
			result.OwnedEstablished++
		case association.Inode != 0 && owned[association.Inode]:
			if result.OwnedOther == nil {
				result.OwnedOther = map[string]int{}
			}
			result.OwnedOther[name]++
		default:
			if result.UnownedByState == nil {
				result.UnownedByState = map[string]int{}
			}
			result.UnownedByState[name]++
		}
	}
	return result
}

// oomKills reads the cgroup v2 oom_kill counter of the container.
func (source procSource) oomKills() (uint64, error) {
	if source.cgroup == "" {
		return 0, fmt.Errorf("cgroup memory events: %w", errUnavailable)
	}
	content, err := os.ReadFile(filepath.Join(source.cgroup, "memory.events"))
	if err != nil {
		return 0, err
	}
	scanner := bufio.NewScanner(bytes.NewReader(content))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 2 && fields[0] == "oom_kill" {
			return strconv.ParseUint(fields[1], 10, 64)
		}
	}
	return 0, errors.New("memory.events has no oom_kill line")
}

type runtimeSnapshot struct {
	LiveHeapBytes    uint64
	HeapObjectsBytes uint64
	HeapGoalBytes    uint64
	GCCycles         uint64
	Goroutines       int
	Classes          memoryClasses
}

// memoryClasses is the runtime's own partition of the memory it has mapped
// (runtime/metrics /memory/classes). The leaves sum to TotalBytes, so growth
// that the live heap does not show is attributed to a class: fragmentation
// (HeapUnusedBytes), retained free pages, goroutine or thread stacks, or
// runtime metadata.
type memoryClasses struct {
	TotalBytes        uint64 `json:"total_bytes"`
	HeapObjectsBytes  uint64 `json:"heap_objects_bytes"`
	HeapUnusedBytes   uint64 `json:"heap_unused_bytes"`
	HeapFreeBytes     uint64 `json:"heap_free_bytes"`
	HeapReleasedBytes uint64 `json:"heap_released_bytes"`
	HeapStacksBytes   uint64 `json:"heap_stacks_bytes"`
	OSStacksBytes     uint64 `json:"os_stacks_bytes"`
	MetadataBytes     uint64 `json:"metadata_bytes"`
	ProfilingBytes    uint64 `json:"profiling_bytes"`
	OtherBytes        uint64 `json:"other_bytes"`
}

var runtimeMetricNames = [...]string{
	"/gc/heap/live:bytes",
	"/memory/classes/heap/objects:bytes",
	"/gc/heap/goal:bytes",
	"/gc/cycles/total:gc-cycles",
	"/sched/goroutines:goroutines",
	"/memory/classes/total:bytes",
	"/memory/classes/heap/unused:bytes",
	"/memory/classes/heap/free:bytes",
	"/memory/classes/heap/released:bytes",
	"/memory/classes/heap/stacks:bytes",
	"/memory/classes/os-stacks:bytes",
	"/memory/classes/metadata/mcache/free:bytes",
	"/memory/classes/metadata/mcache/inuse:bytes",
	"/memory/classes/metadata/mspan/free:bytes",
	"/memory/classes/metadata/mspan/inuse:bytes",
	"/memory/classes/metadata/other:bytes",
	"/memory/classes/profiling/buckets:bytes",
	"/memory/classes/other:bytes",
}

// readRuntime reads the runtime counters without stopping the world or
// forcing a collection. /gc/heap/live:bytes is the heap the most recent
// collection marked live.
func readRuntime() runtimeSnapshot {
	samples := make([]metrics.Sample, len(runtimeMetricNames))
	for index, name := range runtimeMetricNames {
		samples[index].Name = name
	}
	metrics.Read(samples)
	value := func(index int) uint64 {
		if samples[index].Value.Kind() != metrics.KindUint64 {
			return 0
		}
		return samples[index].Value.Uint64()
	}
	return runtimeSnapshot{
		LiveHeapBytes:    value(0),
		HeapObjectsBytes: value(1),
		HeapGoalBytes:    value(2),
		GCCycles:         value(3),
		Goroutines:       int(value(4)),
		Classes: memoryClasses{
			TotalBytes:        value(5),
			HeapObjectsBytes:  value(1),
			HeapUnusedBytes:   value(6),
			HeapFreeBytes:     value(7),
			HeapReleasedBytes: value(8),
			HeapStacksBytes:   value(9),
			OSStacksBytes:     value(10),
			MetadataBytes:     value(11) + value(12) + value(13) + value(14) + value(15),
			ProfilingBytes:    value(16),
			OtherBytes:        value(17),
		},
	}
}

// forcedRuntimeSnapshot is the retained-heap measurement: two forced
// collections, the second after the first's finalizers and sweeps, then
// memory returned to the OS, then the live heap that last collection marked.
func forcedRuntimeSnapshot() runtimeSnapshot {
	runtime.GC()
	runtime.GC()
	debug.FreeOSMemory()
	return readRuntime()
}

// sampler records the one-second RSS series and the ten-second heap series,
// each sample tagged with the phase current when it was taken.
type sampler struct {
	source    procSource
	rssEvery  time.Duration
	heapEvery time.Duration
	start     time.Time

	mutex  sync.Mutex
	phase  string
	phases []phaseMark
	rss    []rssSample
	heap   []heapSample
}

func newSampler(source procSource, rssEvery, heapEvery time.Duration) *sampler {
	return &sampler{source: source, rssEvery: rssEvery, heapEvery: heapEvery, start: time.Now(),
		phase: phaseStartup, phases: []phaseMark{{Name: phaseStartup}}}
}

func (recorder *sampler) millis() int64 {
	return time.Since(recorder.start).Milliseconds()
}

func (recorder *sampler) currentPhase() string {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	return recorder.phase
}

func (recorder *sampler) setPhase(name string) {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	now := recorder.millis()
	recorder.phases[len(recorder.phases)-1].EndMillis = now
	recorder.phases = append(recorder.phases, phaseMark{Name: name, StartMillis: now, EndMillis: now})
	recorder.phase = name
}

func (recorder *sampler) run(ctx context.Context) {
	rssTicker := time.NewTicker(recorder.rssEvery)
	defer rssTicker.Stop()
	heapTicker := time.NewTicker(recorder.heapEvery)
	defer heapTicker.Stop()
	recorder.sampleRSS()
	recorder.sampleHeap()
	for {
		select {
		case <-ctx.Done():
			return
		case <-rssTicker.C:
			recorder.sampleRSS()
		case <-heapTicker.C:
			recorder.sampleHeap()
		}
	}
}

func (recorder *sampler) sampleRSS() {
	status, err := recorder.source.status()
	snapshot := readRuntime()
	sample := rssSample{AtMillis: recorder.millis(), RSSBytes: status.RSS, RSSAnonBytes: status.RSSAnon,
		RSSFileBytes: status.RSSFile, Threads: status.Threads, HeapObjectsBytes: snapshot.HeapObjectsBytes}
	if err != nil {
		sample.Error = err.Error()
	}
	recorder.mutex.Lock()
	sample.Phase = recorder.phase
	recorder.rss = append(recorder.rss, sample)
	recorder.mutex.Unlock()
}

func (recorder *sampler) sampleHeap() {
	snapshot := readRuntime()
	descriptors, _ := recorder.source.fds()
	sample := heapSample{AtMillis: recorder.millis(), LiveHeapBytes: snapshot.LiveHeapBytes,
		HeapObjectsBytes: snapshot.HeapObjectsBytes, HeapGoalBytes: snapshot.HeapGoalBytes,
		GCCycles: snapshot.GCCycles, Goroutines: snapshot.Goroutines, FDs: descriptors.Total, Classes: snapshot.Classes}
	if descriptors.Error != "" {
		sample.FDs = -1
		sample.Error = descriptors.Error
	}
	if huge, err := recorder.source.anonHugePages(); err != nil {
		sample.Error = strings.TrimPrefix(sample.Error+"; "+err.Error(), "; ")
	} else {
		sample.AnonHugePagesBytes = huge
	}
	if snapshot.LiveHeapBytes == 0 {
		sample.Error = strings.TrimPrefix(sample.Error+"; live heap unavailable", "; ")
	}
	recorder.mutex.Lock()
	sample.Phase = recorder.phase
	recorder.heap = append(recorder.heap, sample)
	recorder.mutex.Unlock()
}

// snapshot copies the series; the open phase ends now.
func (recorder *sampler) snapshot() ([]phaseMark, []rssSample, []heapSample) {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	phases := append([]phaseMark(nil), recorder.phases...)
	phases[len(phases)-1].EndMillis = recorder.millis()
	return phases, append([]rssSample(nil), recorder.rss...), append([]heapSample(nil), recorder.heap...)
}
