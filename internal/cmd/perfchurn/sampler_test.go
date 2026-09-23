package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const statusFixture = `Name:	perfchurn
VmPeak:	 1300000 kB
VmHWM:	   60000 kB
VmRSS:	   51200 kB
RssAnon:	   40960 kB
RssFile:	   10240 kB
RssShmem:	       0 kB
Threads:	9
`

// Header and rows in the layout of the kernel's sctp_assocs_seq_show.
const assocsFixture = ` ASSOC     SOCK   STY SST ST HBKT ASSOC-ID TX_QUEUE RX_QUEUE UID INODE LPORT RPORT LADDRS <-> RADDRS HBINT INS OUTS MAXRT T1X T2X RTXC wmema wmemq sndbuf rcvbuf
0000000000000000 0000000000000000 1   1   3  1234    5        0        0       0   100 2905  30000  172.31.250.10 <-> *172.31.250.20 	    7500    17    17   10    0    0        0        1        0   212992   212992
0000000000000000 0000000000000000 1   1   7  1235    6        0        0       0     0 2905  30101  172.31.250.10 <-> *172.31.250.20 	    7500    17    17   10    0    0        0        1        0   212992   212992
0000000000000000 0000000000000000 1   1   3  1236    7        0        0       0   555 2905  30102  172.31.250.10 <-> *172.31.250.20 	    7500    17    17   10    0    0        0        1        0   212992   212992
0000000000000000 0000000000000000 1   1   5  1237    8        0        0       0   300 2905  30103  172.31.250.10 <-> *172.31.250.20 	    7500    17    17   10    0    0        0        1        0   212992   212992
`

const endpointsFixture = ` ENDPT     SOCK   STY SST HBKT LPORT   UID INODE LADDRS
0000000000000000 0000000000000000 1   10  17   2905      0 200 172.31.250.10
`

func fakeProc(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, directory := range []string{"self/fd", "net/sctp"} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	files := map[string]string{
		"self/status":       statusFixture,
		"net/sctp/assocs":   assocsFixture,
		"net/sctp/eps":      endpointsFixture,
		"self/smaps_rollup": smapsRollupFixture,
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	links := map[string]string{"0": "/dev/null", "3": "socket:[100]", "4": "socket:[200]", "5": "anon_inode:[eventpoll]",
		"6": "socket:[300]", "7": "socket:[999]"}
	for name, target := range links {
		if err := os.Symlink(target, filepath.Join(root, "self/fd", name)); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestParseStatusBreaksDownRSSAndCountsThreads(t *testing.T) {
	status, err := parseStatus([]byte(statusFixture))
	if err != nil || status.RSS != 51200*1024 || status.RSSAnon != 40960*1024 || status.RSSFile != 10240*1024 || status.Threads != 9 {
		t.Fatalf("status %+v, %v", status, err)
	}
	if _, err := parseStatus([]byte("VmRSS:\t1 kB\nThreads:\tmany\n")); err == nil {
		t.Fatal("unparsable Threads accepted")
	}
}

func TestMemoryClassesAccountForTheRuntimeFootprint(t *testing.T) {
	snapshot := forcedRuntimeSnapshot()
	classes := snapshot.Classes
	if classes.TotalBytes == 0 || classes.HeapObjectsBytes == 0 || classes.HeapStacksBytes == 0 || classes.MetadataBytes == 0 {
		t.Fatalf("classes %+v", classes)
	}
	parts := classes.HeapObjectsBytes + classes.HeapUnusedBytes + classes.HeapFreeBytes + classes.HeapReleasedBytes +
		classes.HeapStacksBytes + classes.OSStacksBytes + classes.MetadataBytes + classes.ProfilingBytes + classes.OtherBytes
	if parts != classes.TotalBytes {
		t.Fatalf("classes %+v sum to %d, total %d", classes, parts, classes.TotalBytes)
	}
}

const smapsRollupFixture = `00400000-7fffe000 ---p 00000000 00:00 0                      [rollup]
Rss:               51200 kB
Anonymous:         40960 kB
AnonHugePages:     18432 kB
Swap:                  0 kB
`

func TestParseSmapsRollupReadsAnonymousHugePages(t *testing.T) {
	huge, err := parseAnonHugePages([]byte(smapsRollupFixture))
	if err != nil || huge != 18432*1024 {
		t.Fatalf("anonymous huge pages %d, %v", huge, err)
	}
	if _, err := parseAnonHugePages([]byte("Rss: 1 kB\n")); err == nil {
		t.Fatal("rollup without AnonHugePages accepted")
	}
	source := procSource{root: fakeProc(t)}
	if huge, err := source.anonHugePages(); err != nil || huge != 18432*1024 {
		t.Fatalf("source anonymous huge pages %d, %v", huge, err)
	}
}

func TestParseStatusRSS(t *testing.T) {
	rss, err := parseStatusRSS([]byte(statusFixture))
	if err != nil || rss != 51200*1024 {
		t.Fatalf("rss %d, %v", rss, err)
	}
	if _, err := parseStatusRSS([]byte("Name:\tx\nVmHWM:\t1 kB\n")); err == nil {
		t.Fatal("status without VmRSS accepted")
	}
	if _, err := parseStatusRSS([]byte("VmRSS:\tlots kB\n")); err == nil {
		t.Fatal("unparsable VmRSS accepted")
	}
}

func TestParseSCTPAssociationsAndEndpoints(t *testing.T) {
	associations, err := parseSCTPAssocs([]byte(assocsFixture))
	if err != nil || len(associations) != 4 {
		t.Fatalf("associations %+v, %v", associations, err)
	}
	if associations[0].State != sctpStateEstablished || associations[0].Inode != 100 || associations[0].LocalPort != 2905 ||
		associations[0].RemotePort != 30000 || associations[1].State != 7 || associations[1].Inode != 0 {
		t.Fatalf("associations %+v", associations)
	}
	endpoints, err := parseSCTPEndpoints([]byte(endpointsFixture))
	if err != nil || !endpoints[200] || len(endpoints) != 1 {
		t.Fatalf("endpoints %v, %v", endpoints, err)
	}
	if _, err := parseSCTPAssocs([]byte(" ASSOC SOCK\nshort row\n")); err == nil {
		t.Fatal("short association row accepted")
	}
	if sctpStateName(sctpStateEstablished) != "ESTABLISHED" || sctpStateName(7) != "SHUTDOWN_ACK_SENT" || sctpStateName(42) != "STATE_42" {
		t.Fatal("state names")
	}
}

func TestDescriptorAndKernelClassification(t *testing.T) {
	source := procSource{root: fakeProc(t)}
	descriptors, inodes := source.fds()
	if descriptors.Error != "" || descriptors.Total != 6 || descriptors.Sockets != 4 ||
		descriptors.SCTPAssociationSockets != 2 || descriptors.SCTPEndpointSockets != 1 || descriptors.Other != 1 {
		t.Fatalf("descriptors %+v", descriptors)
	}
	kernel := source.kernel(inodes)
	if kernel.Error != "" || kernel.Total != 4 || kernel.OwnedEstablished != 1 || kernel.OwnedOther["SHUTDOWN_SENT"] != 1 ||
		kernel.UnownedByState["SHUTDOWN_ACK_SENT"] != 1 || kernel.unownedEstablished() != 1 {
		t.Fatalf("kernel %+v", kernel)
	}
	rss, err := source.rss()
	if err != nil || rss != 51200*1024 {
		t.Fatalf("rss %d, %v", rss, err)
	}
}

func TestMissingProcIsReportedNotFatal(t *testing.T) {
	source := procSource{root: filepath.Join(t.TempDir(), "absent")}
	if _, err := source.rss(); err == nil {
		t.Fatal("absent status read")
	}
	descriptors, _ := source.fds()
	kernel := source.kernel(nil)
	if descriptors.Error == "" || kernel.Error == "" {
		t.Fatalf("absent proc not reported: %+v %+v", descriptors, kernel)
	}
}

func TestRuntimeSnapshotReadsTheLiveHeap(t *testing.T) {
	snapshot := forcedRuntimeSnapshot()
	if snapshot.LiveHeapBytes == 0 || snapshot.HeapObjectsBytes == 0 || snapshot.Goroutines < 1 || snapshot.GCCycles < 2 {
		t.Fatalf("snapshot %+v", snapshot)
	}
}

func TestSamplerRecordsBothSeriesByPhase(t *testing.T) {
	source := procSource{root: fakeProc(t)}
	recorder := newSampler(source, 5*time.Millisecond, 20*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		recorder.run(ctx)
		close(done)
	}()
	recorder.setPhase(phaseSteady)
	time.Sleep(60 * time.Millisecond)
	recorder.setPhase(phaseOverload)
	time.Sleep(60 * time.Millisecond)
	cancel()
	<-done
	phases, rssSeries, heapSeries := recorder.snapshot()
	if len(phases) != 3 || phases[0].Name != phaseStartup || phases[1].Name != phaseSteady || phases[2].Name != phaseOverload {
		t.Fatalf("phases %+v", phases)
	}
	for index, mark := range phases {
		if mark.EndMillis < mark.StartMillis || (index > 0 && mark.StartMillis != phases[index-1].EndMillis) {
			t.Fatalf("phase boundaries %+v", phases)
		}
	}
	seen := map[string]bool{}
	for _, sample := range rssSeries {
		if sample.Error != "" || sample.RSSBytes != 51200*1024 || sample.RSSAnonBytes != 40960*1024 || sample.Threads != 9 || sample.HeapObjectsBytes == 0 {
			t.Fatalf("rss sample %+v", sample)
		}
		seen[sample.Phase] = true
	}
	if !seen[phaseSteady] || !seen[phaseOverload] || len(rssSeries) < 10 {
		t.Fatalf("rss series of %d samples covered %v", len(rssSeries), seen)
	}
	if len(heapSeries) < 3 || heapSeries[0].LiveHeapBytes == 0 || heapSeries[0].FDs != 6 || heapSeries[0].Classes.TotalBytes == 0 ||
		heapSeries[0].AnonHugePagesBytes != 18432*1024 {
		t.Fatalf("heap series %+v", heapSeries)
	}
	if len(heapSeries) >= len(rssSeries) {
		t.Fatalf("heap sampled as often as RSS: %d vs %d", len(heapSeries), len(rssSeries))
	}
}

func TestSamplerKeepsSamplingWhenProcIsUnavailable(t *testing.T) {
	recorder := newSampler(procSource{root: filepath.Join(t.TempDir(), "absent")}, 5*time.Millisecond, 10*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	recorder.run(ctx)
	_, rssSeries, heapSeries := recorder.snapshot()
	if len(rssSeries) == 0 || !strings.Contains(rssSeries[0].Error, "status") || rssSeries[0].HeapObjectsBytes == 0 {
		t.Fatalf("rss series %+v", rssSeries)
	}
	if len(heapSeries) == 0 || heapSeries[0].LiveHeapBytes == 0 || heapSeries[0].FDs != -1 {
		t.Fatalf("heap series %+v", heapSeries)
	}
}
