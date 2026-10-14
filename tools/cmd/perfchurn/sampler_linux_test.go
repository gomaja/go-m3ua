//go:build linux

package main

import (
	"os"
	"testing"
)

func TestLinuxProcReadersMeasureThisProcess(t *testing.T) {
	source := defaultProcSource()
	rss, err := source.rss()
	if err != nil || rss == 0 {
		t.Fatalf("rss %d, %v", rss, err)
	}
	descriptors, _ := source.fds()
	if descriptors.Error != "" || descriptors.Total < 3 {
		t.Fatalf("descriptors %+v", descriptors)
	}
	if _, err := source.anonHugePages(); err != nil {
		t.Fatalf("anonymous huge pages: %v", err)
	}
	file, err := os.Open("/proc/self/status")
	if err != nil {
		t.Fatal(err)
	}
	opened, _ := source.fds()
	_ = file.Close()
	if opened.Total != descriptors.Total+1 {
		t.Fatalf("an opened file moved the descriptor count from %d to %d", descriptors.Total, opened.Total)
	}
	if _, err := os.Stat("/proc/net/sctp/assocs"); err != nil {
		t.Skipf("the SCTP module is not loaded, so /proc/net/sctp is absent: %v", err)
	}
	if kernel := source.kernel(nil); kernel.Error != "" {
		t.Fatalf("kernel %+v", kernel)
	}
}
