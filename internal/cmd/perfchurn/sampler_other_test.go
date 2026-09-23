//go:build !linux

package main

import (
	"errors"
	"testing"
)

func TestProcReadersAreUnavailableOffLinux(t *testing.T) {
	source := defaultProcSource()
	if _, err := source.rss(); !errors.Is(err, errUnavailable) {
		t.Fatalf("rss error %v, want errUnavailable", err)
	}
	descriptors, _ := source.fds()
	kernel := source.kernel(nil)
	if descriptors.Error == "" || kernel.Error == "" {
		t.Fatalf("descriptors %+v kernel %+v", descriptors, kernel)
	}
	if _, err := source.oomKills(); !errors.Is(err, errUnavailable) {
		t.Fatalf("oom error %v", err)
	}
	if snapshot := forcedRuntimeSnapshot(); snapshot.LiveHeapBytes == 0 {
		t.Fatalf("the runtime heap reader must still work: %+v", snapshot)
	}
}
