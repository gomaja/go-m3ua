//go:build !linux

package main

// defaultProcSource has no proc tree off Linux: the RSS, descriptor, kernel
// association and OOM readers report errUnavailable, while the runtime heap
// and goroutine readers still work.
func defaultProcSource() procSource {
	return procSource{}
}
