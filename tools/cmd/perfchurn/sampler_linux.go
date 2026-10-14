//go:build linux

package main

func defaultProcSource() procSource {
	return procSource{root: "/proc", cgroup: "/sys/fs/cgroup"}
}
