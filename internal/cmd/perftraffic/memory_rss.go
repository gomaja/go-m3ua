package main

import (
	"bufio"
	"errors"
	"os"
	"strconv"
	"strings"
)

// peakRSSBytes reads the process's resident-set high-water mark (VmHWM) from
// /proc/self/status. It covers the whole process lifetime, so it bounds the
// peak of any cohort from above. Only Linux provides it; elsewhere it is an
// error, never a zero.
func peakRSSBytes() (uint64, error) {
	file, err := os.Open("/proc/self/status")
	if err != nil {
		return 0, err
	}
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		value, found := strings.CutPrefix(scanner.Text(), "VmHWM:")
		if !found {
			continue
		}
		fields := strings.Fields(value)
		if len(fields) != 2 || fields[1] != "kB" {
			return 0, errors.New("malformed VmHWM line")
		}
		kilobytes, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil || kilobytes > (^uint64(0))/1024 {
			return 0, errors.New("malformed VmHWM value")
		}
		return kilobytes * 1024, nil
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return 0, errors.New("/proc/self/status has no VmHWM")
}
