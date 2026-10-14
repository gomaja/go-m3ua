//go:build !linux

package main

import "errors"

func newMeasurementClock() (measurementClock, error) {
	return nil, errors.New("same-host clock mode requires Linux CLOCK_MONOTONIC and time namespaces")
}
