//go:build race

package main

// raceDetector reports a race-instrumented test binary, whose several-fold
// slowdown makes wall-clock budgets meaningless.
const raceDetector = true
