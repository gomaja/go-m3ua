// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/gomaja/go-sctp"
)

func congestionTestConfig(t *testing.T) TransportCongestionConfig {
	t.Helper()
	cfg, err := newTransportCongestionConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func congestionObserve(d *transportCongestionDetector, cfg TransportCongestionConfig, now time.Time, charged, limit int) TransportCongestionState {
	return d.observe(cfg, now, &sctp.SendBacklog{Charged: charged, Limit: limit}, nil)
}

// congestionObserveContinuously keeps the previous valid input sampled at
// Interval until the requested next observation. Tests of gaps use the single
// observation helper instead, so they cannot manufacture missing coverage.
func congestionObserveContinuously(d *transportCongestionDetector, cfg TransportCongestionConfig, now time.Time, charged, limit int) TransportCongestionState {
	if d.haveValid && d.state.SampleValid {
		previousCharged, previousLimit := d.state.Charged, d.state.Limit
		for sample := d.state.LastValidAt.Add(cfg.Interval); sample.Before(now); sample = sample.Add(cfg.Interval) {
			congestionObserve(d, cfg, sample, previousCharged, previousLimit)
		}
	}
	return congestionObserve(d, cfg, now, charged, limit)
}

func TestTransportCongestionThresholdBoundaries(t *testing.T) {
	cfg := congestionTestConfig(t)
	start := time.Unix(100, 0)
	for _, tc := range []struct {
		name    string
		charged int
		level   uint8
	}{
		{"below1", 49, 0}, {"at1", 50, 1}, {"below2", 69, 1}, {"at2", 70, 2}, {"below3", 89, 2}, {"at3", 90, 3}, {"overLimit", 150, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := transportCongestionDetector{}
			first := congestionObserveContinuously(&d, cfg, start, tc.charged, 100)
			if !first.Known || !first.SampleValid || first.Level != 0 {
				t.Fatalf("initial: %+v", first)
			}
			got := congestionObserveContinuously(&d, cfg, start.Add(cfg.OnsetDwell), tc.charged, 100)
			if got.Level != tc.level {
				t.Fatalf("level=%d want=%d", got.Level, tc.level)
			}
		})
	}
	for _, tc := range []struct {
		name    string
		level   uint8
		charged int
		want    uint8
	}{
		{"at1", 1, 40, 1}, {"below1", 1, 39, 0}, {"at2", 2, 60, 2}, {"below2", 2, 59, 1}, {"at3", 3, 80, 3}, {"below3", 3, 79, 2},
	} {
		t.Run("abatement/"+tc.name, func(t *testing.T) {
			d := transportCongestionDetector{state: TransportCongestionState{Level: tc.level, Known: true}}
			congestionObserveContinuously(&d, cfg, start, tc.charged, 100)
			got := congestionObserveContinuously(&d, cfg, start.Add(cfg.AbatementDwell), tc.charged, 100)
			if got.Level != tc.want {
				t.Fatalf("level=%d want=%d", got.Level, tc.want)
			}
		})
	}
}

func TestTransportCongestionThresholdOverflowSafe(t *testing.T) {
	cfg := congestionTestConfig(t)
	start := time.Unix(100, 0)
	maxInt := int(^uint(0) >> 1)
	d := transportCongestionDetector{}
	congestionObserveContinuously(&d, cfg, start, maxInt, maxInt)
	got := congestionObserveContinuously(&d, cfg, start.Add(cfg.OnsetDwell), maxInt, maxInt)
	if !got.SampleValid || got.Level != 3 {
		t.Fatalf("maximum int sample: %+v", got)
	}
}

func TestTransportCongestionDwellExactAndReset(t *testing.T) {
	cfg := congestionTestConfig(t)
	start := time.Time{}
	d := transportCongestionDetector{}
	congestionObserveContinuously(&d, cfg, start, 90, 100)
	if got := congestionObserveContinuously(&d, cfg, start.Add(cfg.OnsetDwell-time.Nanosecond), 90, 100); got.Level != 0 {
		t.Fatalf("early onset: %+v", got)
	}
	if got := congestionObserveContinuously(&d, cfg, start.Add(cfg.OnsetDwell), 90, 100); got.Level != 3 {
		t.Fatalf("exact onset: %+v", got)
	}
	low := start.Add(time.Second)
	congestionObserveContinuously(&d, cfg, low, 0, 100)
	if got := congestionObserveContinuously(&d, cfg, low.Add(cfg.AbatementDwell-time.Nanosecond), 0, 100); got.Level != 3 {
		t.Fatalf("early abatement: %+v", got)
	}
	if got := congestionObserveContinuously(&d, cfg, low.Add(cfg.AbatementDwell), 0, 100); got.Level != 0 {
		t.Fatalf("exact abatement: %+v", got)
	}

	d = transportCongestionDetector{}
	congestionObserveContinuously(&d, cfg, start, 90, 100)
	congestionObserveContinuously(&d, cfg, start.Add(200*time.Millisecond), 0, 100)
	congestionObserveContinuously(&d, cfg, start.Add(250*time.Millisecond), 90, 100)
	if got := congestionObserveContinuously(&d, cfg, start.Add(400*time.Millisecond), 90, 100); got.Level != 0 {
		t.Fatalf("burst counted across interruption: %+v", got)
	}
	if got := congestionObserveContinuously(&d, cfg, start.Add(550*time.Millisecond), 90, 100); got.Level != 3 {
		t.Fatalf("fresh onset: %+v", got)
	}
}

func TestTransportCongestionDwellIndependentBoundaries(t *testing.T) {
	cfg := congestionTestConfig(t)
	start := time.Unix(100, 0)
	d := transportCongestionDetector{}
	for i, charged := range []int{85, 95, 85, 95} {
		got := congestionObserveContinuously(&d, cfg, start.Add(time.Duration(i)*100*time.Millisecond), charged, 100)
		want := uint8(0)
		if i == 3 {
			want = 2
		}
		if got.Level != want {
			t.Fatalf("sample %d: level=%d want=%d", i, got.Level, want)
		}
	}
	d = transportCongestionDetector{state: TransportCongestionState{Known: true, Level: 3}}
	for i, charged := range []int{30, 50, 70, 30, 70, 50, 70, 30, 70, 50, 70} {
		got := congestionObserveContinuously(&d, cfg, start.Add(time.Duration(i)*100*time.Millisecond), charged, 100)
		want := uint8(3)
		if i == 10 {
			want = 2
		}
		if got.Level != want {
			t.Fatalf("down sample %d: level=%d want=%d", i, got.Level, want)
		}
	}
}

func TestTransportCongestionLimitChanges(t *testing.T) {
	cfg := congestionTestConfig(t)
	start := time.Unix(100, 0)
	d := transportCongestionDetector{state: TransportCongestionState{Association: 17, Epoch: 9, MissedSamples: 4}}
	congestionObserveContinuously(&d, cfg, start, 90, 100)
	got := congestionObserveContinuously(&d, cfg, start.Add(200*time.Millisecond), 90, 80)
	if !got.LimitChanged || got.PreviousLimit != 100 || got.Generation != 1 || got.Level != 0 || got.Association != 17 || got.Epoch != 9 || got.MissedSamples != 4 {
		t.Fatalf("decreased limit: %+v", got)
	}
	got = congestionObserveContinuously(&d, cfg, start.Add(300*time.Millisecond), 90, 80)
	if got.Level != 0 || got.LimitChanged || got.PreviousLimit != 0 || got.Generation != 1 {
		t.Fatalf("inherited dwell: %+v", got)
	}
	got = congestionObserveContinuously(&d, cfg, start.Add(500*time.Millisecond), 90, 80)
	if got.Level != 3 {
		t.Fatalf("fresh dwell: %+v", got)
	}
	got = congestionObserveContinuously(&d, cfg, start.Add(time.Second), 90, 1000)
	if got.Level != 3 || !got.LimitChanged || got.PreviousLimit != 80 || got.Generation != 2 {
		t.Fatalf("increased limit: %+v", got)
	}
	if got = congestionObserveContinuously(&d, cfg, start.Add(2*time.Second-time.Nanosecond), 90, 1000); got.Level != 3 {
		t.Fatalf("early denominator abatement: %+v", got)
	}
	if got = congestionObserveContinuously(&d, cfg, start.Add(2*time.Second), 90, 1000); got.Level != 0 {
		t.Fatalf("denominator abatement: %+v", got)
	}
}

func TestTransportCongestionInvalidSamplesAndStaleness(t *testing.T) {
	cfg := congestionTestConfig(t)
	start := time.Unix(100, 0)
	for _, tc := range []struct {
		name    string
		backlog *sctp.SendBacklog
		err     error
	}{
		{"nil", nil, nil}, {"error", nil, errors.New("query failed")}, {"negativeCharge", &sctp.SendBacklog{Charged: -1, Limit: 100}, nil},
		{"zeroLimit", &sctp.SendBacklog{Charged: 12, Limit: 0}, nil}, {"negativeLimit", &sctp.SendBacklog{Charged: 12, Limit: -1}, nil},
		{"errorWithRaw", &sctp.SendBacklog{Charged: 27, Limit: 333}, errors.New("query failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := transportCongestionDetector{state: TransportCongestionState{Level: 3}}
			congestionObserveContinuously(&d, cfg, start, 90, 100)
			got := d.observe(cfg, start.Add(cfg.StaleAfter-time.Nanosecond), tc.backlog, tc.err)
			if got.SampleValid || !got.Known || got.Level != 3 || got.Errors != 1 || got.Error == "" || !got.LastValidAt.Equal(start) {
				t.Fatalf("invalid: %+v", got)
			}
			if tc.backlog != nil && (got.Charged != tc.backlog.Charged || got.Limit != tc.backlog.Limit) {
				t.Fatalf("raw lost: %+v", got)
			}
			got = d.observe(cfg, start.Add(cfg.StaleAfter), nil, errors.New("still failed"))
			if got.Known || got.Level != 3 || got.Errors != 2 {
				t.Fatalf("stale: %+v", got)
			}
			recovered := start.Add(cfg.StaleAfter + time.Second)
			got = congestionObserveContinuously(&d, cfg, recovered, 0, 100)
			if !got.Known || !got.SampleValid || got.Level != 3 || got.Error != "" || got.Generation != 0 || got.LimitChanged {
				t.Fatalf("recovery: %+v", got)
			}
			if got = congestionObserveContinuously(&d, cfg, recovered.Add(cfg.AbatementDwell-time.Nanosecond), 0, 100); got.Level != 3 {
				t.Fatalf("recovery reused dwell: %+v", got)
			}
			if got = congestionObserveContinuously(&d, cfg, recovered.Add(cfg.AbatementDwell), 0, 100); got.Level != 0 {
				t.Fatalf("recovery abatement: %+v", got)
			}
		})
	}
	d := transportCongestionDetector{}
	got := d.observe(cfg, start, nil, errors.New("unavailable"))
	if got.Known || got.SampleValid || got.Level != 0 || !got.LastValidAt.IsZero() {
		t.Fatalf("never valid: %+v", got)
	}
}

func TestTransportCongestionInvalidResetsPendingDwell(t *testing.T) {
	cfg := congestionTestConfig(t)
	start := time.Unix(100, 0)
	for _, tc := range []struct {
		name    string
		level   uint8
		charged int
		dwell   time.Duration
		want    uint8
	}{
		{"onset", 0, 90, cfg.OnsetDwell, 3}, {"abatement", 3, 0, cfg.AbatementDwell, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := transportCongestionDetector{state: TransportCongestionState{Level: tc.level}}
			congestionObserveContinuously(&d, cfg, start, tc.charged, 100)
			d.observe(cfg, start.Add(tc.dwell-time.Nanosecond), nil, errors.New("query failed"))
			resume := start.Add(tc.dwell)
			if got := congestionObserveContinuously(&d, cfg, resume, tc.charged, 100); got.Level != tc.level {
				t.Fatalf("invalid counted toward dwell: %+v", got)
			}
			if got := congestionObserveContinuously(&d, cfg, resume.Add(tc.dwell), tc.charged, 100); got.Level != tc.want {
				t.Fatalf("fresh dwell: %+v", got)
			}
		})
	}
}

func TestTransportCongestionAbsoluteBoundaries(t *testing.T) {
	cfg := congestionTestConfig(t)
	cfg.AbsoluteOnset = [3]int{100, 200, 300}
	cfg.AbsoluteAbatement = [3]int{80, 180, 280}
	start := time.Unix(100, 0)
	for _, tc := range []struct {
		name           string
		charged, limit int
		level          uint8
	}{
		{"below1", 99, 10000, 0}, {"at1", 100, 10000, 1}, {"below2", 199, 10000, 1}, {"at2", 200, 10000, 2}, {"below3", 299, 10000, 2}, {"at3", 300, 10000, 3}, {"ratioWins", 90, 100, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := transportCongestionDetector{}
			congestionObserveContinuously(&d, cfg, start, tc.charged, tc.limit)
			if got := congestionObserveContinuously(&d, cfg, start.Add(cfg.OnsetDwell), tc.charged, tc.limit); got.Level != tc.level {
				t.Fatalf("absolute onset: %+v want=%d", got, tc.level)
			}
		})
	}
	for _, tc := range []struct {
		name           string
		level          uint8
		charged, limit int
		want           uint8
	}{
		{"at1", 1, 80, 10000, 1}, {"below1", 1, 79, 10000, 0}, {"at2", 2, 180, 10000, 2}, {"below2", 2, 179, 10000, 1}, {"at3", 3, 280, 10000, 3}, {"below3", 3, 279, 10000, 2},
		{"ratioHolds", 3, 80, 100, 3}, {"absoluteHolds", 3, 290, 10000, 3},
	} {
		t.Run("abatement/"+tc.name, func(t *testing.T) {
			d := transportCongestionDetector{state: TransportCongestionState{Known: true, Level: tc.level}}
			congestionObserveContinuously(&d, cfg, start, tc.charged, tc.limit)
			if got := congestionObserveContinuously(&d, cfg, start.Add(cfg.AbatementDwell), tc.charged, tc.limit); got.Level != tc.want {
				t.Fatalf("absolute abatement: %+v want=%d", got, tc.want)
			}
		})
	}
}

func TestTransportCongestionConfigDefaultsAndValidation(t *testing.T) {
	cfg := congestionTestConfig(t)
	if cfg.Interval != 100*time.Millisecond || cfg.Workers != 4 || cfg.Onset != [3]float64{.5, .7, .9} || cfg.Abatement != [3]float64{.4, .6, .8} || cfg.OnsetDwell != 300*time.Millisecond || cfg.AbatementDwell != time.Second || cfg.StaleAfter != 3*time.Second {
		t.Fatalf("defaults: %+v", cfg)
	}
	input := TransportCongestionConfig{Disabled: true, Interval: time.Second, Workers: 64, Onset: [3]float64{.3, .6, .95}, Abatement: [3]float64{.2, .4, .8}, OnsetDwell: time.Second, AbatementDwell: 2 * time.Second, StaleAfter: 4 * time.Second, AbsoluteOnset: [3]int{100, 200, 300}, AbsoluteAbatement: [3]int{50, 150, 250}}
	copyInput := input
	got, err := newTransportCongestionConfig(&input)
	if err != nil || got != input || input != copyInput {
		t.Fatalf("custom config: %+v err=%v input=%+v", got, err, input)
	}
	for _, tc := range []struct {
		name   string
		change func(*TransportCongestionConfig)
	}{
		{"interval", func(c *TransportCongestionConfig) { c.Interval = -1 }}, {"workersNegative", func(c *TransportCongestionConfig) { c.Workers = -1 }}, {"workersExcess", func(c *TransportCongestionConfig) { c.Workers = 65 }},
		{"onsetDwell", func(c *TransportCongestionConfig) { c.OnsetDwell = -1 }}, {"abatementDwell", func(c *TransportCongestionConfig) { c.AbatementDwell = -1 }}, {"stale", func(c *TransportCongestionConfig) { c.StaleAfter = -1 }},
		{"onsetNaN", func(c *TransportCongestionConfig) { c.Onset[1] = math.NaN() }}, {"abatementInf", func(c *TransportCongestionConfig) { c.Abatement[1] = math.Inf(1) }},
		{"onsetNegative", func(c *TransportCongestionConfig) { c.Onset[0] = -.1 }}, {"abatementNegative", func(c *TransportCongestionConfig) { c.Abatement[0] = -.1 }},
		{"equalPair", func(c *TransportCongestionConfig) { c.Abatement[1] = c.Onset[1] }},
		{"onsetOrder", func(c *TransportCongestionConfig) { c.Onset[1] = c.Onset[0] }}, {"abatementOrder", func(c *TransportCongestionConfig) { c.Abatement[1] = c.Abatement[0] }},
		{"overlap", func(c *TransportCongestionConfig) { c.Abatement[1] = c.Onset[0] }},
		{"absolutePartial", func(c *TransportCongestionConfig) { c.AbsoluteOnset[0] = 100 }}, {"absoluteUnpaired", func(c *TransportCongestionConfig) { c.AbsoluteOnset = [3]int{100, 200, 300} }},
		{"absoluteNegative", func(c *TransportCongestionConfig) {
			c.AbsoluteOnset = [3]int{-1, 200, 300}
			c.AbsoluteAbatement = [3]int{50, 150, 250}
		}},
		{"absoluteEqual", func(c *TransportCongestionConfig) {
			c.AbsoluteOnset = [3]int{100, 200, 300}
			c.AbsoluteAbatement = [3]int{100, 150, 250}
		}},
		{"absoluteOrder", func(c *TransportCongestionConfig) {
			c.AbsoluteOnset = [3]int{100, 90, 300}
			c.AbsoluteAbatement = [3]int{50, 60, 250}
		}},
		{"absoluteOverlap", func(c *TransportCongestionConfig) {
			c.AbsoluteOnset = [3]int{100, 200, 300}
			c.AbsoluteAbatement = [3]int{50, 100, 250}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := cfg
			tc.change(&bad)
			_, err := newTransportCongestionConfig(&bad)
			if !errors.Is(err, ErrInvalidRoleConfiguration) {
				t.Fatalf("error=%v", err)
			}
		})
	}
	partial := TransportCongestionConfig{Onset: [3]float64{.3, 0, 0}, Abatement: [3]float64{.2, 0, 0}}
	got, err = newTransportCongestionConfig(&partial)
	if err != nil || got.Onset != [3]float64{.3, .7, .9} || got.Abatement != [3]float64{.2, .6, .8} {
		t.Fatalf("per-field defaults: %+v %v", got, err)
	}
}
