// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"fmt"
	"math"
	"time"

	"github.com/gomaja/go-sctp"
)

// TransportCongestionConfig controls detection of outbound SCTP memory pressure.
// Onset and Abatement are charged-memory/limit ratios for levels 1, 2 and 3.
// AbsoluteOnset and AbsoluteAbatement optionally add charged-memory byte
// thresholds; all six must be positive when enabled. Either onset condition
// starts entry dwell, while both abatement conditions must hold to leave.
// Zero fields use the defaults; Disabled disables automatic sampling.
// These thresholds are engineering settings, not standardized congestion levels.
// Memory pressure alone does not identify network loss, receiver backpressure,
// payload backlog or time to drain.
type TransportCongestionConfig struct {
	Disabled bool
	// Interval is the sampling period. Pending dwell requires consecutive
	// valid samples no more than Interval + Interval/2 apart, without a
	// reported missed slot. A coverage gap restarts dwell, retaining Level.
	Interval          time.Duration
	Workers           int
	Onset             [3]float64
	Abatement         [3]float64
	OnsetDwell        time.Duration
	AbatementDwell    time.Duration
	StaleAfter        time.Duration
	AbsoluteOnset     [3]int
	AbsoluteAbatement [3]int
}

func newTransportCongestionConfig(input *TransportCongestionConfig) (TransportCongestionConfig, error) {
	cfg := TransportCongestionConfig{}
	if input != nil {
		cfg = *input
	}
	if cfg.Interval == 0 {
		cfg.Interval = 100 * time.Millisecond
	}
	if cfg.Workers == 0 {
		cfg.Workers = 4
	}
	if cfg.OnsetDwell == 0 {
		cfg.OnsetDwell = 300 * time.Millisecond
	}
	if cfg.AbatementDwell == 0 {
		cfg.AbatementDwell = time.Second
	}
	if cfg.StaleAfter == 0 {
		cfg.StaleAfter = 3 * time.Second
	}
	defaultsOnset := [3]float64{.5, .7, .9}
	defaultsAbatement := [3]float64{.4, .6, .8}
	for i := range cfg.Onset {
		if cfg.Onset[i] == 0 {
			cfg.Onset[i] = defaultsOnset[i]
		}
		if cfg.Abatement[i] == 0 {
			cfg.Abatement[i] = defaultsAbatement[i]
		}
	}
	invalid := func(reason string) (TransportCongestionConfig, error) {
		return TransportCongestionConfig{}, fmt.Errorf("%w: transport congestion %s", ErrInvalidRoleConfiguration, reason)
	}
	if cfg.Interval < 0 || cfg.OnsetDwell < 0 || cfg.AbatementDwell < 0 || cfg.StaleAfter < 0 {
		return invalid("durations must be positive")
	}
	if cfg.Workers < 1 || cfg.Workers > 64 {
		return invalid("workers must be between 1 and 64")
	}
	absolute := cfg.AbsoluteOnset != [3]int{} || cfg.AbsoluteAbatement != [3]int{}
	for i := range cfg.Onset {
		onset, abatement := cfg.Onset[i], cfg.Abatement[i]
		if math.IsNaN(onset) || math.IsInf(onset, 0) || math.IsNaN(abatement) || math.IsInf(abatement, 0) || abatement <= 0 || onset <= abatement {
			return invalid("ratios must be finite with 0 < abatement < onset")
		}
		if i > 0 && (onset <= cfg.Onset[i-1] || abatement <= cfg.Abatement[i-1] || abatement <= cfg.Onset[i-1]) {
			return invalid("ratios must increase without overlapping hysteresis bands")
		}
		if absolute {
			onset, abatement := cfg.AbsoluteOnset[i], cfg.AbsoluteAbatement[i]
			if abatement <= 0 || onset <= abatement {
				return invalid("absolute thresholds require three positive abatement/onset pairs")
			}
			if i > 0 && (onset <= cfg.AbsoluteOnset[i-1] || abatement <= cfg.AbsoluteAbatement[i-1] || abatement <= cfg.AbsoluteOnset[i-1]) {
				return invalid("absolute thresholds must increase without overlapping hysteresis bands")
			}
		}
	}
	return cfg, nil
}

// TransportCongestionState is a local outbound transport-pressure observation.
// Level is an implementation-defined severity from 0 to 3, independent of
// received SCON and inbound overload. Known=false means unavailable or stale;
// Level then retains historical severity and must not be interpreted as clear.
// Charged and Limit are allocated-memory bytes including kernel overhead, not
// payload bytes, message counts or delay. Charged can exceed Limit. Raw values
// are retained even for invalid samples. A nil sample retains previous values.
// SampledAt timestamps the latest attempt; LastValidAt timestamps the latest
// successful measurement. Errors counts invalid attempts, and MissedSamples is
// maintained by the scheduler. A valid changed Limit increments Generation;
// LimitChanged and PreviousLimit describe that observation only.
type TransportCongestionState struct {
	// DiagnosticAt timestamps the separately read SCTP status; it is not atomic with the backlog.
	DiagnosticAt time.Time
	// DiagnosticValid distinguishes a successful status read from unavailable diagnostics.
	DiagnosticValid bool
	// DiagnosticError preserves the most recent failed status read.
	DiagnosticError string
	// ReceiverWindow is the tracked peer receiver credit; zero suggests peer flow control.
	ReceiverWindow uint32
	// UnackedDataChunks counts unacknowledged DATA chunks, not unsent memory.
	UnackedDataChunks uint16
	// PrimaryCwnd is the primary path congestion window in bytes, not an association backlog.
	PrimaryCwnd uint32
	Association AssociationID
	Epoch       uint64
	Level       uint8
	Known       bool
	SampleValid bool
	Charged     int
	Limit       int
	SampledAt   time.Time
	LastValidAt time.Time
	// Samples counts completed backlog observations, including invalid ones.
	Samples       uint64
	Errors        uint64
	MissedSamples uint64
	Generation    uint64
	LimitChanged  bool
	PreviousLimit int
	Error         string
}

// The caller serializes observations and owns association/epoch lifecycle.
type transportCongestionDetector struct {
	state               TransportCongestionState
	aboveOnsetSince     [3]time.Time
	belowAbatementSince [3]time.Time
	aboveOnset          [3]bool
	belowAbatement      [3]bool
	haveValid           bool
	lastLimit           int
}

func (d *transportCongestionDetector) resetDwell() {
	d.aboveOnsetSince = [3]time.Time{}
	d.belowAbatementSince = [3]time.Time{}
	d.aboveOnset = [3]bool{}
	d.belowAbatement = [3]bool{}
}

func (d *transportCongestionDetector) observe(cfg TransportCongestionConfig, now time.Time, backlog *sctp.SendBacklog, err error) TransportCongestionState {
	d.state.Samples++
	d.state.SampledAt = now
	d.state.LimitChanged = false
	d.state.PreviousLimit = 0
	if backlog != nil {
		d.state.Charged = backlog.Charged
		d.state.Limit = backlog.Limit
	}
	if err == nil {
		switch {
		case backlog == nil:
			err = fmt.Errorf("transport congestion: no backlog sample")
		case backlog.Charged < 0:
			err = fmt.Errorf("transport congestion: negative charged memory")
		case backlog.Limit <= 0:
			err = fmt.Errorf("transport congestion: nonpositive memory limit")
		}
	}
	if err != nil {
		d.state.SampleValid = false
		d.state.Error = err.Error()
		d.state.Errors++
		d.resetDwell()
		if !d.haveValid || now.Sub(d.state.LastValidAt) >= cfg.StaleAfter {
			d.state.Known = false
		}
		return d.state
	}
	if !d.state.Known {
		d.resetDwell()
	}
	if d.haveValid {
		// Allow half an interval of scheduling jitter, not an unobserved
		// sampling period. Separate additions also represent bounds larger
		// than a Duration without saturating the elapsed sampling gap.
		deadline := d.state.LastValidAt.Add(cfg.Interval).Add(cfg.Interval / 2)
		if now.Before(d.state.LastValidAt) || now.After(deadline) {
			d.resetDwell()
		}
	}
	if d.haveValid && backlog.Limit != d.lastLimit {
		d.state.PreviousLimit = d.lastLimit
		d.state.LimitChanged = true
		d.state.Generation++
		d.resetDwell()
	}
	d.lastLimit = backlog.Limit
	d.haveValid = true
	d.state.Known = true
	d.state.SampleValid = true
	d.state.LastValidAt = now
	d.state.Error = ""
	// Division avoids overflowing int products at large kernel counters.
	ratio := float64(backlog.Charged) / float64(backlog.Limit)
	absolute := cfg.AbsoluteOnset != [3]int{}
	for i := range cfg.Onset {
		onset := ratio >= cfg.Onset[i]
		abatement := ratio < cfg.Abatement[i]
		if absolute {
			onset = onset || backlog.Charged >= cfg.AbsoluteOnset[i]
			abatement = abatement && backlog.Charged < cfg.AbsoluteAbatement[i]
		}
		if onset {
			if !d.aboveOnset[i] {
				d.aboveOnsetSince[i] = now
			}
		} else {
			d.aboveOnsetSince[i] = time.Time{}
		}
		if abatement {
			if !d.belowAbatement[i] {
				d.belowAbatementSince[i] = now
			}
		} else {
			d.belowAbatementSince[i] = time.Time{}
		}
		d.aboveOnset[i] = onset
		d.belowAbatement[i] = abatement
	}
	for i := range cfg.Onset {
		if d.aboveOnset[i] && now.Sub(d.aboveOnsetSince[i]) >= cfg.OnsetDwell && d.state.Level < uint8(i+1) {
			d.state.Level = uint8(i + 1)
		}
	}
	for d.state.Level > 0 {
		i := d.state.Level - 1
		if !d.belowAbatement[i] || now.Sub(d.belowAbatementSince[i]) < cfg.AbatementDwell {
			break
		}
		d.state.Level--
	}
	return d.state
}
