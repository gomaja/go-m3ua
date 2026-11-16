package m3ua

import (
	"fmt"
	"math"
)

// CongestionAdmissionConfig opts DATA into cached outbound resource admission.
// ChargedRatioLimit zero selects the engineering default 0.9. A configured
// gate requires the Endpoint backlog monitor and a finite ratio in (0,1).
// At or above the ratio, or without a valid fresh sample, DATA returns a
// *DataWriteError with DataNotSent and ErrDataCongestionRefused. The gate reads
// cached state without a per-send socket query.
// It reserves no kernel memory and gives control no delivery guarantee.
type CongestionAdmissionConfig struct {
	// ChargedRatioLimit is the charged-memory/limit ratio at which new DATA is refused.
	ChargedRatioLimit float64
}

func validateCongestionAdmissionConfig(config *CongestionAdmissionConfig) error {
	if config == nil || config.ChargedRatioLimit == 0 {
		return nil
	}
	ratio := config.ChargedRatioLimit
	if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio <= 0 || ratio >= 1 {
		return fmt.Errorf("%w: DATA congestion admission ratio must be finite and between zero and one", ErrInvalidRoleConfiguration)
	}
	return nil
}

func (e *Endpoint) validateCongestionAdmissionMonitor(config *AssociationConfig) error {
	if config != nil && config.CongestionAdmission != nil && (e.transport == nil || e.transport.config.Disabled) {
		return fmt.Errorf("%w: DATA congestion admission requires the backlog monitor", ErrInvalidRoleConfiguration)
	}
	return nil
}

// Called only under sendMu, after existing scope, framing and method admission.
// The snapshot is immutable. A clock read checks freshness without a socket
// backlog/status query or any change to the detector's independent observation.
func (c *Association) checkCachedDataCongestionAdmission() error {
	config := c.cfg.CongestionAdmission
	if config == nil {
		return nil
	}
	if c.endpoint == nil || c.endpoint.transport == nil || c.endpoint.transport.config.Disabled {
		return ErrDataCongestionRefused
	}
	monitor := c.endpoint.transport
	sample := c.transportSnapshot.Load()
	if sample == nil || sample.Association != c.ID() || sample.Epoch != c.Epoch() ||
		!sample.SampleValid || !sample.Known || sample.Charged < 0 || sample.Limit <= 0 || sample.LastValidAt.IsZero() || monitor.now == nil {
		return ErrDataCongestionRefused
	}
	now := monitor.now()
	if now.Before(sample.LastValidAt) || now.Sub(sample.LastValidAt) >= monitor.config.StaleAfter {
		return ErrDataCongestionRefused
	}
	ratio := config.ChargedRatioLimit
	if ratio == 0 {
		ratio = .9
	}
	// Division avoids overflow when the kernel's charged-memory counters are large.
	if float64(sample.Charged)/float64(sample.Limit) >= ratio {
		return ErrDataCongestionRefused
	}
	return nil
}
