package m3ua

import "sync"

// MTPStatusProcedure implements the message-count alternative of Q.704 (07/96)
// §§11.2.3.1 and 11.2.5 for one destination at one local MTP3 boundary. It is safe
// for concurrent calls and must not be copied after first use. It owns no map.
// Construct it with NewMTPStatusProcedure. Routed ASP MTPTransfer already offers
// automatically; use this helper for IPSP or application-owned routing instead.
type MTPStatusProcedure struct {
	mu          sync.Mutex
	profile     CongestionProfile
	destination MTPDestination
	interval    mtpStatusInterval
}

// NewMTPStatusProcedure validates and snapshots an explicit network method.
// National MaxLevel zero selects 3, as in CongestionConfig.
func NewMTPStatusProcedure(profile CongestionProfile) (*MTPStatusProcedure, error) {
	profile, err := normalizeCongestionProfile(profile)
	if err != nil {
		return nil, err
	}
	return &MTPStatusProcedure{profile: profile}, nil
}

// Offered records one original, validated local-user offer, including a failed
// transport attempt. It returns nil unless initial/every-eight status is due.
// The caller supplies its effective aggregate status and fans each returned
// primitive to each registered MTP3-User. Raw WriteData/WriteSignal do not offer
// automatically. A changed Destination, clear or expired received knowledge
// or transport uncertainty resets the interval. Call Reset for clear,
// uncertainty, expiry or lifecycle/method
// retirement between offers; the helper cannot observe those events itself.
// Order source updates, Reset and Offered at the application boundary, including
// peer-record expiry and renewal. Coalesced MTPIndications alone cannot reveal
// every intermediate ending; use an ordered source stream and Reset after lost
// continuity/resynchronization when that stream owns the boundary.
// National-with-priorities stays transition-driven under
// Q.704 (07/96) §11.2.4 and returns nil here. Level-4 users decide when to resume
// traffic under §11.2.3.3; an unknown snapshot is not measured abatement.
func (p *MTPStatusProcedure) Offered(status MTPDestinationStatus) *MTPIndication {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.destination != status.Destination {
		p.destination = status.Destination
		p.interval = mtpStatusInterval{}
	}
	return p.interval.offered(p.profile, status)
}

// Reset retires the current destination's offered-message interval. The next
// known-congested offer starts with the initial notification again.
func (p *MTPStatusProcedure) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.destination = MTPDestination{}
	p.interval = mtpStatusInterval{}
}

// remaining is always 0..7: no lifetime message count can wrap past a repeat.
// Q.704 (07/96) §§11.2.3.1/11.2.5 prescribe eight messages, not UserData octets.
type mtpStatusInterval struct{ remaining uint8 }

func continuedMTPStatusKnown(status MTPDestinationStatus) bool {
	return status.Congested && !status.ReceivedCongestionUnknown && !status.TransportCongestionUnknown
}

func (interval *mtpStatusInterval) offered(profile CongestionProfile, status MTPDestinationStatus) *MTPIndication {
	if !continuedMTPStatusKnown(status) {
		interval.remaining = 0
		return nil
	}
	if profile.Method != CongestionInternational && profile.Method != CongestionNationalWithoutPriorities {
		return nil
	}
	if interval.remaining > 0 {
		interval.remaining--
		return nil
	}
	interval.remaining = 7
	return &MTPIndication{Kind: MTPStatusIndication, Cause: MTPStatusSignallingNetworkCongested, Reason: MTPStatusOfferedTraffic, Destination: status}
}
