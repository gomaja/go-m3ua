// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

// MTPIndicationKind identifies an RFC 4666 Section 1.6.1 MTP3-User indication
// primitive.
type MTPIndicationKind uint8

const (
	// MTPPauseIndication reports that a destination became unavailable.
	MTPPauseIndication MTPIndicationKind = iota + 1
	// MTPResumeIndication reports that a previously unavailable destination
	// became available or restricted.
	MTPResumeIndication
	// MTPStatusIndication reports a restriction or congestion change without a
	// transition into or out of destination unavailability.
	MTPStatusIndication
)

func (kind MTPIndicationKind) String() string {
	switch kind {
	case MTPPauseIndication:
		return "MTP-PAUSE"
	case MTPResumeIndication:
		return "MTP-RESUME"
	case MTPStatusIndication:
		return "MTP-STATUS"
	default:
		return "UNKNOWN"
	}
}

// MTPDestination identifies a provisioned ASP destination or destination
// range. Mask is the number of wildcarded low-order point-code bits.
type MTPDestination struct {
	MTPRoute  MTPRouteID
	PointCode uint32
	Mask      uint8
}

// MTPDestinationStatus is the ASP's derived view of one MTP destination over
// every provisioned Signalling Gateway route.
//
// It is the MTP3-User's aggregate view, and it reads a silent Signalling
// Gateway as one that can carry traffic. RFC 4666 Appendix A.2.2 defines
// capability negatively: an SG "is capable of transferring traffic to a
// provisioned SS7 destination X if an SCTP association with at least one SGP
// of the SG is established, the SGP has returned an acknowledgement to the ASP
// to indicate that the ASP is actively handling traffic for that destination X,
// the SGP has not indicated that the destination X is inaccessible, and the SGP
// has not indicated MTP Restart." A destination no SG has reported on satisfies
// every clause of that sentence, so the aggregate reports it Available and no
// MTP-PAUSE is raised for a destination nobody has said anything about.
//
// Endpoint.MTPTransfer answers the narrower question differently, and
// deliberately: per-path selection reads the canonical SSNM store, where an
// absent availability record is absent rather than favourable, and refuses the
// candidate with ErrDestinationStateUnknown unless
// ASPRoutingConfig.AllowUnknownDestinations opts back into the Appendix A.2.2
// reading. The two answers can therefore disagree for exactly one case — a
// destination with an established, activated, silent Signalling Gateway — where
// this snapshot says Available and a transfer to it is refused. Neither is
// derived from the other: this one is the aggregate MTP3-User status of
// Section 4.5.2.2, and that one is a decision about one candidate.
type MTPDestinationStatus struct {
	Destination        MTPDestination
	Availability       DestinationAvailability
	Congested          bool
	CongestionLevel    uint8
	CongestionLevelSet bool
}

// MTPIndication carries an MTP-PAUSE, MTP-RESUME, or MTP-STATUS indication to
// the MTP3-User. RFC 4666 Section 4.5.2.2 requires these to follow the derived
// status over all routes rather than any one SG report. ResyncRequired means
// the bounded indication queue overflowed; further deltas are suppressed until
// the marker is read, and the receiver must query the Endpoint's current
// destination state.
type MTPIndication struct {
	Kind           MTPIndicationKind
	Destination    MTPDestinationStatus
	ResyncRequired bool
}

// MTPIndications returns the ASP Endpoint's derived MTP3-User indication
// stream. It is closed by Endpoint.Close, not by an individual Association.
//
// It reports changes in the derived status described by MTPDestinationStatus,
// which is the aggregate over every provisioned Signalling Gateway route and
// not the per-candidate decision MTPTransfer makes. An MTP-RESUME is not a
// promise that the next MTPTransfer will be admitted.
//
// It is nil for any Endpoint that is not an ASP with an ASPConfig, and a
// receive from a nil channel blocks forever. An ASP that left
// ASPConfig.Routing nil has the channel but nothing ever arrives on it: these
// indications are derived from provisioned MTP Routes, and that ASP has none.
// Such an application owns outbound selection itself and consumes
// Endpoint.SubscribeSSNM instead.
func (e *Endpoint) MTPIndications() <-chan *MTPIndication {
	if e == nil || e.role != RoleASP || e.aspRoutes == nil {
		return nil
	}
	return e.aspRoutes.indications
}

// MTPDestinationStatus returns the ASP Endpoint's current derived status for a
// provisioned destination. The boolean is false for an invalid or unprovisioned
// MTP Route or point-code range, or when the requested range contains mixed
// statuses. Use MTPDestinationStatuses to enumerate canonical mixed ranges.
func (e *Endpoint) MTPDestinationStatus(destination MTPDestination) (MTPDestinationStatus, bool) {
	if e == nil || e.role != RoleASP || e.aspRoutes == nil {
		return MTPDestinationStatus{}, false
	}
	return e.aspRoutes.mtpDestinationStatus(destination)
}

// MTPDestinationStatuses returns a deterministic snapshot of the ASP
// Endpoint's canonical, non-overlapping destination ranges. It is the
// authoritative resynchronization source after an MTPIndication reports
// ResyncRequired. The returned slice is owned by the caller.
//
// Like MTPDestinationStatus, these are derived aggregate statuses.
// Endpoint.SSNMKnowledge is the other view: what each Signalling Gateway
// actually reported, per canonical Application Server, with absence
// distinguishable from a report.
func (e *Endpoint) MTPDestinationStatuses() []MTPDestinationStatus {
	if e == nil || e.role != RoleASP || e.aspRoutes == nil {
		return nil
	}
	return e.aspRoutes.mtpDestinationStatuses()
}
