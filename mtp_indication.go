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
type MTPDestinationStatus struct {
	Destination        MTPDestination
	Availability       DestinationState
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
func (e *Endpoint) MTPDestinationStatuses() []MTPDestinationStatus {
	if e == nil || e.role != RoleASP || e.aspRoutes == nil {
		return nil
	}
	return e.aspRoutes.mtpDestinationStatuses()
}
