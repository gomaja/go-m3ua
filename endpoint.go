// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

// Role identifies the M3UA protocol role of an Endpoint.
//
// RFC 4666 Section 1.4.8 deliberately separates this role from which peer
// initiates the SCTP association: both an ASP and an SGP can dial or accept.
type Role uint8

const (
	// RoleASP is an Application Server Process, as defined by RFC 4666 Section 1.2.
	RoleASP Role = iota + 1
	// RoleSGP is a Signalling Gateway Process, as defined by RFC 4666 Section 1.2.
	RoleSGP
	// RoleIPSP is an IP Server Process, as defined by RFC 4666 Section 1.2.
	RoleIPSP
)

func (r Role) String() string {
	switch r {
	case RoleASP:
		return "ASP"
	case RoleSGP:
		return "SGP"
	case RoleIPSP:
		return "IPSP"
	default:
		return "unknown"
	}
}

// Endpoint owns an immutable M3UA protocol role independently of SCTP
// association initiation.
type Endpoint struct {
	role Role
}

// NewEndpoint creates an M3UA endpoint with an immutable protocol role.
func NewEndpoint(role Role) (*Endpoint, error) {
	switch role {
	case RoleASP, RoleSGP, RoleIPSP:
		return &Endpoint{role: role}, nil
	default:
		return nil, ErrUnsupportedRole
	}
}

// Role returns the endpoint's immutable M3UA protocol role.
func (e *Endpoint) Role() Role {
	if e == nil {
		return 0
	}
	return e.role
}

func (e *Endpoint) associationRole() (Role, error) {
	if e == nil {
		return 0, ErrUnsupportedRole
	}
	switch e.role {
	case RoleASP:
		return RoleASP, nil
	case RoleSGP:
		return RoleSGP, nil
	default:
		return 0, ErrUnsupportedRole
	}
}
