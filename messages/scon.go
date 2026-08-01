// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package messages

import (
	"fmt"

	"github.com/gomaja/go-m3ua/messages/params"
)

// SignallingCongestion is a SignallingCongestion type of M3UA message.
//
// Spec: 3.4.4, RFC4666.
type SignallingCongestion struct {
	*Header
	NetworkAppearance     *params.Param
	RoutingContext        *params.Param
	AffectedPointCode     *params.Param
	ConcernedDestination  *params.Param
	CongestionIndications *params.Param
	InfoString            *params.Param
	// Others holds parameters this version does not define.
	//
	// RFC 4666 Section 3: "For forward compatibility, all Message Types may
	// have attached parameters even if none are specified in this version."
	// They are kept rather than rejected so a peer running a later extension
	// still interoperates, and so a message that must be echoed back can be
	// echoed whole.
	Others []*params.Param
}

// NewSignallingCongestion creates a new SignallingCongestion.
func NewSignallingCongestion(nwApr, rtCtx, apc, cdst, ind, info *params.Param) *SignallingCongestion {
	s := &SignallingCongestion{
		Header: &Header{
			Version:  1,
			Reserved: 0,
			Class:    MsgClassSSNM,
			Type:     MsgTypeSignallingCongestion,
		},
		NetworkAppearance:     nwApr,
		RoutingContext:        rtCtx,
		AffectedPointCode:     apc,
		ConcernedDestination:  cdst,
		CongestionIndications: ind,
		InfoString:            info,
	}
	s.SetLength()

	return s
}

// MarshalBinary returns the byte sequence generated from a SignallingCongestion.
func (s *SignallingCongestion) MarshalBinary() ([]byte, error) {
	b := make([]byte, s.MarshalLen())
	if err := s.MarshalTo(b); err != nil {
		return nil, err
	}
	return b, nil
}

// MarshalTo puts the byte sequence in the byte array given as b.
func (s *SignallingCongestion) MarshalTo(b []byte) error {
	if err := requireParameter(s.AffectedPointCode, "Affected Point Code"); err != nil {
		return err
	}
	if len(b) < s.MarshalLen() {
		return ErrTooShortToMarshalBinary
	}

	s.Header.Payload = make([]byte, s.MarshalLen()-8)

	var offset = 0
	if param := s.NetworkAppearance; param != nil {
		if err := param.MarshalTo(s.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}
	if param := s.RoutingContext; param != nil {
		if err := param.MarshalTo(s.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}
	if param := s.AffectedPointCode; param != nil {
		if err := param.MarshalTo(s.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}
	if param := s.ConcernedDestination; param != nil {
		if err := param.MarshalTo(s.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}
	if param := s.CongestionIndications; param != nil {
		if err := param.MarshalTo(s.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}
	if param := s.InfoString; param != nil {
		if err := param.MarshalTo(s.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}
	if err := marshalOtherParams(s.Header.Payload, offset, s.Others); err != nil {
		return err
	}

	return s.Header.MarshalTo(b)
}

// ParseSignallingCongestion decodes given byte sequence as a SignallingCongestion.
func ParseSignallingCongestion(b []byte) (*SignallingCongestion, error) {
	s := &SignallingCongestion{}
	if err := s.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return s, nil
}

// UnmarshalBinary sets the values retrieved from byte sequence in a M3UA common header.
func (s *SignallingCongestion) UnmarshalBinary(b []byte) error {
	*s = SignallingCongestion{}
	var err error
	s.Header, err = parseTypedHeader(b, MsgClassSSNM, MsgTypeSignallingCongestion)
	if err != nil {
		return err
	}

	prs, err := params.ParseMultiParams(s.Header.Payload)
	if err != nil {
		return err
	}
	for _, pr := range prs {
		switch pr.Tag {
		case params.NetworkAppearance:
			if s.NetworkAppearance != nil {
				return ErrInvalidParameter
			}
			s.NetworkAppearance = pr
		case params.RoutingContext:
			if s.RoutingContext != nil {
				return ErrInvalidParameter
			}
			s.RoutingContext = pr
		case params.AffectedPointCode:
			if s.AffectedPointCode != nil {
				return ErrInvalidParameter
			}
			s.AffectedPointCode = pr
		case params.ConcernedDestination:
			if s.ConcernedDestination != nil {
				return ErrInvalidParameter
			}
			s.ConcernedDestination = pr
		case params.CongestionIndications:
			if s.CongestionIndications != nil {
				return ErrInvalidParameter
			}
			s.CongestionIndications = pr
		case params.InfoString:
			if s.InfoString != nil {
				return ErrInvalidParameter
			}
			s.InfoString = pr
		default:
			s.Others = append(s.Others, pr)
		}
	}
	return requireParameter(s.AffectedPointCode, "Affected Point Code")
}

// SetLength sets the length in Length field.
func (s *SignallingCongestion) SetLength() {
	if param := s.NetworkAppearance; param != nil {
		param.SetLength()
	}
	if param := s.RoutingContext; param != nil {
		param.SetLength()
	}
	if param := s.AffectedPointCode; param != nil {
		param.SetLength()
	}
	if param := s.ConcernedDestination; param != nil {
		param.SetLength()
	}
	if param := s.CongestionIndications; param != nil {
		param.SetLength()
	}
	if param := s.InfoString; param != nil {
		param.SetLength()
	}
	setParamLengths(s.Others)

	s.Header.Length = uint32(s.MarshalLen())
}

// MarshalLen returns the serial length of SignallingCongestion.
func (s *SignallingCongestion) MarshalLen() int {
	l := 8
	if param := s.NetworkAppearance; param != nil {
		l += param.MarshalLen()
	}
	if param := s.RoutingContext; param != nil {
		l += param.MarshalLen()
	}
	if param := s.AffectedPointCode; param != nil {
		l += param.MarshalLen()
	}
	if param := s.ConcernedDestination; param != nil {
		l += param.MarshalLen()
	}
	if param := s.CongestionIndications; param != nil {
		l += param.MarshalLen()
	}
	if param := s.InfoString; param != nil {
		l += param.MarshalLen()
	}
	l += paramsMarshalLen(s.Others)
	return l
}

// String returns the SignallingCongestion values in human readable format.
func (s *SignallingCongestion) String() string {
	if s == nil {
		return ""
	}
	return fmt.Sprintf("{Header: %s, NetworkAppearance: %s, RoutingContext: %s, AffectedPointCode: %s, ConcernedDestination: %s, CongestionIndications: %s, InfoString: %s}",
		s.Header.String(),
		s.NetworkAppearance.String(),
		s.RoutingContext.String(),
		s.AffectedPointCode.String(),
		s.ConcernedDestination.String(),
		s.CongestionIndications.String(),
		s.InfoString.String(),
	)
}

// Version returns the version of M3UA in int.
func (s *SignallingCongestion) Version() uint8 {
	return s.Header.Version
}

// MessageType returns the message type in int.
func (s *SignallingCongestion) MessageType() uint8 {
	return MsgTypeSignallingCongestion
}

// MessageClass returns the message class in int.
func (s *SignallingCongestion) MessageClass() uint8 {
	return MsgClassSSNM
}

// MessageClassName returns the name of message class.
func (s *SignallingCongestion) MessageClassName() string {
	return MsgClassNameSSNM
}

// MessageTypeName returns the name of message type.
func (s *SignallingCongestion) MessageTypeName() string {
	return "Signalling Congestion"
}
