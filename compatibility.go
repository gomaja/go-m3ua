package m3ua

import "github.com/gomaja/go-m3ua/messages"

// CompatibilityPolicy configures explicit receive-side tolerance for classified
// peer protocol violations. The zero value is RFC-strict.
type CompatibilityPolicy struct {
	Tolerator ProtocolTolerator
}

// ProtocolDecision is the action a compatibility tolerator chooses for a
// classified protocol violation.
type ProtocolDecision = messages.ProtocolDecision

const (
	// ProtocolReject keeps the RFC-strict behaviour.
	ProtocolReject = messages.ProtocolReject
	// ProtocolAccept accepts the violation and keeps the offending parameter.
	ProtocolAccept = messages.ProtocolAccept
	// ProtocolDropParameter accepts the message but discards the offending
	// optional parameter.
	ProtocolDropParameter = messages.ProtocolDropParameter
	// ProtocolUseLocalDefault is reserved for future classified violations whose
	// safe action is to infer a configured local value.
	ProtocolUseLocalDefault = messages.ProtocolUseLocalDefault
)

// ProtocolViolationKind identifies a protocol violation known well enough for a
// caller to make an explicit compatibility decision.
type ProtocolViolationKind = messages.ProtocolViolationKind

const (
	// ViolationInvalidOptionalInfoString is an optional INFO String whose value is
	// no more than 255 octets but is not valid UTF-8.
	ViolationInvalidOptionalInfoString = messages.ViolationInvalidOptionalInfoString
)

// ProtocolViolation describes one classified receive-side protocol violation.
type ProtocolViolation = messages.ProtocolViolation

// ProtocolTolerator decides whether a classified protocol violation should be
// tolerated. It is called only after structural lengths are safe.
type ProtocolTolerator = messages.ProtocolTolerator

// ToleratorFunc adapts a function into a ProtocolTolerator.
type ToleratorFunc = messages.ToleratorFunc

// AcceptInvalidOptionalInfoString returns a policy that accepts optional INFO
// String parameters whose value is not valid UTF-8 while preserving the raw
// parameter bytes. All other protocol violations remain rejected.
func AcceptInvalidOptionalInfoString() CompatibilityPolicy {
	return CompatibilityPolicy{
		Tolerator: ToleratorFunc(func(v ProtocolViolation) ProtocolDecision {
			if v.Kind == ViolationInvalidOptionalInfoString {
				return ProtocolAccept
			}
			return ProtocolReject
		}),
	}
}

func (c *Conn) parseInboundMessage(raw []byte) (messages.M3UA, error) {
	if c == nil || c.cfg == nil {
		return messages.Parse(raw)
	}
	return messages.ParseWithOptions(raw, messages.ParseOptions{
		Tolerator: c.cfg.Compatibility.Tolerator,
	})
}
