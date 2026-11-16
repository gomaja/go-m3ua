package m3ua

import "fmt"

// IdentityLengthError reports a local identity that exceeds its octet ceiling.
// It wraps ErrInvalidParameterValue; configuration entry points additionally
// preserve their existing configuration-error classification. It stores no
// copy of the offending identity.
type IdentityLengthError struct {
	// Field identifies the library input field, never the supplied name.
	Field string
	// Length is the supplied identity's byte length.
	Length int
	// Limit is the maximum permitted byte length.
	Limit int
}

// Error describes the bounded field and octet counts.
func (e *IdentityLengthError) Error() string {
	return fmt.Sprintf("%s identity is %d octets; limit is %d", e.Field, e.Length, e.Limit)
}

// Unwrap preserves the invalid-parameter classification.
func (*IdentityLengthError) Unwrap() error { return ErrInvalidParameterValue }

func validateIdentityLength(field, value string, limit int) error {
	if len(value) > limit {
		return &IdentityLengthError{Field: field, Length: len(value), Limit: limit}
	}
	return nil
}
func validateRemoteIdentity(key SGASKey) error {
	if err := validateIdentityLength("SignallingGatewayID", string(key.SignallingGateway), maxObservationIdentityBytes); err != nil {
		return err
	}
	return validateIdentityLength("RemoteASID", string(key.ApplicationServer), maxObservationIdentityBytes)
}
