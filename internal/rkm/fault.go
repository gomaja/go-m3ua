// Package rkm carries decoder faults between the parameter, message and
// association layers without making receive-only state part of the public API.
package rkm

// SemanticFault identifies a framed Routing Key that can be answered with a
// Registration Result under RFC 4666 Sections 3.6.2 and 4.4.1.
type SemanticFault struct {
	LocalIdentifier uint32
	Status          uint32
	Cause           error
}

func (f *SemanticFault) Error() string { return f.Cause.Error() }
func (f *SemanticFault) Unwrap() error { return f.Cause }
