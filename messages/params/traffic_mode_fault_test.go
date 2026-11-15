package params

import (
	"errors"
	"testing"
)

func TestUndefinedTrafficModeRemainsStrict(t *testing.T) {
	for _, mode := range []uint32{0, 4, 0xffffffff} {
		p := NewTrafficModeType(mode)
		if _, err := p.MarshalBinary(); !errors.Is(err, ErrInvalidValue) || !errors.Is(err, ErrInvalidTrafficMode) {
			t.Fatalf("mode %d marshal = %v", mode, err)
		}
		raw := append([]byte{0, byte(TrafficModeType), 0, 8}, p.Data...)
		if _, err := Parse(raw); !errors.Is(err, ErrInvalidValue) || !errors.Is(err, ErrInvalidTrafficMode) {
			t.Fatalf("mode %d parse = %v", mode, err)
		}
	}
}
