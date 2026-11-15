// Package congestionmethod parses the explicit SS7 method choice used by commands.
package congestionmethod

import (
	"fmt"

	"github.com/gomaja/go-m3ua"
)

// Parse requires one named method; it never chooses a default from NI or MP.
func Parse(value string) (m3ua.CongestionMethod, error) {
	switch value {
	case "international":
		return m3ua.CongestionInternational, nil
	case "national-with-priorities":
		return m3ua.CongestionNationalWithPriorities, nil
	case "national-without-priorities":
		return m3ua.CongestionNationalWithoutPriorities, nil
	default:
		return 0, fmt.Errorf("congestion-method %q: choose international, national-with-priorities or national-without-priorities", value)
	}
}
