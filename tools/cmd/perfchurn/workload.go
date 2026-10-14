package main

import "fmt"

// payloadWorkload is the payload size schedule of the ledgered DATA.
type payloadWorkload string

// workloadMix is the section 2 deterministic mix: of every 100 consecutive
// messages of a flow, 90 carry 128 bytes, 9 carry 512 and 1 carries 4,096.
const workloadMix payloadWorkload = "mix"

func parseWorkload(name string) (payloadWorkload, error) {
	switch workload := payloadWorkload(name); workload {
	case workloadMix, "128", "512", "4096":
		return workload, nil
	default:
		return "", fmt.Errorf("payload %q is not mix, 128, 512 or 4096", name)
	}
}

// size is the payload length of one flow's message at sequence.
func (workload payloadWorkload) size(sequence uint64) int {
	switch workload {
	case "128":
		return 128
	case "512":
		return 512
	case "4096":
		return 4096
	case workloadMix:
		switch position := sequence % 100; {
		case position < 90:
			return 128
		case position < 99:
			return 512
		default:
			return 4096
		}
	default:
		return 0
	}
}

// Each stable association carries flowsPerAssociation ordered flows, each on
// its own Signalling Link Selection. RFC 4666 Section 1.4.7 keeps one SLS in
// sequence on one stream, so order is judged per flow and not across the
// association.
const (
	flowsPerAssociation = 4
	flowCount           = stableAssociations * flowsPerAssociation
)

func flowIndex(association, flow int) int {
	return association*flowsPerAssociation + flow
}
