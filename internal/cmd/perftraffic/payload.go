package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-m3ua/messages/params"
)

const (
	payloadHeaderSize     = 48
	flowCount             = 32
	testNetworkAppearance = uint32(7)
	payloadVersion        = byte(1)
)

// Payload kinds occupy header byte 7. Throughput DATA and echo requests are
// useful offered load validated by the receiver; echo replies travel back to
// the sender for RTT measurement and are never counted as useful deliveries.
const (
	kindData        = byte(0)
	kindEchoRequest = byte(1)
	kindEchoReply   = byte(2)
)

var payloadMagic = [4]byte{'M', '3', 'P', 'F'}

type messageIdentity struct {
	Cohort      string
	CohortHash  [16]byte
	Seed        uint64
	Association uint8
	Flow        uint8
	Sequence    uint64
	Kind        byte
}

type messageTuple struct {
	OriginatingPointCode    uint32
	DestinationPointCode    uint32
	ServiceIndicator        uint8
	NetworkIndicator        uint8
	MessagePriority         uint8
	SignallingLinkSelection uint8
	RoutingContext          uint32
}

type protocolData struct {
	OriginatingPointCode    uint32
	DestinationPointCode    uint32
	ServiceIndicator        uint8
	NetworkIndicator        uint8
	MessagePriority         uint8
	SignallingLinkSelection uint8
	Data                    []byte
}

type receivedMessage struct {
	ProtocolData         protocolData
	NetworkAppearance    uint32
	NetworkAppearanceSet bool
	RoutingContext       uint32
	RoutingContextSet    bool
}

func (message receivedMessage) clone() receivedMessage {
	message.ProtocolData.Data = append([]byte(nil), message.ProtocolData.Data...)
	return message
}

func tupleFor(flow uint8, association uint8) messageTuple {
	return messageTuple{
		OriginatingPointCode:    0x110000 + uint32(association)<<8 + uint32(flow),
		DestinationPointCode:    0x220000 + uint32(association)<<8 + uint32(flow),
		ServiceIndicator:        3 + 2*(flow%2),
		NetworkIndicator:        flow % 4,
		MessagePriority:         (flow / 4) % 4,
		SignallingLinkSelection: flow % 16,
		RoutingContext:          100 + uint32(flow),
	}
}

// dataRequest builds the typed per-message DATA this tuple describes. The
// Application Server scope repeats the configured Network Appearance because a
// request names its scope exactly; the stream is left unset so each message
// follows the stream its own SLS maps to, which is what keeps one flow in
// sequence.
func (tuple messageTuple) dataRequest(payload []byte) m3ua.DataRequest {
	return m3ua.DataRequest{
		AS: m3ua.ASKey{
			NetworkAppearance:    testNetworkAppearance,
			NetworkAppearanceSet: true,
			RoutingContext:       tuple.RoutingContext,
			RoutingContextSet:    true,
		},
		ProtocolData: params.ProtocolDataPayload{
			OriginatingPointCode:    tuple.OriginatingPointCode,
			DestinationPointCode:    tuple.DestinationPointCode,
			ServiceIndicator:        tuple.ServiceIndicator,
			NetworkIndicator:        tuple.NetworkIndicator,
			MessagePriority:         tuple.MessagePriority,
			SignallingLinkSelection: tuple.SignallingLinkSelection,
			Data:                    payload,
		},
	}
}

// reverseTuple swaps the point codes for SGP-to-ASP traffic (echo replies and
// the reverse direction of a bidirectional run). Service, network, priority,
// link selection and routing context stay unchanged.
func reverseTuple(tuple messageTuple) messageTuple {
	tuple.OriginatingPointCode, tuple.DestinationPointCode = tuple.DestinationPointCode, tuple.OriginatingPointCode
	return tuple
}

func cohortHash(cohort string) [16]byte {
	digest := sha256.Sum256([]byte(cohort))
	var short [16]byte
	copy(short[:], digest[:len(short)])
	return short
}

func buildPayload(identity messageIdentity, size int) []byte {
	if size < payloadHeaderSize {
		return nil
	}
	payload := make([]byte, size)
	copy(payload[0:4], payloadMagic[:])
	payload[4] = payloadVersion
	payload[5] = identity.Association
	payload[6] = identity.Flow
	payload[7] = identity.Kind
	digest := cohortHash(identity.Cohort)
	copy(payload[8:24], digest[:])
	binary.BigEndian.PutUint64(payload[24:32], identity.Seed)
	binary.BigEndian.PutUint64(payload[32:40], identity.Sequence)
	binary.BigEndian.PutUint32(payload[40:44], uint32(size))
	fillDeterministic(payload[payloadHeaderSize:], identity)
	return payload
}

func fillDeterministic(destination []byte, identity messageIdentity) {
	state := identity.Seed ^ identity.Sequence ^ uint64(identity.Association)<<40 ^ uint64(identity.Flow)<<32
	if state == 0 {
		state = 0x9e3779b97f4a7c15
	}
	for index := range destination {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		destination[index] = byte(state)
	}
}

func parsePayload(payload []byte) (messageIdentity, error) {
	if len(payload) < payloadHeaderSize {
		return messageIdentity{}, errors.New("payload shorter than fixture header")
	}
	if !bytes.Equal(payload[:4], payloadMagic[:]) {
		return messageIdentity{}, errors.New("payload magic mismatch")
	}
	if payload[4] != payloadVersion {
		return messageIdentity{}, fmt.Errorf("payload version %d is unsupported", payload[4])
	}
	if payload[7] > kindEchoReply || binary.BigEndian.Uint32(payload[44:48]) != 0 {
		return messageIdentity{}, errors.New("payload reserved fields are non-zero or the kind is unknown")
	}
	if int(binary.BigEndian.Uint32(payload[40:44])) != len(payload) {
		return messageIdentity{}, errors.New("payload length marker mismatch")
	}
	identity := messageIdentity{
		Association: payload[5],
		Flow:        payload[6],
		Kind:        payload[7],
		Seed:        binary.BigEndian.Uint64(payload[24:32]),
		Sequence:    binary.BigEndian.Uint64(payload[32:40]),
	}
	copy(identity.CohortHash[:], payload[8:24])
	return identity, nil
}

func validateMessage(message receivedMessage, cohort string, seed uint64, associations int, workload workload, kind byte, reverse bool) (messageIdentity, error) {
	identity, err := parsePayload(message.ProtocolData.Data)
	if err != nil {
		return messageIdentity{}, err
	}
	if identity.Kind != kind {
		return messageIdentity{}, errors.New("payload kind mismatch")
	}
	if identity.CohortHash != cohortHash(cohort) {
		return messageIdentity{}, errors.New("cohort mismatch")
	}
	if identity.Seed != seed {
		return messageIdentity{}, errors.New("seed mismatch")
	}
	if identity.Flow >= flowCount {
		return messageIdentity{}, errors.New("flow is out of range")
	}
	if identity.Sequence > (^uint64(0)-uint64(identity.Flow))/uint64(flowCount) {
		return messageIdentity{}, errors.New("global message index overflows")
	}
	globalIndex := identity.Sequence*uint64(flowCount) + uint64(identity.Flow)
	expectedSize := workload.size(globalIndex)
	if expectedSize == 0 || len(message.ProtocolData.Data) != expectedSize {
		return messageIdentity{}, errors.New("payload size does not match scheduled workload")
	}
	if int(identity.Association) >= associations || int(identity.Association) != int(identity.Flow)%associations {
		return messageIdentity{}, errors.New("association assignment mismatch")
	}
	tuple := tupleFor(identity.Flow, identity.Association)
	if reverse {
		tuple = reverseTuple(tuple)
	}
	if message.ProtocolData.OriginatingPointCode != tuple.OriginatingPointCode ||
		message.ProtocolData.DestinationPointCode != tuple.DestinationPointCode ||
		message.ProtocolData.ServiceIndicator != tuple.ServiceIndicator ||
		message.ProtocolData.NetworkIndicator != tuple.NetworkIndicator ||
		message.ProtocolData.MessagePriority != tuple.MessagePriority ||
		message.ProtocolData.SignallingLinkSelection != tuple.SignallingLinkSelection {
		return messageIdentity{}, errors.New("protocol data tuple mismatch")
	}
	if !message.NetworkAppearanceSet || message.NetworkAppearance != testNetworkAppearance {
		return messageIdentity{}, errors.New("network appearance mismatch")
	}
	if !message.RoutingContextSet || message.RoutingContext != tuple.RoutingContext {
		return messageIdentity{}, errors.New("routing context mismatch")
	}
	expected := buildPayload(messageIdentity{
		Cohort:      cohort,
		Seed:        identity.Seed,
		Association: identity.Association,
		Flow:        identity.Flow,
		Sequence:    identity.Sequence,
		Kind:        identity.Kind,
	}, expectedSize)
	if !bytes.Equal(message.ProtocolData.Data, expected) {
		return messageIdentity{}, errors.New("deterministic payload mismatch")
	}
	return identity, nil
}
