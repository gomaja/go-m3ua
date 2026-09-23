package main

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type routingIdentity struct {
	Cohort     string
	CohortHash [16]byte
	Seed       uint64
	Route      uint16
	Sequence   uint64
}

func planRouteMessage(cohort string, seed, index uint64) routingIdentity {
	return routingIdentity{Cohort: cohort, Seed: seed, Route: uint16(index % routingRouteCount), Sequence: index / routingRouteCount}
}

func routingGlobalIndex(identity routingIdentity) (uint64, error) {
	if identity.Route >= routingRouteCount || identity.Sequence > (^uint64(0)-uint64(identity.Route))/routingRouteCount {
		return 0, errors.New("routing identity is outside the representable schedule")
	}
	return identity.Sequence*routingRouteCount + uint64(identity.Route), nil
}

func buildRoutePayload(identity routingIdentity, size int) ([]byte, error) {
	if _, err := routingGlobalIndex(identity); err != nil {
		return nil, err
	}
	if identity.Cohort == "" || size < payloadHeaderSize || size > 4096 {
		return nil, errors.New("invalid routing cohort or payload size")
	}
	payload := make([]byte, size)
	copy(payload[:4], payloadMagic[:])
	payload[4] = 2
	digest := cohortHash(identity.Cohort)
	copy(payload[8:24], digest[:])
	binary.BigEndian.PutUint64(payload[24:32], identity.Seed)
	binary.BigEndian.PutUint64(payload[32:40], identity.Sequence)
	binary.BigEndian.PutUint32(payload[40:44], uint32(size))
	binary.BigEndian.PutUint32(payload[44:48], uint32(identity.Route))
	state := identity.Seed ^ identity.Sequence ^ uint64(identity.Route)<<32
	if state == 0 {
		state = 0x9e3779b97f4a7c15
	}
	for index := payloadHeaderSize; index < len(payload); index++ {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		payload[index] = byte(state)
	}
	return payload, nil
}

func parseRoutePayload(payload []byte) (routingIdentity, error) {
	if len(payload) < payloadHeaderSize || len(payload) > 4096 {
		return routingIdentity{}, errors.New("routing payload length is out of range")
	}
	if !bytes.Equal(payload[:4], payloadMagic[:]) || payload[4] != 2 || payload[5] != 0 || payload[6] != 0 || payload[7] != kindData ||
		binary.BigEndian.Uint32(payload[40:44]) != uint32(len(payload)) || binary.BigEndian.Uint32(payload[44:48]) >= routingRouteCount {
		return routingIdentity{}, errors.New("routing payload header mismatch")
	}
	identity := routingIdentity{Seed: binary.BigEndian.Uint64(payload[24:32]), Sequence: binary.BigEndian.Uint64(payload[32:40]), Route: uint16(binary.BigEndian.Uint32(payload[44:48]))}
	copy(identity.CohortHash[:], payload[8:24])
	if _, err := routingGlobalIndex(identity); err != nil {
		return routingIdentity{}, err
	}
	return identity, nil
}
