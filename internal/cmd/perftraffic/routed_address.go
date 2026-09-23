package main

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"

	"github.com/gomaja/go-sctp"
)

// routedPeerCount is the number of SGP endpoints in the routed topology. Each
// listens on its own port: -sctp-address names the first, and the others
// follow consecutively on the same address.
const routedPeerCount = 4

// splitRoutedAddress parses one host:port whose port leaves room for the four
// consecutive SGP listener ports. Multi-homed address lists are refused: the
// routed fixture pairs every association by its single transport address.
func splitRoutedAddress(value string) (string, int, error) {
	host, portText, err := net.SplitHostPort(value)
	if err != nil {
		return "", 0, fmt.Errorf("%q must be host:port: %w", value, err)
	}
	if host == "" || strings.Contains(host, "/") {
		return "", 0, fmt.Errorf("%q must name exactly one address", value)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535-(routedPeerCount-1) {
		return "", 0, fmt.Errorf("%q must leave room for four consecutive SGP ports below 65536", value)
	}
	return host, port, nil
}

// resolveRoutedIP resolves host to exactly one concrete unicast address. A
// wildcard would make the actual local transport ambiguous, and a name that
// resolves to several addresses would make it unclear which one the peer sees.
func resolveRoutedIP(host string) (net.IP, error) {
	var candidates []net.IP
	if literal := net.ParseIP(host); literal != nil {
		candidates = []net.IP{literal}
	} else {
		resolved, err := net.LookupIP(host)
		if err != nil {
			return nil, fmt.Errorf("resolve %q: %w", host, err)
		}
		candidates = resolved
	}
	unique := make(map[netip.Addr]bool, len(candidates))
	var chosen netip.Addr
	for _, candidate := range candidates {
		address, valid := netip.AddrFromSlice(candidate)
		if !valid {
			continue
		}
		address = address.Unmap()
		if !unique[address] {
			unique[address] = true
			chosen = address
		}
	}
	if len(unique) != 1 {
		return nil, fmt.Errorf("%q must resolve to exactly one address, not %d", host, len(unique))
	}
	if chosen.IsUnspecified() || chosen.IsMulticast() || chosen.Zone() != "" || chosen == netip.AddrFrom4([4]byte{255, 255, 255, 255}) {
		return nil, fmt.Errorf("%q must be one concrete unicast address, not a wildcard", host)
	}
	return net.IP(chosen.AsSlice()), nil
}

// routedPeerAddresses returns the four SGP listener addresses named by one
// host:port: the same concrete address on the given port and the three that
// follow it.
func routedPeerAddresses(value string) ([]*sctp.SCTPAddr, error) {
	host, port, err := splitRoutedAddress(value)
	if err != nil {
		return nil, err
	}
	ip, err := resolveRoutedIP(host)
	if err != nil {
		return nil, fmt.Errorf("routed SGP address: %w", err)
	}
	addresses := make([]*sctp.SCTPAddr, routedPeerCount)
	for index := range addresses {
		addresses[index] = &sctp.SCTPAddr{IPAddrs: []net.IPAddr{{IP: append(net.IP(nil), ip...)}}, Port: port + index}
	}
	return addresses, nil
}

// routedLocalAddress returns the ASP's concrete local bind with an ephemeral
// port, so each of the eight associations gets its own local port on the one
// address the SGPs see.
func routedLocalAddress(value string) (*sctp.SCTPAddr, error) {
	host, portText, err := net.SplitHostPort(value)
	if err != nil {
		return nil, fmt.Errorf("%q must be host:0: %w", value, err)
	}
	if portText != "0" {
		return nil, errors.New("the routed ASP binds an ephemeral local port; use port 0")
	}
	if host == "" || strings.Contains(host, "/") {
		return nil, fmt.Errorf("%q must name exactly one concrete address", value)
	}
	ip, err := resolveRoutedIP(host)
	if err != nil {
		return nil, fmt.Errorf("routed ASP local address: %w", err)
	}
	return &sctp.SCTPAddr{IPAddrs: []net.IPAddr{{IP: ip}}, Port: 0}, nil
}
