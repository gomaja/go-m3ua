package main

import (
	"net"
	"net/netip"
	"testing"
)

// The peer binds its dials to -local-ip in the address's own family. An IPv6
// address used to go through To4, which is nil for it, and the conversion to a
// four-byte array panicked before the first dial.
func TestPeerLocalAddressKeepsTheConfiguredFamily(t *testing.T) {
	for _, tc := range []struct {
		ip   string
		want netip.Addr
	}{
		{ip: "127.0.0.1", want: netip.MustParseAddr("127.0.0.1")},
		{ip: "::1", want: netip.MustParseAddr("::1")},
		{ip: "2001:db8::7", want: netip.MustParseAddr("2001:db8::7")},
	} {
		peer := &peerRun{localIP: net.ParseIP(tc.ip)}
		address := peer.localAddress(2905)
		if len(address.IPs) != 1 || address.IPs[0] != tc.want || address.Port != 2905 {
			t.Errorf("localAddress for %s = %+v, want [%s]:2905", tc.ip, address, tc.want)
		}
	}
}
