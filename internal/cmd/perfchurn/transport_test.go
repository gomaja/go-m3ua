package main

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

func TestInterfacesAreReadFromSysfs(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{"bonding_masters": "\n", "eth0/mtu": "1500\n", "eth0/tx_queue_len": "1000\n", "eth0/gso_max_size": "65536\n",
		"eth0/gso_max_segs": "65535\n", "eth0/gro_max_size": "65536\n", "lo/mtu": "65536\n"}
	for name, content := range files {
		if err := os.MkdirAll(filepath.Join(root, filepath.Dir(name)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	interfaces := readInterfaces(root, func(string) (map[string]bool, map[string]string) { return map[string]bool{"tso": true}, nil })
	eth0, lo := interfaces["eth0"], interfaces["lo"]
	if eth0.MTU != 1500 || eth0.TxQueueLen != 1000 || eth0.GSOMaxSize != 65536 || eth0.GSOMaxSegs != 65535 || eth0.GROMaxSize != 65536 ||
		!eth0.Offloads["tso"] || len(eth0.Unreadable) != 0 {
		t.Fatalf("eth0 %+v", eth0)
	}
	if lo.MTU != 65536 || lo.GSOMaxSize != -1 || len(lo.Unreadable) != 4 {
		t.Fatalf("lo %+v: unreadable values must be recorded, not zeroed", lo)
	}
	if _, listed := interfaces["bonding_masters"]; listed || len(interfaces) != 2 {
		t.Fatalf("a sysfs file that is not an interface was listed: %v", interfaces)
	}
	if len(readInterfaces(filepath.Join(root, "absent"), nil)) != 0 {
		t.Fatal("an absent sysfs produced interfaces")
	}
}

func sctpAddress(port int) *sctp.SCTPAddr {
	return &sctp.SCTPAddr{IPAddrs: []net.IPAddr{{IP: net.ParseIP("172.31.250.10")}}, Port: port}
}

func TestAssociationEvidenceJoinsSocketsKernelAndLibrary(t *testing.T) {
	source := procSource{root: fakeProc(t)}
	snapshots := []m3ua.AssociationSnapshot{
		{Association: 11, LocalAddr: sctpAddress(2905), RemoteAddr: sctpAddress(30000),
			SCTP: &m3ua.AssociationStatus{InboundStreams: 16, OutboundStreams: 16, PrimaryMTU: 1500, FragmentationPoint: 1452}},
		{Association: 12, LocalAddr: sctpAddress(2905), RemoteAddr: sctpAddress(30103), SCTPError: errors.New("status unavailable")},
	}
	options := func(fd int) (socketOptions, error) {
		if fd == 6 {
			return socketOptions{}, errors.New("bad descriptor")
		}
		return socketOptions{SendBuffer: 425984, ReceiveBuffer: 425984, NoDelay: 1, SACKDelay: 0, SACKFrequency: 1}, nil
	}
	evidence := associationEvidenceFor(source, snapshots, func(local, remote int) int { return remote }, options)
	if len(evidence) != 2 {
		t.Fatalf("evidence %+v", evidence)
	}
	first, second := evidence[0], evidence[1]
	if first.Association != 11 || first.StableIndex != 0 || first.InboundStreams != 16 || first.PrimaryMTU != 1500 ||
		first.KernelInStreams != 17 || first.KernelSendBuffer != 212992 || first.SocketSendBuffer != 425984 || first.SocketNoDelay != 1 ||
		first.SocketSACKFrequency != 1 || first.Descriptor != 3 {
		t.Fatalf("first %+v", first)
	}
	if second.Association != 12 || second.StableIndex != -1 || second.StatusError == "" || second.SocketError == "" || second.Descriptor != 6 {
		t.Fatalf("second %+v", second)
	}
}
