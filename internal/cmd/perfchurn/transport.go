package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gomaja/go-m3ua"
)

// transportEvidence is the section 1 record of the transport under test:
// what the fixture configures, what each association negotiated and what its
// socket reports, and the container's interfaces.
type transportEvidence struct {
	ConfiguredNoDelay       bool                         `json:"configured_sctp_nodelay"`
	ConfiguredSACKDelay     uint32                       `json:"configured_sack_delay_ms"`
	ConfiguredSACKFrequency uint32                       `json:"configured_sack_frequency"`
	Associations            []associationEvidence        `json:"associations"`
	Interfaces              map[string]interfaceEvidence `json:"interfaces"`
}

// associationEvidence joins one Association's library status, its row in
// /proc/net/sctp/assocs and getsockopt on its own socket.
type associationEvidence struct {
	Association        uint64 `json:"association"`
	StableIndex        int    `json:"stable_index"`
	LocalPort          int    `json:"local_port"`
	RemotePort         int    `json:"remote_port"`
	InboundStreams     uint16 `json:"inbound_streams"`
	OutboundStreams    uint16 `json:"outbound_streams"`
	PrimaryMTU         uint32 `json:"primary_mtu"`
	FragmentationPoint uint32 `json:"fragmentation_point"`
	StatusError        string `json:"status_error,omitempty"`

	KernelInStreams     int    `json:"kernel_in_streams"`
	KernelOutStreams    int    `json:"kernel_out_streams"`
	KernelSendBuffer    int    `json:"kernel_sndbuf"`
	KernelReceiveBuffer int    `json:"kernel_rcvbuf"`
	KernelError         string `json:"kernel_error,omitempty"`
	Descriptor          int    `json:"descriptor"`
	SocketSendBuffer    int    `json:"so_sndbuf"`
	SocketReceiveBuffer int    `json:"so_rcvbuf"`
	SocketNoDelay       int    `json:"sctp_nodelay"`
	SocketSACKDelay     uint32 `json:"sctp_sack_delay_ms"`
	SocketSACKFrequency uint32 `json:"sctp_sack_frequency"`
	SocketError         string `json:"socket_error,omitempty"`
}

// interfaceEvidence is one network interface from sysfs, with its offloads
// from the legacy ethtool queries. A value that could not be read is -1 and
// named in Unreadable, never recorded as zero.
type interfaceEvidence struct {
	MTU           int               `json:"mtu"`
	TxQueueLen    int               `json:"tx_queue_len"`
	GSOMaxSize    int               `json:"gso_max_size"`
	GSOMaxSegs    int               `json:"gso_max_segs"`
	GROMaxSize    int               `json:"gro_max_size"`
	Unreadable    []string          `json:"unreadable,omitempty"`
	Offloads      map[string]bool   `json:"offloads,omitempty"`
	OffloadErrors map[string]string `json:"offload_errors,omitempty"`
}

type socketOptions struct {
	SendBuffer    int
	ReceiveBuffer int
	NoDelay       int
	SACKDelay     uint32
	SACKFrequency uint32
}

func readInterfaces(root string, offloads func(string) (map[string]bool, map[string]string)) map[string]interfaceEvidence {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil
	}
	interfaces := make(map[string]interfaceEvidence, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		evidence := interfaceEvidence{}
		for _, field := range []struct {
			file  string
			value *int
		}{{"mtu", &evidence.MTU}, {"tx_queue_len", &evidence.TxQueueLen}, {"gso_max_size", &evidence.GSOMaxSize},
			{"gso_max_segs", &evidence.GSOMaxSegs}, {"gro_max_size", &evidence.GROMaxSize}} {
			content, err := os.ReadFile(filepath.Join(root, name, field.file))
			value, parseErr := strconv.Atoi(strings.TrimSpace(string(content)))
			if err != nil || parseErr != nil {
				*field.value = -1
				evidence.Unreadable = append(evidence.Unreadable, field.file)
				continue
			}
			*field.value = value
		}
		if offloads != nil {
			evidence.Offloads, evidence.OffloadErrors = offloads(name)
		}
		interfaces[name] = evidence
	}
	return interfaces
}

// socketDescriptors maps each socket inode of this process to its
// descriptor number.
func (source procSource) socketDescriptors() map[uint64]int {
	descriptors := map[uint64]int{}
	if source.root == "" {
		return descriptors
	}
	directory := filepath.Join(source.root, "self", "fd")
	entries, err := os.ReadDir(directory)
	if err != nil {
		return descriptors
	}
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join(directory, entry.Name()))
		if err != nil {
			continue
		}
		inodeText, isSocket := strings.CutPrefix(target, "socket:[")
		if !isSocket {
			continue
		}
		inode, err := strconv.ParseUint(strings.TrimSuffix(inodeText, "]"), 10, 64)
		descriptor, fdErr := strconv.Atoi(entry.Name())
		if err == nil && fdErr == nil {
			descriptors[inode] = descriptor
		}
	}
	return descriptors
}

// associationEvidenceFor builds one row per snapshot. peerPort says which of
// the two ports identifies the peer (the remote one at the ASP, the local one
// at the dialing peer), which is what classifies a stable association.
func associationEvidenceFor(source procSource, snapshots []m3ua.AssociationSnapshot, peerPort func(local, remote int) int,
	options func(fd int) (socketOptions, error),
) []associationEvidence {
	type ports struct{ local, remote int }
	rows := map[ports]sctpAssoc{}
	kernelError := ""
	if source.root == "" {
		kernelError = errUnavailable.Error()
	} else if content, err := os.ReadFile(filepath.Join(source.root, "net", "sctp", "assocs")); err != nil {
		kernelError = err.Error()
	} else if parsed, err := parseSCTPAssocs(content); err != nil {
		kernelError = err.Error()
	} else {
		for _, row := range parsed {
			rows[ports{row.LocalPort, row.RemotePort}] = row
		}
	}
	descriptors := source.socketDescriptors()
	evidence := make([]associationEvidence, 0, len(snapshots))
	for _, snapshot := range snapshots {
		row := associationEvidence{Association: uint64(snapshot.Association), StableIndex: -1, Descriptor: -1, KernelError: kernelError}
		if snapshot.LocalAddr != nil {
			row.LocalPort = snapshot.LocalAddr.Port
		}
		if snapshot.RemoteAddr != nil {
			row.RemotePort = snapshot.RemoteAddr.Port
		}
		if role, err := classifyPort(peerPort(row.LocalPort, row.RemotePort)); err == nil && role.Stable {
			row.StableIndex = role.StableIndex
		}
		if status := snapshot.SCTP; status != nil {
			row.InboundStreams, row.OutboundStreams = status.InboundStreams, status.OutboundStreams
			row.PrimaryMTU, row.FragmentationPoint = status.PrimaryMTU, status.FragmentationPoint
		} else if snapshot.SCTPError != nil {
			row.StatusError = snapshot.SCTPError.Error()
		}
		kernel, found := rows[ports{row.LocalPort, row.RemotePort}]
		if !found {
			if row.KernelError == "" {
				row.KernelError = "no /proc/net/sctp/assocs row for these ports"
			}
			evidence = append(evidence, row)
			continue
		}
		row.KernelInStreams, row.KernelOutStreams = kernel.InStreams, kernel.OutStreams
		row.KernelSendBuffer, row.KernelReceiveBuffer = kernel.SendBuffer, kernel.ReceiveBuffer
		descriptor, owned := descriptors[kernel.Inode]
		if !owned {
			row.SocketError = "the association's socket is not one of this process's descriptors"
			evidence = append(evidence, row)
			continue
		}
		row.Descriptor = descriptor
		if values, err := options(descriptor); err != nil {
			row.SocketError = err.Error()
		} else {
			row.SocketSendBuffer, row.SocketReceiveBuffer, row.SocketNoDelay = values.SendBuffer, values.ReceiveBuffer, values.NoDelay
			row.SocketSACKDelay, row.SocketSACKFrequency = values.SACKDelay, values.SACKFrequency
		}
		evidence = append(evidence, row)
	}
	return evidence
}

// collectTransport records the transport evidence of one process.
func collectTransport(source procSource, snapshots []m3ua.AssociationSnapshot, peerPort func(local, remote int) int) *transportEvidence {
	return &transportEvidence{
		ConfiguredNoDelay:       true,
		ConfiguredSACKDelay:     0,
		ConfiguredSACKFrequency: 1,
		Associations:            associationEvidenceFor(source, snapshots, peerPort, platformSocketOptions),
		Interfaces:              readInterfaces(interfaceRoot, ethtoolOffloads),
	}
}
