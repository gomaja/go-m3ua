// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"bytes"
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestMTPManagementDataOverSCTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := ASKey{RoutingContext: 1, RoutingContextSet: true}
	config := func() *AssociationConfig {
		c := NewAssociationConfig().SetApplicationServers(ASConfig{ASKey: key, TrafficMode: params.TrafficModeLoadshare})
		c.HeartbeatInfo = &HeartbeatInfo{Enabled: false}
		return c
	}
	sgp, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sgp.Close() }()
	asp, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = asp.Close() }()
	address := &sctp.Addr{IPs: []netip.Addr{netip.MustParseAddr("127.0.0.1")}, Port: 3299}
	listener, err := sgp.Listen("m3ua", address, NewListenerConfig(config()))
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan *Association, 1)
	acceptErr := make(chan error, 1)
	go func() {
		c, err := listener.Accept(ctx)
		if err != nil {
			acceptErr <- err
		} else {
			accepted <- c
		}
	}()
	client, err := asp.Dial(ctx, "m3ua", nil, address, config())
	if err != nil {
		t.Fatal(err)
	}
	var server *Association
	select {
	case server = <-accepted:
	case err := <-acceptErr:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for _, pair := range [][2]*Association{{client, server}, {server, client}} {
		sender, receiver := pair[0], pair[1]
		t.Run(receiver.Role().String(), func(t *testing.T) {
			local := receiver.ManagementIndications()
			remote := sender.ManagementIndications()
			_, err := sender.WriteData(DataRequest{AS: key, ProtocolData: params.ProtocolDataPayload{OriginatingPointCode: 200, DestinationPointCode: 100, ServiceIndicator: 0, Data: []byte("local-management-refused")}})
			requireManagementDataNotSent(t, err)
			if !errors.Is(err, ErrMTPManagementData) {
				t.Errorf("local SI 0 DATA error=%v", err)
			}
			// Inject a hostile peer frame directly through SCTP, bypassing the public
			// send admission and DATA encoder deliberately.
			_, raw := managementData(t, 0)
			if _, err := sender.sctpConn.SendMsg(raw, sctp.SendOptions{Info: &sctp.SndInfo{Stream: 1, PPID: M3UAPPID}}); err != nil {
				t.Fatal(err)
			}
			// A valid ordered marker proves the receive dispatcher has passed the
			// injection before the user queue is inspected.
			if _, err := sender.WriteData(DataRequest{AS: key, Stream: 1, ProtocolData: params.ProtocolDataPayload{ServiceIndicator: 3, Data: []byte("user-marker")}}); err != nil {
				t.Fatal(err)
			}
			msg, err := receiver.ReadData(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if msg.ProtocolData.ServiceIndicator != 3 || !bytes.Equal(msg.ProtocolData.Data, []byte("user-marker")) {
				t.Errorf("ordinary user received SI %d payload %q", msg.ProtocolData.ServiceIndicator, msg.ProtocolData.Data)
			}
			for _, ch := range []<-chan *ManagementIndication{local, remote} {
				seen := false
				for !seen {
					select {
					case ind := <-ch:
						if ind == nil {
							t.Fatal("indications closed")
						}
						if ind.Kind == ManagementError && ind.ErrorCode == params.ErrInvalidParameterValue {
							seen = true
						}
					case <-ctx.Done():
						t.Fatal("missing Invalid Parameter Value indication")
					}
				}
			}
			short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			defer cancel()
			if msg, err := receiver.ReadData(short); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("unexpected user DATA=%v err=%v", msg, err)
			}
		})
	}
}
