// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-sctp"
)

func TestSCTPNoDelayConfiguration(t *testing.T) {
	for _, role := range []Role{RoleASP, RoleSGP, RoleIPSP} {
		for _, direction := range []string{"dial", "accept", "selected"} {
			for _, nagle := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/nagle=%t", role, direction, nagle), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					endpoint, err := NewEndpoint(EndpointConfig{Role: role})
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = endpoint.Close() }()
					cfg := NewAssociationConfig()
					if role != RoleSGP {
						cfg.ASPProcedures = explicitASPProcedurePolicy()
					}
					if role == RoleIPSP {
						cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
					}
					// Leave the default untouched to exercise zero-value configuration.
					if nagle {
						cfg.SetSCTPNagle(true)
					}
					type observation struct {
						association *Association
						noDelay     bool
						err         error
					}
					observed := make(chan observation, 2)
					// Subscribe is the first public callback with an Association. It runs
					// after socket setup and before the M3UA handshake starts.
					cfg.Subscribe = func(a *Association) {
						got, err := a.sctpConn.NoDelay()
						observed <- observation{a, got, err}
					}
					type result struct {
						association *Association
						err         error
					}
					finished := make(chan result, 1)
					var peer *sctp.Conn
					if direction == "dial" {
						ln, e := sctp.Listen("sctp4", mcAddr(0, "127.0.0.1"))
						if e != nil {
							skipIfSCTPUnsupported(t, e)
							t.Fatal(e)
						}
						defer func() { _ = ln.Close() }()
						deadline, _ := ctx.Deadline()
						if err := ln.SetDeadline(deadline); err != nil {
							t.Fatal(err)
						}
						go func() {
							a, err := endpoint.Dial(ctx, "m3ua4", nil, ln.Addr().(*sctp.Addr), cfg)
							finished <- result{a, err}
						}()
						peer, err = ln.AcceptSCTP()
					} else {
						lc := NewListenerConfig(cfg)
						if direction == "selected" {
							selected := snapshotAssociationConfig(cfg)
							cfg.SetSCTPNagle(!nagle)
							cfg.Subscribe = func(*Association) { t.Error("default Subscribe used instead of selected config") }
							lc = NewListenerConfig(cfg)
							lc.SelectAssociationConfig = func(AcceptInfo) (*AssociationConfig, error) { return selected, nil }
						}
						ln, e := endpoint.Listen("m3ua4", mcAddr(0, "127.0.0.1"), lc)
						if e != nil {
							skipIfSCTPUnsupported(t, e)
							t.Fatal(e)
						}
						peer, err = sctp.Dial(ctx, "sctp4", nil, ln.Addr().(*sctp.Addr))
						if err != nil {
							t.Fatal(err)
						}
						go func() {
							a, err := ln.Accept(ctx)
							finished <- result{a, err}
						}()
					}
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = peer.Close() }()
					var before observation
					select {
					case before = <-observed:
					case <-ctx.Done():
						t.Fatal("Subscribe was not called: ", ctx.Err())
					}
					if before.err != nil || before.noDelay != !nagle {
						t.Fatalf("Subscribe NoDelay() = %t, %v; want %t", before.noDelay, before.err, !nagle)
					}
					// An SGP returns only after its peer activates traffic. Send these
					// messages after observing Subscribe, so the option is checked first.
					if role == RoleSGP {
						for _, message := range []messages.M3UA{messages.NewAspUp(nil, nil), messages.NewAspActive(nil, nil, nil)} {
							data, err := message.MarshalBinary()
							if err != nil {
								t.Fatal(err)
							}
							n, err := peer.SendMsg(data, sctp.SendOptions{Info: &sctp.SndInfo{PPID: M3UAPPID}})
							if err != nil || n != len(data) {
								t.Fatalf("send handshake: %d/%d, %v", n, len(data), err)
							}
						}
					}
					var established result
					select {
					case established = <-finished:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					if established.err != nil {
						t.Fatal(established.err)
					}
					a := established.association
					defer func() { _ = a.Close() }()
					if before.association != a {
						t.Fatal("Subscribe observed another association")
					}
					select {
					case <-observed:
						t.Fatal("Subscribe called more than once")
					default:
					}
					if got, e := a.sctpConn.NoDelay(); e != nil || got != !nagle {
						t.Fatalf("NoDelay() = %t, %v; want %t", got, e, !nagle)
					}
					for _, noDelay := range []bool{nagle, !nagle} {
						if err := a.SetSCTPNoDelay(noDelay); err != nil {
							t.Fatal(err)
						}
						if got, e := a.sctpConn.NoDelay(); e != nil || got != noDelay {
							t.Fatalf("runtime NoDelay() = %t, %v; want %t", got, e, noDelay)
						}
					}
				})
			}
		}
	}
}
