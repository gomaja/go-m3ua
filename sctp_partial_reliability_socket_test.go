// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/gomaja/go-sctp"
)

func partialReliabilityASPConfig(enabled bool) *AssociationConfig {
	cfg := mcASPConfig(mcASPPointCode)
	cfg.ASPProcedures = explicitASPProcedurePolicy()
	cfg.PartialReliability = enabled
	return cfg
}

func TestSCTPPartialReliabilityNegotiation(t *testing.T) {
	for _, direction := range []string{"dial", "listen"} {
		for _, offered := range []bool{false, true} {
			for _, peerOffers := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/local=%t/peer=%t", direction, offered, peerOffers), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = endpoint.Close() }()
					cfg := partialReliabilityASPConfig(offered)
					rawConfig := &sctp.Config{PartialReliability: new(peerOffers)}
					var a *Association
					var raw *sctp.Conn
					if direction == "dial" {
						ln, e := rawConfig.Listen("sctp4", mcAddr(0, "127.0.0.1"))
						if e != nil {
							skipIfSCTPUnsupported(t, e)
							t.Fatal(e)
						}
						defer func() { _ = ln.Close() }()
						a, err = endpoint.Dial(ctx, "m3ua", mcAddr(0, "127.0.0.1"), ln.Addr().(*sctp.Addr), cfg)
						if err != nil {
							t.Fatal(err)
						}
						raw, err = ln.AcceptSCTP()
					} else {
						ln, e := endpoint.Listen("m3ua", mcAddr(0, "127.0.0.1"), NewListenerConfig(cfg))
						if e != nil {
							skipIfSCTPUnsupported(t, e)
							t.Fatal(e)
						}
						// Changing the caller's config cannot change an offer fixed at Listen.
						cfg.PartialReliability = !offered
						raw, err = rawConfig.Dial(ctx, "sctp4", mcAddr(0, "127.0.0.1"), ln.Addr().(*sctp.Addr))
						if err != nil {
							t.Fatal(err)
						}
						defer func() { _ = raw.Close() }()
						a, err = ln.Accept(ctx)
					}
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = raw.Close() }()
					want := offered && peerOffers
					checkPartialReliabilityStatus(t, a, want)
					if got, e := raw.PRSupported(); e != nil || got != want {
						t.Fatalf("raw peer PRSupported = %t, %v; want %t", got, e, want)
					}
					if err := a.Close(); err != nil {
						t.Fatal(err)
					}
					if _, e := a.SCTPPRSupported(); !errors.Is(e, net.ErrClosed) {
						t.Fatalf("closed PRSupported error = %v; want net.ErrClosed", e)
					}
				})
			}
		}
	}
}

func checkPartialReliabilityStatus(t *testing.T, a *Association, want bool) {
	t.Helper()
	if got, err := a.SCTPPRSupported(); err != nil || got != want {
		t.Fatalf("SCTPPRSupported = %t, %v; want %t", got, err, want)
	}
	status, err := a.AssociationStatus()
	if err != nil {
		t.Fatal(err)
	}
	if status.PRSupported != want {
		t.Fatalf("AssociationStatus.PRSupported = %t; want %t", status.PRSupported, want)
	}
}

func TestSCTPPartialReliabilitySelectionKeepsListenerServing(t *testing.T) {
	for _, offered := range []bool{false, true} {
		t.Run(fmt.Sprint(offered), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = endpoint.Close() }()
			cfg := partialReliabilityASPConfig(offered)
			lc := NewListenerConfig(cfg)
			calls := 0
			lc.SelectAssociationConfig = func(AcceptInfo) (*AssociationConfig, error) {
				calls++
				if calls == 1 {
					return partialReliabilityASPConfig(!offered), nil
				}
				return partialReliabilityASPConfig(offered), nil
			}
			ln, err := endpoint.Listen("m3ua", mcAddr(0, "127.0.0.1"), lc)
			if err != nil {
				skipIfSCTPUnsupported(t, err)
				t.Fatal(err)
			}
			for _, mismatch := range []bool{true, false} {
				raw, e := (&sctp.Config{PartialReliability: new(true)}).Dial(ctx, "sctp4", mcAddr(0, "127.0.0.1"), ln.Addr().(*sctp.Addr))
				if e != nil {
					t.Fatal(e)
				}
				defer func() { _ = raw.Close() }()
				a, e := ln.Accept(ctx)
				if mismatch {
					if a != nil || !errors.Is(e, ErrInvalidSCTPConfig) {
						t.Fatalf("mismatched selection = %v, %v; want ErrInvalidSCTPConfig", a, e)
					}
					continue
				}
				if e != nil {
					t.Fatalf("matching selection after refusal: %v", e)
				}
				checkPartialReliabilityStatus(t, a, offered)
			}
		})
	}
}
