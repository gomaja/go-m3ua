package main

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

type releasingInboundFence struct {
	*m3ua.Association
	release func()
}

func (f releasingInboundFence) ASPActive(ctx context.Context, keys ...m3ua.ASKey) error {
	f.release()
	return f.Association.ASPActive(ctx, keys...)
}

func TestRouteReferenceBoundaryJoinsPendingProtocolError(t *testing.T) {
	for _, inside := range []bool{false, true} {
		name := "before window"
		if inside {
			name = "last in-window instant"
		}
		t.Run(name, func(t *testing.T) { exercisePendingProtocolErrorBoundary(t, inside) })
	}
}

func exercisePendingProtocolErrorBoundary(t *testing.T, inside bool) {
	t.Helper()
	base := routedLoopbackPortBase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	entered, released := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(released) }) }

	targetEndpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{
		Role:                m3ua.RoleIPSP,
		TransportCongestion: &m3ua.TransportCongestionConfig{Disabled: true},
		RoutingKeyManagement: &m3ua.RoutingKeyManagementConfig{
			AuthorizeRegistration: func(m3ua.RoutingKeyRegistrationRequest) m3ua.RegistrationStatus {
				enterOnce.Do(func() { close(entered) })
				select {
				case <-released:
				case <-ctx.Done():
				}
				return m3ua.RegistrationPermissionDenied
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = targetEndpoint.Close() })
	peerEndpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{
		Role:                m3ua.RoleIPSP,
		TransportCongestion: &m3ua.TransportCongestionConfig{Disabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peerEndpoint.Close() })

	key := m3ua.ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true,
		RoutingContext: 7, RoutingContextSet: true}
	config := func() *m3ua.AssociationConfig {
		c := m3ua.NewAssociationConfig()
		c.IPSP = &m3ua.IPSPConfig{ExchangeModel: m3ua.IPSPExchangeSingle}
		c.ASPProcedures = &m3ua.ASPProcedurePolicy{
			ASPUp: m3ua.ASPProcedureExplicit, ASPDown: m3ua.ASPProcedureExplicit,
			ASPActive: m3ua.ASPProcedureExplicit, ASPInactive: m3ua.ASPProcedureExplicit,
		}
		c.SetApplicationServers(m3ua.ASConfig{ASKey: key, TrafficMode: params.TrafficModeLoadshare})
		c.HeartbeatInfo = &m3ua.HeartbeatInfo{Enabled: false}
		c.TAck = 2 * time.Minute
		return c
	}
	address, err := sctp.ResolveAddr("sctp", fmt.Sprintf("127.0.0.1:%d", base))
	if err != nil {
		t.Fatal(err)
	}
	listener, err := targetEndpoint.Listen("m3ua", address, m3ua.NewListenerConfig(config()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	t.Cleanup(release)
	type acceptedResult struct {
		association *m3ua.Association
		err         error
	}
	accepted := make(chan acceptedResult, 1)
	go func() { a, e := listener.Accept(ctx); accepted <- acceptedResult{a, e} }()
	local, err := sctp.ResolveAddr("sctp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peer, err := peerEndpoint.Dial(ctx, "m3ua", local, address, config())
	if err != nil {
		t.Fatal(err)
	}
	var target *m3ua.Association
	select {
	case result := <-accepted:
		if result.err != nil {
			t.Fatal(result.err)
		}
		target = result.association
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := peer.ASPUp(ctx); err != nil {
		t.Fatal(err)
	}
	if err := peer.ASPActive(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := target.ASPActive(ctx, key); err != nil {
		t.Fatal(err)
	}
	if peer.State() != m3ua.StateASPActive || target.State() != m3ua.StateASPActive {
		t.Fatal("IPSP startup did not reach active state")
	}

	clock := newCohortTestClock()
	clock.observe([]*m3ua.Association{target})
	specification := routeReferenceSpecFixture(routeReferencesStatic)
	now, _ := clock.Now()
	specification.Clock.Start = now + 1
	specification.Clock.End = specification.Clock.Start + int64(specification.Duration)
	if inside {
		if err := clock.boundary(ctx, specification.Clock.Start); err != nil {
			t.Fatal(err)
		}
	}

	type registrationOutcome struct {
		results []m3ua.RoutingKeyRegistrationResult
		err     error
	}
	registrationDone := make(chan registrationOutcome, 1)
	go func() {
		results, e := peer.RegisterRoutingKeys(ctx, m3ua.RoutingKeyRegistration{
			RoutingKey: m3ua.RoutingKey{
				NetworkAppearance: 7, NetworkAppearanceSet: true,
				TrafficMode: params.TrafficModeLoadshare, TrafficModeSet: true,
				Groups: []m3ua.RoutingKeyGroup{{DestinationPointCode: 0x330001,
					ServiceIndicators: []uint8{3}}},
			},
		})
		registrationDone <- registrationOutcome{results, e}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if inside {
		clock.advance(specification.Clock.End + int64(specification.Drain))
	}
	if _, err := peer.WriteSignal(messages.NewError(
		params.NewErrorCode(params.ErrProtocolError), nil, nil, nil, nil,
	)); err != nil {
		t.Fatal(err)
	}

	clock.fences[0] = releasingInboundFence{Association: target, release: release}
	after := specification.Clock.End + int64(specification.Drain) + 1
	next := specification.Clock.Start
	if inside {
		next = after
	}
	boundaryErr := clock.boundary(ctx, next)
	release() // old implementation reaches here with target still blocked
	if boundaryErr != nil {
		t.Fatal(boundaryErr)
	}
	// Join even the old implementation's late dispatch, without observing the
	// indication channel. This is after sealing and cannot change the old At.
	if err := target.ASPActive(ctx, key); err != nil {
		t.Fatal(err)
	}
	clock.advance(after + 1)
	select {
	case result := <-registrationDone:
		if result.err != nil || len(result.results) != 1 ||
			result.results[0].Status != m3ua.RegistrationPermissionDenied {
			t.Fatalf("registration results %+v, error %v", result.results, result.err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if peer.State() != m3ua.StateASPActive || target.State() != m3ua.StateASPActive {
		t.Fatal("gate or fence changed active state")
	}
	clean := routeReferenceRecordFixture() // clean evaluator snapshots; this
	// isolated two-association witness does not claim eight-route state proof.
	run := &routeReferenceRun{spec: *specification.RouteReferences, observer: clock.observer}
	record := run.evaluate(specification, clean.Before, clean.After)
	wantEvents, wantVerdict, wantAt := 0, failoverPass, now
	if inside {
		wantEvents, wantVerdict, wantAt = 1, failoverFail, after-1
	}
	all := clock.observer.events
	if len(all) != 1 || all[0].Association != target.ID() || all[0].At != wantAt ||
		all[0].Kind != "management-indication" || all[0].Detail != "M-ERROR (kind 1): Protocol Error" ||
		len(record.Events) != wantEvents || record.Verdict != wantVerdict {
		t.Fatalf("pending Error misattributed: %+v; all observed events %+v", record, all)
	}
	for _, criterion := range record.Criteria {
		want := failoverPass
		if inside && criterion.Name == "no_library_indications" {
			want = failoverFail
		}
		if criterion.Outcome != want {
			t.Errorf("unexpected criterion %+v", criterion)
		}
	}
}

type observedStateHistoryFence struct {
	*m3ua.Association
	test  *testing.T
	calls int
}

func (f *observedStateHistoryFence) ASPActive(ctx context.Context, keys ...m3ua.ASKey) error {
	f.calls++
	f.test.Logf("state history fence entered: exchange=%d", f.calls)
	if err := f.Association.ASPActive(ctx, keys...); err != nil {
		return err
	}
	f.test.Logf("state history fence completed: exchange=%d state=%s queued=%d", f.calls, f.State(), len(f.StateChanges()))
	return nil
}

// stateHistoryAssociation consumes the startup ACTIVE indication before handing
// the channel to the clock. The scripted peer sends only requested Acks, so
// there is no trailing preparation publication that could flush the withdrawal
// before the regression's controlled monitor pause.
func stateHistoryAssociation(t *testing.T) (context.Context, *m3ua.Endpoint, *m3ua.Association, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	address, err := sctp.ResolveAddr("sctp", fmt.Sprintf("127.0.0.1:%d", routedLoopbackPortBase(t)))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	listener, err := (&sctp.Config{InitMsg: sctp.InitMsg{OutStreams: 16, MaxInStreams: 16}}).Listen("sctp", address)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	accepted := make(chan *sctp.Conn, 1)
	peerErrors := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		serve := func() error {
			connection, err := listener.AcceptSCTP()
			if err != nil {
				return err
			}
			accepted <- connection
			defer func() { _ = connection.Close() }()
			for {
				raw, info, err := connection.ReadMsg(65536)
				if err != nil {
					return err
				}
				if info.Stream != 0 || info.PPID != m3ua.M3UAPPID {
					return fmt.Errorf("control stream=%d PPID=%d", info.Stream, info.PPID)
				}
				message, err := messages.Parse(raw)
				if err != nil {
					return err
				}
				var reply messages.M3UA
				// RFC 4666 §§4.3.4.1, 4.3.4.2, 4.3.4.3 and 4.3.4.4:
				// acknowledge the exact requested procedure and ASPTM scope.
				switch request := message.(type) {
				case *messages.AspUp:
					reply = messages.NewAspUpAck(nil, nil)
				case *messages.AspActive:
					reply = messages.NewAspActiveAck(params.NewTrafficModeType(params.TrafficModeLoadshare), request.RoutingContext, nil)
				case *messages.AspInactive:
					reply = messages.NewAspInactiveAck(request.RoutingContext, nil)
				case *messages.AspDown:
					reply = messages.NewAspDownAck(nil)
				default:
					return fmt.Errorf("unexpected control %T", message)
				}
				data, err := reply.MarshalBinary()
				if err != nil {
					return err
				}
				// RFC 6458 §5.3.4; Errata 6111. With explicit EOR
				// disabled, each send is a complete record (§8.1.26).
				if _, err := connection.SendMsgContext(ctx, data, sctp.SendOptions{Info: &sctp.SndInfo{PPID: m3ua.M3UAPPID, Stream: 0}}); err != nil {
					return err
				}
			}
		}
		peerErrors <- serve()
	}()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		select {
		case connection := <-accepted:
			_ = connection.Close()
		case <-done:
		}
		<-done
	})
	endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleASP, TransportCongestion: &m3ua.TransportCongestionConfig{Disabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	config := m3ua.NewAssociationConfig()
	config.TAck = cohortTAck
	config.HeartbeatInfo = &m3ua.HeartbeatInfo{Enabled: false}
	config.SetApplicationServers(m3ua.ASConfig{ASKey: m3ua.ASKey{RoutingContext: 7, RoutingContextSet: true}, TrafficMode: params.TrafficModeLoadshare})
	config.Subscribe = func(association *m3ua.Association) { association.StateChanges() }
	local, err := sctp.ResolveAddr("sctp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	association, err := endpoint.Dial(ctx, "m3ua", local, address, config)
	if err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case state, open := <-association.StateChanges():
			if !open {
				t.Fatal("association ended during startup")
			}
			if state == m3ua.StateASPActive {
				return ctx, endpoint, association, peerErrors
			}
		case err := <-peerErrors:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

// A solicited Inactive/Ack supplies a real transient edge independently of
// management indications or the policy for unowed Up Acks. The boundary's
// Active procedure restores the endpoint before the final snapshot, so only
// the state-change history can retain the withdrawal. This isolated witness
// uses clean evaluator snapshots; the eight-association route and SSNM
// fixtures exercise the same fence separately.
func TestRouteReferenceBoundaryJoinsStateHistory(t *testing.T) {
	for _, inside := range []bool{false, true} {
		name := "before window"
		if inside {
			name = "last in-window instant"
		}
		t.Run(name, func(t *testing.T) {
			ctx, endpoint, association, peerErrors := stateHistoryAssociation(t)
			clock := newCohortTestClock()
			clock.observe([]*m3ua.Association{association})
			now, _ := clock.Now()
			specification := routeReferenceSpecFixture(routeReferencesStatic)
			domain, _ := clock.Domain()
			specification.Clock = &sharedClockWindow{Domain: domain, Start: now + 1, End: now + 1 + int64(specification.Duration)}
			if inside {
				// Startup's ACTIVE indication already joined the last incoming Ack's
				// flush. No protocol input occurs while positioning this clean window.
				clock.advance(specification.Clock.Start)
				clock.advance(specification.Clock.End + int64(specification.Drain))
			}
			before, closing := endpoint.ASPStatuses(), endpoint.ASPStatuses()
			if association.State() != m3ua.StateASPActive || len(before) != 1 || !before[0].LocalStateSet || before[0].LocalState != m3ua.StateASPActive || !reflect.DeepEqual(before, closing) {
				t.Fatalf("initial snapshots: %+v / %+v", before, closing)
			}
			clock.fences[0] = &observedStateHistoryFence{Association: association, test: t}
			t.Logf("state history withdrawal: association=%p id=%d local=%s remote=%s", association, association.ID(), association.LocalAddr(), association.RemoteAddr())
			if err := association.ASPInactive(ctx); err != nil {
				t.Fatal(err)
			}
			if association.State() != m3ua.StateASPInactive {
				t.Fatal("solicited withdrawal did not commit INACTIVE")
			}
			afterInstant := specification.Clock.End + int64(specification.Drain) + 1
			boundary := specification.Clock.Start
			if inside {
				boundary = afterInstant
			}
			if err := clock.boundary(ctx, boundary); err != nil {
				t.Fatal(err)
			}
			t.Logf("state history boundary sealed: instant=%d", boundary)
			if association.State() != m3ua.StateASPActive {
				t.Fatal("boundary did not restore ACTIVE")
			}
			// Join after sealing too: a missing boundary join must expose late
			// state events at the wrong instant rather than silently losing them.
			for range 3 {
				if err := clock.fence(ctx); err != nil {
					t.Fatal(err)
				}
			}
			clock.advance(afterInstant + 1)
			after := endpoint.ASPStatuses()
			if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(closing, after) {
				t.Fatalf("fence did not restore both ACTIVE snapshots: %+v / %+v / %+v", before, closing, after)
			}
			select {
			case err := <-peerErrors:
				t.Fatal(err)
			default:
			}
			events := clock.observer.events
			wantAt := now
			wantVerdict, wantActivation := failoverPass, failoverPass
			if inside {
				wantAt = afterInstant - 1
				wantVerdict, wantActivation = failoverFail, failoverFail
			}
			want := []string{m3ua.StateASPInactive.String(), m3ua.StateASPActive.String()}
			if len(events) != len(want) {
				t.Fatalf("state history = %+v", events)
			}
			for i, event := range events {
				if event.Association != association.ID() || event.Kind != "state-change" || event.Detail != want[i] || event.At != wantAt {
					t.Errorf("state event %d: %+v, want %s at %d", i, event, want[i], wantAt)
				}
			}
			clean := routeReferenceRecordFixture()
			run := &routeReferenceRun{spec: *specification.RouteReferences, observer: clock.observer}
			record := run.evaluate(specification, clean.Before, clean.After)
			if len(record.Criteria) != 3 || record.Verdict != wantVerdict {
				t.Errorf("state-history verdict %s, criteria %+v, events %+v", record.Verdict, record.Criteria, record.Events)
			}
			for _, criterion := range record.Criteria {
				expected := failoverPass
				if criterion.Name == "no_as_deactivation" {
					expected = wantActivation
				}
				if criterion.Outcome != expected {
					t.Errorf("state-history criterion %+v; events %+v", criterion, record.Events)
				}
			}
		})
	}
}
