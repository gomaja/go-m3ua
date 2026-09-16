package main

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

const (
	// associationAcceptTimeout bounds the ASP listener's wait for the SGP's
	// SCTP connections, mirroring the 30-second receiver readiness deadline.
	associationAcceptTimeout = 30 * time.Second

	// dialRetryWindow bounds how long the SGP-dial side tolerates a refused
	// SCTP dial while the ASP listener is still coming up. The library's own
	// dial then bounds the M3UA establishment with AssociationConfig's
	// EstablishTimeout (DefaultEstablishTimeout).
	dialRetryWindow   = 30 * time.Second
	dialRetryInterval = 100 * time.Millisecond
)

var (
	// errPeerNotAccepting names the bounded wait expiring while every SCTP
	// dial is refused; it is not a generic fatal network error.
	errPeerNotAccepting = errors.New("peer did not accept SCTP dials within the retry window")

	// errAssociationNotActive names an association that completed SCTP and
	// M3UA establishment without reaching AS-ACTIVE, so traffic on it would
	// fail with ErrNotEstablished. The library's Dial already waits for the
	// policy-selected readiness state (AS-ACTIVE for the SGP role); this
	// check makes the fixture's readiness contract explicit rather than
	// assumed.
	errAssociationNotActive = errors.New("association did not reach AS-ACTIVE before serving traffic")

	// errAcceptTimeout names the bounded accept wait expiring before the peer
	// connected.
	errAcceptTimeout = errors.New("peer did not connect within the accept window")
)

// associationAcceptor is the accept side of an M3UA listener, abstracted so
// the acquisition rules are testable without SCTP sockets.
type associationAcceptor interface {
	Accept(ctx context.Context) (*m3ua.Association, error)
}

type associationAcceptorCloser interface {
	associationAcceptor
	Close() error
}

// endpointConnector abstracts Endpoint Listen/Dial behind exactly the
// signatures the fixture needs.
type endpointConnector interface {
	Listen(network string, laddr *sctp.SCTPAddr, cfg *m3ua.ListenerConfig) (associationAcceptorCloser, error)
	Dial(ctx context.Context, network string, laddr, raddr *sctp.SCTPAddr, cfg *m3ua.AssociationConfig) (*m3ua.Association, error)
}

type m3uaConnector struct {
	endpoint *m3ua.Endpoint
}

func (connector m3uaConnector) Listen(network string, laddr *sctp.SCTPAddr, cfg *m3ua.ListenerConfig) (associationAcceptorCloser, error) {
	return connector.endpoint.Listen(network, laddr, cfg)
}

func (connector m3uaConnector) Dial(ctx context.Context, network string, laddr, raddr *sctp.SCTPAddr, cfg *m3ua.AssociationConfig) (*m3ua.Association, error) {
	return connector.endpoint.Dial(ctx, network, laddr, raddr, cfg)
}

// establishSenderAssociations acquires every sender-side association for the
// configured SCTP initiation direction. The returned release function closes
// the listener and must run only when the run ends: m3ua.Listener.Close
// closes every association it accepted, so releasing early tears down the
// very associations the cohorts send on. The dial path's release is a no-op.
func establishSenderAssociations(ctx context.Context, config commandConfig, connector endpointConnector) ([]*m3ua.Association, func(), error) {
	if config.Transport != "listen" {
		associations, err := dialSenderAssociations(ctx, config, connector)
		return associations, func() {}, err
	}
	listenAddress, err := sctp.ResolveSCTPAddr("sctp", config.SCTPAddress)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve ASP listen address: %w", err)
	}
	listener, err := connector.Listen("m3ua", listenAddress, m3ua.NewListenerConfig(associationConfig("asp")))
	if err != nil {
		return nil, nil, fmt.Errorf("listen for M3UA associations: %w", err)
	}
	associations, err := acceptAssociations(ctx, listener, config.Associations, associationAcceptTimeout)
	if err != nil {
		_ = listener.Close()
		return nil, nil, err
	}
	release := func() { _ = listener.Close() }
	return associations, release, nil
}

// acceptAssociations accepts count associations with a bounded total wait.
// The library's Accept returns only after each association reaches its
// policy-selected readiness state (AS-ACTIVE for the ASP role), so a peer
// that never activates surfaces as a bounded accept error, not traffic on an
// unestablished association.
//
// The caller's ctx is passed to Accept untouched: the library runs every
// accepted association's monitor on the Accept context for the association's
// whole lifetime, so deriving and later cancelling a shorter context — or
// letting a timeout context's timer fire — tears down every accepted
// association. The wait is bounded externally instead; see acceptOne.
func acceptAssociations(ctx context.Context, listener associationAcceptorCloser, count int, window time.Duration) ([]*m3ua.Association, error) {
	deadline := time.Now().Add(window)
	associations := make([]*m3ua.Association, 0, count)
	for index := 0; index < count; index++ {
		association, err := acceptOne(ctx, listener, deadline, index)
		if err != nil {
			return nil, err
		}
		associations = append(associations, association)
	}
	return associations, nil
}

type acceptResult struct {
	association *m3ua.Association
	err         error
}

// acceptOne accepts one association with an externally bounded wait. On
// timeout or caller cancellation the listener is closed, which is the
// documented way to interrupt a blocked Accept; a successful Accept keeps
// the caller's context alive for the association's whole lifetime.
func acceptOne(ctx context.Context, listener associationAcceptorCloser, deadline time.Time, index int) (*m3ua.Association, error) {
	result := make(chan acceptResult, 1)
	go func() {
		association, err := listener.Accept(ctx)
		result <- acceptResult{association, err}
	}()
	timer := time.NewTimer(max(time.Until(deadline), 0))
	defer timer.Stop()
	select {
	case accepted := <-result:
		if accepted.err != nil {
			return nil, fmt.Errorf("accept association %d: %w", index, accepted.err)
		}
		return accepted.association, nil
	case <-timer.C:
		_ = listener.Close()
		return nil, fmt.Errorf("accept association %d: %w", index, errAcceptTimeout)
	case <-ctx.Done():
		_ = listener.Close()
		return nil, ctx.Err()
	}
}

// dialSenderAssociations is the ASP-dial acquisition path. Its behavior is
// unchanged from the working direction: one bounded Dial per association, no
// retry, the caller reports the first failure.
func dialSenderAssociations(ctx context.Context, config commandConfig, connector endpointConnector) ([]*m3ua.Association, error) {
	remoteAddress, err := sctp.ResolveSCTPAddr("sctp", config.SCTPAddress)
	if err != nil {
		return nil, fmt.Errorf("resolve SGP address: %w", err)
	}
	var localAddress *sctp.SCTPAddr
	if config.LocalAddress != "" {
		localAddress, err = sctp.ResolveSCTPAddr("sctp", config.LocalAddress)
		if err != nil {
			return nil, fmt.Errorf("resolve ASP local address: %w", err)
		}
	}
	associations := make([]*m3ua.Association, 0, config.Associations)
	for index := 0; index < config.Associations; index++ {
		association, err := connector.Dial(ctx, "m3ua", localAddress, remoteAddress, associationConfig("asp"))
		if err != nil {
			return nil, fmt.Errorf("dial association %d: %w", index, err)
		}
		associations = append(associations, association)
	}
	return associations, nil
}

// requireStateActive enforces the fixture readiness contract: no association
// serves traffic or counts toward readiness before it reaches AS-ACTIVE.
func requireStateActive(state m3ua.State) error {
	if state != m3ua.StateASPActive {
		return fmt.Errorf("%w: association state is %s", errAssociationNotActive, state)
	}
	return nil
}

// isTransientDialError reports whether a dial failure is worth retrying while
// the peer listener comes up. Only a refused connection qualifies; every
// other failure is returned immediately.
func isTransientDialError(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED)
}

// dialWithRetry repeats one SCTP dial while it is refused, bounded by window,
// so an SGP that starts before the ASP listener waits instead of failing the
// run at startup. The window expiry is the named errPeerNotAccepting failure,
// never a generic fatal.
func dialWithRetry(ctx context.Context, window, interval time.Duration, dial func() (*m3ua.Association, error)) (*m3ua.Association, error) {
	deadline := time.Now().Add(window)
	for {
		association, err := dial()
		if err == nil {
			return association, nil
		}
		if !isTransientDialError(err) {
			return nil, err
		}
		if !time.Now().Before(deadline) {
			return nil, fmt.Errorf("%w: %v", errPeerNotAccepting, err)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
