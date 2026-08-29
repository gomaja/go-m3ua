// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"sync"
)

// Role identifies the M3UA protocol role of an Endpoint.
//
// RFC 4666 Section 1.4.8 deliberately separates this role from which peer
// initiates the SCTP association: both an ASP and an SGP can dial or accept.
type Role uint8

const (
	// RoleASP is an Application Server Process, as defined by RFC 4666 Section 1.2.
	RoleASP Role = iota + 1
	// RoleSGP is a Signalling Gateway Process, as defined by RFC 4666 Section 1.2.
	RoleSGP
	// RoleIPSP is an IP Server Process, as defined by RFC 4666 Section 1.2.
	RoleIPSP
)

func (r Role) String() string {
	switch r {
	case RoleASP:
		return "ASP"
	case RoleSGP:
		return "SGP"
	case RoleIPSP:
		return "IPSP"
	default:
		return "unknown"
	}
}

// Endpoint owns an immutable M3UA protocol role independently of SCTP
// association initiation.
type Endpoint struct {
	role Role

	mu           sync.Mutex
	closed       bool
	closeErr     error
	listeners    map[*Listener]struct{}
	associations map[*Association]struct{}
	done         chan struct{}
	ctx          context.Context
	cancel       context.CancelCauseFunc
	operations   sync.WaitGroup
	as           *applicationServers
	aspRoutes    *aspRoutes
	nif          *nifAvailability
	destinations *destinations
	mtp3Restarts *mtp3RestartRegistry
}

// NewEndpoint creates an M3UA endpoint with an immutable protocol role and
// role-specific node policy.
func NewEndpoint(config EndpointConfig) (*Endpoint, error) {
	switch config.Role {
	case RoleASP, RoleSGP, RoleIPSP:
		if config.Role != RoleASP && config.ASP != nil {
			return nil, ErrInvalidRoleConfiguration
		}
		if config.Role != RoleSGP && config.SGP != nil {
			return nil, ErrInvalidRoleConfiguration
		}
		var routes *aspRoutes
		if config.Role == RoleASP {
			var err error
			routes, err = newASPRoutes(config.ASP)
			if err != nil {
				return nil, err
			}
		}
		ctx, cancel := context.WithCancelCause(context.Background())
		endpoint := &Endpoint{
			role:         config.Role,
			listeners:    make(map[*Listener]struct{}),
			associations: make(map[*Association]struct{}),
			done:         make(chan struct{}),
			ctx:          ctx,
			cancel:       cancel,
			mtp3Restarts: &mtp3RestartRegistry{},
			aspRoutes:    routes,
		}
		switch config.Role {
		case RoleSGP:
			endpoint.as = newApplicationServersForSGP(snapshotSGPConfig(config.SGP))
			endpoint.nif = &nifAvailability{}
			endpoint.destinations = newDestinations()
		case RoleIPSP:
			// RFC 4666 Sections 4.3.1 and 4.3.4.3 require the peer M3UA
			// layer to maintain each remote IPSP's per-AS state, including
			// Override across all IPSPs serving the same AS. The registry is
			// therefore Endpoint state, not Association state.
			endpoint.as = newApplicationServers(DefaultRecoveryTimer)
		}
		return endpoint, nil
	default:
		return nil, ErrUnsupportedRole
	}
}

func (e *Endpoint) beginOperation() bool {
	if e == nil {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return false
	}
	e.operations.Add(1)
	return true
}

func (e *Endpoint) endOperation() {
	if e != nil {
		e.operations.Done()
	}
}

func (e *Endpoint) operationContext(ctx context.Context) (context.Context, context.CancelCauseFunc, func()) {
	operationCtx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(e.ctx, func() {
		cancel(ErrEndpointClosed)
	})
	return operationCtx, cancel, func() { stop() }
}

// Done is closed after Endpoint.Close has closed every Listener and
// Association owned by the endpoint.
func (e *Endpoint) Done() <-chan struct{} {
	if e == nil {
		return nil
	}
	return e.done
}

func (e *Endpoint) trackListener(listener *Listener) bool {
	if e == nil || listener == nil {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return false
	}
	e.listeners[listener] = struct{}{}
	return true
}

func (e *Endpoint) forgetListener(listener *Listener) {
	if e == nil || listener == nil {
		return
	}
	e.mu.Lock()
	delete(e.listeners, listener)
	e.mu.Unlock()
}

func (e *Endpoint) trackAssociation(association *Association) bool {
	if e == nil || association == nil {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return false
	}
	if e.aspRoutes != nil && !e.aspRoutes.attach(association) {
		return false
	}
	association.endpoint = e
	e.associations[association] = struct{}{}
	return true
}

func (e *Endpoint) forgetAssociation(association *Association) {
	if e == nil || association == nil {
		return
	}
	e.mu.Lock()
	delete(e.associations, association)
	applicationServers := e.as
	aspRoutes := e.aspRoutes
	e.mu.Unlock()
	if aspRoutes != nil {
		aspRoutes.detach(association)
	}
	if applicationServers != nil {
		applicationServers.forget(association)
	}
}

func (e *Endpoint) setNIFAvailable(available bool) error {
	associations, err := e.setAvailability(func(nif *nifAvailability) {
		nif.setIsolated(!available)
	})
	if err != nil || available {
		return err
	}
	runAssociationIsolation(associations, isolateNIFConnection)
	return nil
}

func (e *Endpoint) setASAvailable(rtCtx uint32, available bool) error {
	if e == nil || e.role != RoleSGP {
		return ErrUnsupportedRole
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrEndpointClosed
	}
	registryKey, _, registryOK, registryAmbiguous := e.as.lookupRoutingContext(rtCtx)
	if registryAmbiguous {
		e.mu.Unlock()
		return nil
	}
	configuredKey, configuredOK, configuredAmbiguous := e.singleConfiguredASKeyForRoutingContextLocked(rtCtx)
	if configuredAmbiguous || registryOK && configuredOK && registryKey != configuredKey {
		e.mu.Unlock()
		return nil
	}
	key := e.as.routingContextASKey(rtCtx)
	if registryOK {
		key = registryKey
	} else if configuredOK {
		key = configuredKey
	}
	e.mu.Unlock()
	return e.setASAvailableForAS(key, available)
}

func (e *Endpoint) setASAvailableForAS(key ASKey, available bool) error {
	associations, err := e.setAvailability(func(nif *nifAvailability) {
		nif.setASAvailableForAS(key, available)
	})
	if err != nil || available {
		return err
	}
	runAssociationIsolation(associations, func(association *Association) {
		for _, configuredKey := range association.configuredASKeys() {
			if configuredKey == key {
				isolateApplicationServerConnection(association, e.as, key)
				return
			}
		}
	})
	return nil
}

func (e *Endpoint) setAvailability(update func(*nifAvailability)) ([]*Association, error) {
	if e == nil || e.role != RoleSGP {
		return nil, ErrUnsupportedRole
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, ErrEndpointClosed
	}
	update(e.nif)
	associations := make([]*Association, 0, len(e.associations))
	for association := range e.associations {
		associations = append(associations, association)
	}
	return associations, nil
}

func (e *Endpoint) singleConfiguredASKeyForRoutingContextLocked(rtCtx uint32) (ASKey, bool, bool) {
	var found ASKey
	foundSet := false
	for association := range e.associations {
		for _, key := range association.configuredASKeys() {
			if !key.RoutingContextSet || key.RoutingContext != rtCtx {
				continue
			}
			if foundSet && key != found {
				return ASKey{}, false, true
			}
			found = key
			foundSet = true
		}
	}
	return found, foundSet, false
}

func runAssociationIsolation(associations []*Association, isolate func(*Association)) {
	var isolated sync.WaitGroup
	isolated.Add(len(associations))
	for _, association := range associations {
		go func() {
			defer isolated.Done()
			isolate(association)
		}()
	}
	isolated.Wait()
}

// Close closes every Listener and Association owned by the Endpoint.
func (e *Endpoint) Close() error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	if e.closed {
		done := e.done
		e.mu.Unlock()
		<-done
		e.mu.Lock()
		err := e.closeErr
		e.mu.Unlock()
		return err
	}
	e.closed = true
	e.cancel(ErrEndpointClosed)
	listeners := make([]*Listener, 0, len(e.listeners))
	for listener := range e.listeners {
		listeners = append(listeners, listener)
	}
	associations := make([]*Association, 0, len(e.associations))
	for association := range e.associations {
		associations = append(associations, association)
	}
	applicationServers := e.as
	e.mu.Unlock()

	// Quiesce node-wide AS state before closing individual transports. Otherwise
	// each Association departure can drive AS transitions and Notify messages to
	// siblings that are themselves being shut down.
	if applicationServers != nil {
		applicationServers.close()
	}
	invalidateMTP3RestartRegistry(e.mtp3Restarts)

	var firstErr error
	for _, listener := range listeners {
		if err := listener.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, association := range associations {
		if err := association.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	e.operations.Wait()
	if e.aspRoutes != nil {
		e.aspRoutes.closeIndications()
	}
	e.mu.Lock()
	e.closeErr = firstErr
	close(e.done)
	e.mu.Unlock()
	return firstErr
}

// Role returns the endpoint's immutable M3UA protocol role.
func (e *Endpoint) Role() Role {
	if e == nil {
		return 0
	}
	return e.role
}

func (e *Endpoint) sgpRegistry() (*applicationServers, *nifAvailability, *destinations, *mtp3RestartRegistry) {
	if e == nil || e.role != RoleSGP {
		return nil, nil, nil, nil
	}
	return e.as, e.nif, e.destinations, e.mtp3Restarts
}

func (e *Endpoint) applicationServerRegistry() *applicationServers {
	if e == nil || (e.role != RoleSGP && e.role != RoleIPSP) {
		return nil
	}
	return e.as
}

func (e *Endpoint) associationRole() (Role, error) {
	if e == nil {
		return 0, ErrUnsupportedRole
	}
	switch e.role {
	case RoleASP:
		return RoleASP, nil
	case RoleSGP:
		return RoleSGP, nil
	case RoleIPSP:
		return RoleIPSP, nil
	default:
		return 0, ErrUnsupportedRole
	}
}

func (e *Endpoint) validateAssociationConfig(config *AssociationConfig) error {
	if e == nil {
		return ErrUnsupportedRole
	}
	if e.role == RoleASP && e.aspRoutes != nil {
		return e.aspRoutes.validateAssociationConfig(config)
	}
	return nil
}
