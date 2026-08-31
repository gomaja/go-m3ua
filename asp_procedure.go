package m3ua

import (
	"context"
	"errors"
	"fmt"

	"github.com/gomaja/go-m3ua/messages/params"
)

// ASPProcedureMode selects whether the M3UA layer initiates an ASP procedure
// automatically or only after an explicit Layer Management request.
type ASPProcedureMode uint8

const (
	// ASPProcedureAutomatic lets the Association initiate the procedure at its
	// corresponding startup or orderly-shutdown lifecycle point.
	ASPProcedureAutomatic ASPProcedureMode = iota + 1
	// ASPProcedureExplicit requires the application to call the corresponding
	// Association method.
	ASPProcedureExplicit
)

// ASPProcedurePolicy configures RFC 4666 Sections 4.3.4.1 through 4.3.4.4
// procedure initiation independently from SCTP association initiation.
// Explicit procedure calls are serialized per Association; a queued caller can
// stop waiting through its context without superseding the in-flight request.
type ASPProcedurePolicy struct {
	ASPUp       ASPProcedureMode
	ASPDown     ASPProcedureMode
	ASPActive   ASPProcedureMode
	ASPInactive ASPProcedureMode
}

type aspProcedure uint8

const (
	aspProcedureUp aspProcedure = iota + 1
	aspProcedureDown
	aspProcedureActive
	aspProcedureInactive
)

func validASPProcedureMode(mode ASPProcedureMode) bool {
	return mode == ASPProcedureAutomatic || mode == ASPProcedureExplicit
}

func validateASPProcedurePolicy(role Role, policy *ASPProcedurePolicy) error {
	if policy == nil {
		return nil
	}
	if role == RoleSGP {
		return fmt.Errorf("%w: ASP procedure initiation applies only to an ASP or IPSP",
			ErrInvalidRoleConfiguration)
	}
	if role != RoleASP && role != RoleIPSP {
		return ErrUnsupportedRole
	}
	if !validASPProcedureMode(policy.ASPUp) ||
		!validASPProcedureMode(policy.ASPDown) ||
		!validASPProcedureMode(policy.ASPActive) ||
		!validASPProcedureMode(policy.ASPInactive) {
		return fmt.Errorf("%w: ASP procedure policy must specify all four modes",
			ErrInvalidRoleConfiguration)
	}
	return nil
}

func (c *Association) aspProcedureMode(procedure aspProcedure) ASPProcedureMode {
	if c == nil || c.cfg == nil {
		return ASPProcedureExplicit
	}
	if policy := c.cfg.ASPProcedures; policy != nil {
		switch procedure {
		case aspProcedureUp:
			return policy.ASPUp
		case aspProcedureDown:
			return policy.ASPDown
		case aspProcedureActive:
			return policy.ASPActive
		case aspProcedureInactive:
			return policy.ASPInactive
		default:
			return ASPProcedureExplicit
		}
	}
	if c.role == RoleASP {
		return ASPProcedureAutomatic
	}
	if c.role == RoleIPSP && c.cfg.IPSP != nil {
		switch procedure {
		case aspProcedureUp:
			if c.cfg.IPSP.InitiateASPSM {
				return ASPProcedureAutomatic
			}
			return ASPProcedureExplicit
		case aspProcedureActive:
			if c.cfg.IPSP.InitiateASPTM {
				return ASPProcedureAutomatic
			}
			return ASPProcedureExplicit
		case aspProcedureDown, aspProcedureInactive:
			return ASPProcedureAutomatic
		}
	}
	return ASPProcedureExplicit
}

// ASPUp initiates the RFC 4666 Section 4.3.4.1 ASP Up procedure and waits for
// ASP Up Ack. It is valid for an ASP and for the locally initiated direction of
// an IPSP exchange.
func (c *Association) ASPUp(ctx context.Context) (err error) {
	defer func() { c.notifyASPProcedureFailure("ASP Up", nil, err) }()
	if err := c.validateExplicitASPProcedure(ctx); err != nil {
		return err
	}
	release, err := c.acquireExplicitASPProcedure(ctx)
	if err != nil {
		return err
	}
	defer release()
	if !c.localASPProcedureDirectionAvailable() {
		return ErrNoConfiguredAS
	}
	if c.localASPProcedureState() != StateASPDown {
		return ErrInvalidState
	}
	request, err := c.beginASPSM()
	if err != nil {
		return err
	}
	return c.waitTAck(ctx, request)
}

// ASPDown initiates the RFC 4666 Section 4.3.4.2 ASP Down procedure and waits
// for ASP Down Ack.
func (c *Association) ASPDown(ctx context.Context) (err error) {
	defer func() { c.notifyASPProcedureFailure("ASP Down", nil, err) }()
	if err := c.validateExplicitASPProcedure(ctx); err != nil {
		return err
	}
	release, err := c.acquireExplicitASPProcedure(ctx)
	if err != nil {
		return err
	}
	defer release()
	if !c.localASPProcedureDirectionAvailable() {
		return ErrNoConfiguredAS
	}
	switch c.localASPProcedureState() {
	case StateASPInactive, StateASPActive:
	default:
		return ErrInvalidState
	}
	request, err := c.initiateASPDown()
	if err != nil {
		return err
	}
	return c.waitTAck(ctx, request)
}

// ASPActive initiates the RFC 4666 Section 4.3.4.3 ASP Active procedure for
// the exact Application Servers and waits for every ASP Active Ack. With no
// keys it requests every configured local AS.
func (c *Association) ASPActive(ctx context.Context, keys ...ASKey) (err error) {
	defer func() { c.notifyASPProcedureFailure("ASP Active", keys, err) }()
	if err := c.validateExplicitASPProcedure(ctx); err != nil {
		return err
	}
	release, err := c.acquireExplicitASPProcedure(ctx)
	if err != nil {
		return err
	}
	defer release()
	if !c.localASPProcedureDirectionAvailable() {
		return ErrNoConfiguredAS
	}
	switch c.localASPProcedureState() {
	case StateASPInactive, StateASPActive:
	default:
		return ErrInvalidState
	}
	routingContext, err := c.routingContextParamForASPProcedure(keys)
	if err != nil {
		return err
	}
	requests, err := c.beginASPActive(routingContext)
	if err != nil {
		return err
	}
	return c.waitASPProcedureRequests(ctx, requests)
}

// ASPInactive initiates the RFC 4666 Section 4.3.4.4 ASP Inactive procedure
// for the exact Application Servers and waits for ASP Inactive Ack. With no
// keys it requests every configured local AS.
func (c *Association) ASPInactive(ctx context.Context, keys ...ASKey) (err error) {
	defer func() { c.notifyASPProcedureFailure("ASP Inactive", keys, err) }()
	if err := c.validateExplicitASPProcedure(ctx); err != nil {
		return err
	}
	release, err := c.acquireExplicitASPProcedure(ctx)
	if err != nil {
		return err
	}
	defer release()
	if !c.localASPProcedureDirectionAvailable() {
		return ErrNoConfiguredAS
	}
	if c.localASPProcedureState() != StateASPActive {
		return ErrInvalidState
	}
	routingContext, err := c.routingContextParamForASPProcedure(keys)
	if err != nil {
		return err
	}
	request, err := c.beginASPInactive(routingContext)
	if err != nil {
		return err
	}
	return c.waitTAck(ctx, request)
}

func (c *Association) validateExplicitASPProcedure(ctx context.Context) error {
	if c == nil {
		return ErrAssociationClosed
	}
	if c.role != RoleASP && c.role != RoleIPSP {
		return ErrUnsupportedRole
	}
	if ctx == nil {
		return errors.New("nil ASP procedure context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-c.done:
		if err := c.Err(); err != nil {
			return err
		}
		return ErrAssociationClosed
	default:
		return nil
	}
}

func (c *Association) acquireExplicitASPProcedure(ctx context.Context) (func(), error) {
	c.aspProcedureGateOnce.Do(func() {
		c.aspProcedureGate = make(chan struct{}, 1)
	})
	select {
	case c.aspProcedureGate <- struct{}{}:
		return func() { <-c.aspProcedureGate }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		if err := c.Err(); err != nil {
			return nil, err
		}
		return nil, ErrAssociationClosed
	}
}

func (c *Association) notifyASPProcedureFailure(
	operation string,
	keys []ASKey,
	cause error,
) {
	if c == nil || cause == nil {
		return
	}
	scope := uniqueASKeys(keys)
	if len(keys) == 0 {
		scope = c.configuredLocalASKeysForStatus()
	}
	c.notifyManagement(&ManagementIndication{
		Kind:        ManagementError,
		ASKeys:      scope,
		Cause:       cause,
		Description: fmt.Sprintf("%s failed: %v", operation, cause),
	})
}

func (c *Association) localASPProcedureDirectionAvailable() bool {
	if c == nil || c.role == RoleSGP {
		return false
	}
	if !c.isIPSPDoubleExchange() {
		return true
	}
	return c.hasLocalIPSPTrafficDirection() || c.usesSingleASPSMExchange()
}

func (c *Association) localASPProcedureState() State {
	if c != nil && c.isIPSPDoubleExchange() {
		return c.localIPSPStateValue()
	}
	if c == nil {
		return StateASPDown
	}
	return c.State()
}

func (c *Association) routingContextParamForASPProcedure(keys []ASKey) (*params.Param, error) {
	if len(keys) == 0 {
		return c.configuredLocalRoutingContextParam(), nil
	}
	configured := c.configuredLocalASKeysForStatus()
	if len(configured) == 0 {
		return nil, ErrNoConfiguredAS
	}
	unique := uniqueASKeys(keys)
	if len(unique) != len(keys) {
		return nil, fmt.Errorf("%w: duplicate Application Server", ErrInvalidParameterValue)
	}
	routingContexts := make([]uint32, 0, len(keys))
	contextless := false
	for _, key := range keys {
		if !containsASKey(configured, key) {
			for _, candidate := range configured {
				if candidate.RoutingContextSet == key.RoutingContextSet &&
					(!key.RoutingContextSet || candidate.RoutingContext == key.RoutingContext) {
					return nil, ErrInvalidNetworkAppearance
				}
			}
			if key.RoutingContextSet {
				return nil, NewInvalidRoutingContextError(key.RoutingContext)
			}
			return nil, ErrNoConfiguredAS
		}
		if !key.RoutingContextSet {
			contextless = true
			continue
		}
		routingContexts = append(routingContexts, key.RoutingContext)
	}
	if contextless {
		if len(keys) != 1 {
			return nil, fmt.Errorf("%w: contextless AS cannot share one ASPTM request",
				ErrInvalidParameterValue)
		}
		return nil, nil
	}
	return c.outboundRoutingContexts(routingContexts)
}

func (c *Association) waitASPProcedureRequests(ctx context.Context, requests []*pendingRequest) error {
	for index, request := range requests {
		if err := c.waitTAck(ctx, request); err != nil {
			for _, remaining := range requests[index+1:] {
				c.cancelTAckRequest(remaining)
			}
			return err
		}
	}
	return nil
}
