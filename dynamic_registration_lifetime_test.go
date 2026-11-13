package m3ua

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func newRegistrationLifetimeEndpoint(t *testing.T, removeUnused bool, provisioned ...ProvisionedRoutingKey) *Endpoint {
	t.Helper()
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP, RoutingKeyManagement: &RoutingKeyManagementConfig{
		AuthorizeRegistration:   func(RoutingKeyRegistrationRequest) RegistrationStatus { return RegistrationSuccessfullyRegistered },
		AllowDynamicRoutingKeys: true, RemoveUnusedRoutingKeys: removeUnused, ProvisionedRoutingKeys: provisioned,
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	return endpoint
}

func newRegistrationLifetimeASP(t *testing.T, endpoint *Endpoint, servers ...ASConfig) (*Association, *[]messages.M3UA) {
	t.Helper()
	association, sent := newTestConn(t, StateASPDown, RoleSGP)
	// A mode-only AS is explicit configuration under RFC 4666 §3.6.1.
	// Dynamic-only ASPs need an empty inventory before their real ASP Up.
	association.cfg = NewAssociationConfig().SetApplicationServers(servers...)
	association.recvStream.Store(0)
	association.endpoint, association.as = endpoint, endpoint.as
	if err := association.handleAspUp(messages.NewAspUp(nil, nil)); err != nil {
		t.Fatal(err)
	}
	if association.State() != StateASPInactive {
		t.Fatal("ASP Up did not make the registration fixture inactive")
	}
	return association, sent
}

func registerLifetimeKey(t *testing.T, association *Association, sent *[]messages.M3UA, identifier uint32, key RoutingKey, expected ...uint32) uint32 {
	t.Helper()
	parameter, err := routingKeyParameter(RoutingKeyRegistrationRequest{LocalRoutingKeyIdentifier: identifier, RoutingKey: key})
	if err != nil {
		t.Fatal(err)
	}
	want := uint32(params.SuccessfullyRegistered)
	if len(expected) > 0 {
		want = expected[0]
	}
	before := len(*sent)
	if err := association.handleRegistrationRequest(messages.NewRegistrationRequest(parameter)); err != nil {
		t.Fatal(err)
	}
	for _, message := range (*sent)[before:] {
		if response, ok := message.(*messages.RegistrationResponse); ok {
			result, err := response.RegistrationResults[0].RegistrationResult()
			if err != nil {
				t.Fatal(err)
			}
			if status := result.RegistrationStatus.RegistrationStatus(); status != want {
				t.Fatalf("REG RSP status = %d, want %d", status, want)
			}
			return result.RoutingContext.RoutingContext()
		}
	}
	t.Fatal("no REG RSP")
	return 0
}

func assertLifetimeRetired(t *testing.T, association *Association, rc uint32) {
	t.Helper()
	if key, ok := association.dynamicASKey(rc, false); ok {
		t.Errorf("retained dynamic association scope %+v", key)
	}
	registry := association.routingKeyRegistry()
	registry.mu.Lock()
	member := registry.entries[rc] != nil && registry.entries[rc].hasMember(association)
	registry.mu.Unlock()
	if member {
		t.Error("retained dynamic registry membership")
	}
	for _, as := range lifetimeApplicationServers(association.as) {
		// ASP Up can also create an implicit contextless AS. Only the AS
		// identified by the retired registration's RC is dynamic membership.
		if !as.key.RoutingContextSet || as.key.RoutingContext != rc {
			continue
		}
		as.mu.Lock()
		_, member := as.asps[association]
		as.mu.Unlock()
		if member {
			t.Errorf("retained dynamic AS membership: %+v", as.key)
		}
	}
}

func TestSGPDeregistersDynamicMembershipOnASPDown(t *testing.T) {
	testSGPRegistrationWithdrawal(t, false)
}

func TestSGPDeregistersDynamicMembershipOnASPUpWhileActive(t *testing.T) {
	testSGPRegistrationWithdrawal(t, true)
}

func testSGPRegistrationWithdrawal(t *testing.T, activeUp bool) {
	t.Helper()
	for _, removeUnused := range []bool{false, true} {
		name := "retain-key"
		if removeUnused {
			name = "remove-key"
		}
		t.Run(name, func(t *testing.T) {
			endpoint := newRegistrationLifetimeEndpoint(t, removeUnused)
			association, sent := newRegistrationLifetimeASP(t, endpoint)
			rc := registerLifetimeKey(t, association, sent, 77, testRoutingKey(10, 100, 3))
			key, _ := association.dynamicASKey(rc, false)
			as, _ := endpoint.as.lookup(key)
			association.commitState(StateASPActive)
			association.noteRoutingContextsActive([]uint32{rc})
			as.setASPState(association, StateASPActive, time.Hour)
			// Mandatory ASPSM withdrawal cannot be vetoed by explicit DEREG policy.
			endpoint.routingKeys.config.AuthorizeDeregistration = func(RoutingKeyDeregistrationRequest) bool {
				t.Error("implicit withdrawal called permission policy")
				return false
			}
			// Explicit DEREG still refuses an active ASP; ASPSM withdrawal is mandatory.
			if results := endpoint.routingKeys.deregister(association, []uint32{rc}); results[0].Status != DeregistrationASPActiveForRoutingContext {
				t.Fatalf("active DEREG = %+v", results)
			}
			writer := association.signalWriter
			acked := false
			association.signalWriter = func(message messages.M3UA) (int, error) {
				switch message.(type) {
				case *messages.AspDownAck, *messages.AspUpAck:
					acked = true
					assertLifetimeRetired(t, association, rc)
				}
				return writer(message)
			}
			if activeUp {
				if err := association.handleAspUp(messages.NewAspUp(nil, nil)); !isLifetimeUnexpectedMessage(err) {
					t.Fatalf("active Up = %v", err)
				}
				if association.State() != StateASPInactive {
					t.Fatal("active Up did not make ASP inactive")
				}
			} else {
				if err := association.handleAspDown(messages.NewAspDown(nil)); err != nil {
					t.Fatal(err)
				}
				if association.State() != StateASPDown {
					t.Fatal("Down did not make ASP down")
				}
			}
			if !acked {
				t.Fatal("no ASPSM acknowledgement")
			}
			assertLifetimeRetired(t, association, rc)
			assertLifetimeDeregistrationCompleted(t, association, rc)
			if _, exists := endpoint.routingKeys.routingKey(rc); exists == removeUnused {
				t.Errorf("retained key = %v, RemoveUnused = %v", exists, removeUnused)
			}
			association.signalWriter = writer
			if !activeUp {
				if err := association.handleAspUp(messages.NewAspUp(nil, nil)); err != nil {
					t.Fatal(err)
				}
			}
			// RFC 4666 §4.3.4.3 rejects a retired RC just as an unknown RC.
			if err := association.handleAspActive(messages.NewAspActive(nil, params.NewRoutingContext(rc), nil)); !errors.Is(err, ErrNoConfiguredAS) {
				t.Fatalf("activation of retired RC = %v", err)
			}
			// Reusing the Local RK Identifier must not resurrect the old replay.
			newRC := registerLifetimeKey(t, association, sent, 77, testRoutingKey(10, 100, 3))
			assertLifetimeRegistered(t, association, newRC)
			beforeActivation := len(*sent)
			if err := association.handleAspActive(messages.NewAspActive(nil, params.NewRoutingContext(newRC), nil)); err != nil {
				t.Errorf("fresh registration cannot activate: %v", err)
			}
			activated := false
			for _, message := range (*sent)[beforeActivation:] {
				if ack, ok := message.(*messages.AspActiveAck); ok && ack.RoutingContext != nil && ack.RoutingContext.RoutingContext() == newRC {
					activated = true
				}
			}
			if !activated {
				t.Error("fresh registration received no ASP Active Ack")
			}
			if association.State() != StateASPActive {
				t.Error("fresh registration did not reach ASP-ACTIVE")
			}
			freshScope := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: newRC, RoutingContextSet: true}
			if freshAS, ok := association.as.lookup(freshScope); !ok || freshAS.State() != ASActive {
				t.Error("fresh registration did not activate its AS")
			}

		})
	}
}

func TestSGPWithdrawalKeepsOtherASPMembership(t *testing.T) {
	for _, activeUp := range []bool{false, true} {
		t.Run(map[bool]string{false: "down", true: "active-up"}[activeUp], func(t *testing.T) {
			endpoint := newRegistrationLifetimeEndpoint(t, true)
			first, sent := newRegistrationLifetimeASP(t, endpoint)
			second, otherSent := newRegistrationLifetimeASP(t, endpoint)
			rc := registerLifetimeKey(t, first, sent, 1, testRoutingKey(10, 100, 3))
			if otherRC := registerLifetimeKey(t, second, otherSent, 1, testRoutingKey(10, 100, 3)); otherRC != rc {
				t.Fatal("AS not shared")
			}
			key, _ := second.dynamicASKey(rc, false)
			as, _ := endpoint.as.lookup(key)
			for _, asp := range []*Association{first, second} {
				asp.commitState(StateASPActive)
				asp.noteRoutingContextsActive([]uint32{rc})
				as.setASPState(asp, StateASPActive, time.Hour)
			}
			if activeUp {
				if err := first.handleAspUp(messages.NewAspUp(nil, nil)); !isLifetimeUnexpectedMessage(err) {
					t.Fatal(err)
				}
			} else if err := first.handleAspDown(messages.NewAspDown(nil)); err != nil {
				t.Fatal(err)
			}
			endpoint.routingKeys.mu.Lock()
			entry := endpoint.routingKeys.entries[rc]
			firstMember, secondMember := entry != nil && entry.hasMember(first), entry != nil && entry.hasMember(second)
			endpoint.routingKeys.mu.Unlock()
			if firstMember || !secondMember {
				t.Fatalf("registry first=%v second=%v", firstMember, secondMember)
			}
			if _, ok := second.dynamicASKey(rc, false); !ok {
				t.Error("other ASP lost dynamic scope")
			}
			as.mu.Lock()
			_, firstMember = as.asps[first]
			state, secondMember := as.asps[second]
			as.mu.Unlock()
			if firstMember || !secondMember || state != StateASPActive || as.State() != ASActive {
				t.Fatalf("AS first=%v second=%v state=%v AS=%v", firstMember, secondMember, state, as.State())
			}
		})
	}
}

func TestSGPWithdrawalKeepsStaticMembership(t *testing.T) {
	for _, activeUp := range []bool{false, true} {
		t.Run(map[bool]string{false: "down", true: "active-up"}[activeUp], func(t *testing.T) {
			routingKey := testRoutingKey(10, 100, 3)
			endpoint := newRegistrationLifetimeEndpoint(t, true, ProvisionedRoutingKey{RoutingContext: 7, RoutingKey: routingKey})
			association, sent := newRegistrationLifetimeASP(t, endpoint, ASConfig{
				ASKey:       ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 7, RoutingContextSet: true},
				TrafficMode: params.TrafficModeLoadshare,
			})
			staticKey := association.staticallyConfiguredASKeys()[0]
			staticAS := endpoint.as.get(staticKey)
			staticAS.setASPState(association, StateASPActive, time.Hour)
			association.commitState(StateASPActive)
			// Dynamic registration into a provisioned key must not remove static membership.
			registerLifetimeKey(t, association, sent, 1, routingKey, uint32(RegistrationRoutingKeyAlreadyRegistered))
			if activeUp {
				if err := association.handleAspUp(messages.NewAspUp(nil, nil)); !isLifetimeUnexpectedMessage(err) {
					t.Fatal(err)
				}
			} else if err := association.handleAspDown(messages.NewAspDown(nil)); err != nil {
				t.Fatal(err)
			}
			if !association.hasStaticApplicationServerMembership(staticKey) {
				t.Fatal("static AS membership retired")
			}
			if len(association.staticallyConfiguredASKeys()) != 1 {
				t.Fatal("static scope changed")
			}
			endpoint.routingKeys.mu.Lock()
			entry := endpoint.routingKeys.entries[7]
			retained := entry != nil && entry.provisioned
			member := entry != nil && entry.hasMember(association)
			endpoint.routingKeys.mu.Unlock()
			if !retained || member {
				t.Fatalf("provisioned key retained=%v dynamic member=%v", retained, member)
			}
			if _, ok := association.dynamicASKey(7, false); ok {
				t.Error("retained dynamic scope over static membership")
			}
		})
	}
}

func lifetimeApplicationServers(registry *applicationServers) []*applicationServer {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	servers := make([]*applicationServer, 0, len(registry.as))
	for _, as := range registry.as {
		servers = append(servers, as)
	}
	return servers
}

func isLifetimeUnexpectedMessage(err error) bool {
	var unexpected *UnexpectedMessageError
	return errors.As(err, &unexpected)
}

func TestSGPWithdrawalRetiresRegistrationWithFailedResponse(t *testing.T) {
	for _, procedure := range []string{"registration", "deregistration"} {
		t.Run(procedure, func(t *testing.T) {
			endpoint := newRegistrationLifetimeEndpoint(t, true)
			association, sent := newRegistrationLifetimeASP(t, endpoint)
			writeErr := errors.New("response write failed")
			writer := association.signalWriter
			if procedure == "registration" {
				association.signalWriter = func(messages.M3UA) (int, error) { return 0, writeErr }
				if err := association.handleRegistrationRequest(registrationRequestMessage(t)); !errors.Is(err, writeErr) {
					t.Fatal(err)
				}
			} else {
				rc := registerLifetimeKey(t, association, sent, 1, testRoutingKey(10, 100, 3))
				association.signalWriter = func(messages.M3UA) (int, error) { return 0, writeErr }
				if err := association.handleDeregistrationRequest(messages.NewDeregistrationRequest(params.NewRoutingContext(rc))); !errors.Is(err, writeErr) {
					t.Fatal(err)
				}
			}
			association.signalWriter = writer
			if err := association.handleAspDown(messages.NewAspDown(nil)); err != nil {
				t.Fatal(err)
			}
			assertLifetimeRetired(t, association, 1)
			if endpoint.routingKeys.dynamicCount() != 0 {
				t.Error("failed response retained registry entry")
			}
		})
	}
}

func TestSGPASPUpWhileInactiveKeepsDynamicRegistration(t *testing.T) {
	endpoint := newRegistrationLifetimeEndpoint(t, true)
	association, sent := newRegistrationLifetimeASP(t, endpoint)
	rc := registerLifetimeKey(t, association, sent, 1, testRoutingKey(10, 100, 3))
	if err := association.handleAspUp(messages.NewAspUp(nil, nil)); err != nil {
		t.Fatal(err)
	}
	if _, ok := association.dynamicASKey(rc, false); !ok {
		t.Fatal("inactive Up retired dynamic registration")
	}
	endpoint.routingKeys.mu.Lock()
	member := endpoint.routingKeys.entries[rc] != nil && endpoint.routingKeys.entries[rc].hasMember(association)
	endpoint.routingKeys.mu.Unlock()
	if !member {
		t.Fatal("inactive Up retired registry membership")
	}
}

func TestSGPASPDownRetiresEveryDynamicRegistration(t *testing.T) {
	for _, state := range []State{StateASPDown, StateASPInactive, StateASPActive} {
		t.Run(state.String(), func(t *testing.T) {
			endpoint := newRegistrationLifetimeEndpoint(t, true)
			association, sent := newRegistrationLifetimeASP(t, endpoint)
			contexts := []uint32{
				registerLifetimeKey(t, association, sent, 1, testRoutingKey(10, 100, 3)),
				registerLifetimeKey(t, association, sent, 2, testRoutingKey(10, 200, 3)),
			}
			association.commitState(state)
			if err := association.handleAspDown(messages.NewAspDown(nil)); err != nil {
				t.Fatal(err)
			}
			for _, rc := range contexts {
				assertLifetimeRetired(t, association, rc)
			}
			if endpoint.routingKeys.dynamicCount() != 0 {
				t.Fatal("retained dynamic keys")
			}
			if err := association.handleAspDown(messages.NewAspDown(nil)); err != nil {
				t.Fatal("duplicate Down:", err)
			}
		})
	}
}

func assertLifetimeRegistered(t *testing.T, association *Association, rc uint32) {
	t.Helper()
	registry := association.routingKeyRegistry()
	registry.mu.Lock()
	entry := registry.entries[rc]
	member := entry != nil && entry.hasMember(association)
	registry.mu.Unlock()
	if !member {
		t.Error("successful REG RSP did not restore registry membership")
	}
	wantScope := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: rc, RoutingContextSet: true}
	if scope, ok := association.dynamicASKey(rc, false); !ok || scope != wantScope {
		t.Errorf("successful REG RSP did not restore dynamic scope: %+v, present=%v", scope, ok)
	}
}

func assertLifetimeDeregistrationCompleted(t *testing.T, association *Association, rc uint32) {
	t.Helper()
	registry := association.routingKeyRegistry()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	replay := registry.deregReplays[association]
	if replay == nil {
		t.Error("implicit withdrawal did not retain completed DEREG result")
		return
	}
	result, ok := replay.byRoutingContext[rc]
	if !ok || result.Status != DeregistrationSuccessfullyDeregistered {
		t.Error("implicit withdrawal did not record successful retirement")
	}
	if len(replay.pending) != 0 {
		t.Error("implicit withdrawal retained a pending response")
	}
}

func TestSGPWithdrawalNotifyTargetsSurvivingMembership(t *testing.T) {
	for _, activeUp := range []bool{false, true} {
		for _, removeUnused := range []bool{false, true} {
			for _, sibling := range []bool{false, true} {
				name := fmt.Sprintf("active-up=%v/remove-unused=%v/sibling=%v", activeUp, removeUnused, sibling)
				t.Run(name, func(t *testing.T) {
					endpoint := newRegistrationLifetimeEndpoint(t, removeUnused)
					association, sent := newRegistrationLifetimeASP(t, endpoint)
					rc := registerLifetimeKey(t, association, sent, 1, testRoutingKey(10, 100, 3))
					var survivor *Association
					if sibling {
						var otherSent *[]messages.M3UA
						survivor, otherSent = newRegistrationLifetimeASP(t, endpoint)
						registerLifetimeKey(t, survivor, otherSent, 1, testRoutingKey(10, 100, 3))
					}
					key, _ := association.dynamicASKey(rc, false)
					as, _ := endpoint.as.lookup(key)
					association.commitState(StateASPActive)
					association.noteRoutingContextsActive([]uint32{rc})
					as.setASPState(association, StateASPActive, time.Hour)
					var mu sync.Mutex
					acked := false
					retiredNotifies := 0
					pendingToSurvivor := 0
					failureToSurvivor := 0
					writer := func(retired bool) func(messages.M3UA) (int, error) {
						return func(message messages.M3UA) (int, error) {
							mu.Lock()
							defer mu.Unlock()
							switch message := message.(type) {
							case *messages.AspUpAck, *messages.AspDownAck:
								acked = true
							case *messages.Notify:
								if message.RoutingContext == nil || message.RoutingContext.RoutingContext() != rc {
									break
								}
								if !acked {
									t.Error("retirement Notify preceded ASPSM Ack")
								}
								if retired {
									retiredNotifies++
								} else {
									switch message.Status.Status() {
									case params.AsStatePending:
										pendingToSurvivor++
									case params.AspFailure:
										failureToSurvivor++
									}
								}
							}
							return message.MarshalLen(), nil
						}
					}
					association.signalWriter = writer(true)
					if sibling {
						survivor.signalWriter = writer(false)
					}
					if activeUp {
						if err := association.handleAspUp(messages.NewAspUp(nil, nil)); !isLifetimeUnexpectedMessage(err) {
							t.Fatal(err)
						}
					} else if err := association.handleAspDown(messages.NewAspDown(nil)); err != nil {
						t.Fatal(err)
					}
					mu.Lock()
					defer mu.Unlock()
					if retiredNotifies != 0 {
						t.Errorf("retired ASP received %d Notify messages for its lost RC", retiredNotifies)
					}
					if sibling && pendingToSurvivor != 1 {
						t.Errorf("surviving inactive ASP received %d AS-PENDING notifications, want 1", pendingToSurvivor)
					}
					wantFailure := 0
					if sibling && activeUp {
						wantFailure = 1
					}
					if failureToSurvivor != wantFailure {
						t.Errorf("surviving ASP received %d ASP Failure notifications, want %d", failureToSurvivor, wantFailure)
					}
				})
			}
		}
	}
}

func TestSGPDeregistrationThenASPDownInInboundOrder(t *testing.T) {
	endpoint := newRegistrationLifetimeEndpoint(t, true)
	association, sent := newRegistrationLifetimeASP(t, endpoint)
	rc := registerLifetimeKey(t, association, sent, 1, testRoutingKey(10, 100, 3))
	queue := make(chan inbound, 2)
	for _, message := range []messages.M3UA{messages.NewDeregistrationRequest(params.NewRoutingContext(rc)), messages.NewAspDown(nil)} {
		data, err := message.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		queue <- inbound{kind: inboundMessage, data: data, ppid: M3UAPPID}
	}
	close(queue)
	before := len(*sent)
	association.dispatchLoop(context.Background(), queue)
	var order []string
	for _, message := range (*sent)[before:] {
		switch message := message.(type) {
		case *messages.DeregistrationResponse:
			result, err := message.DeregistrationResults[0].DeregistrationResult()
			if err != nil {
				t.Fatal(err)
			}
			if result.DeregistrationStatus.DeregistrationStatus() != params.SuccessfullyDeregistered {
				t.Fatal("ordered DEREG failed")
			}
			order = append(order, "DEREG RSP")
		case *messages.AspDownAck:
			order = append(order, "ASP Down Ack")
		}
	}
	if !slices.Equal(order, []string{"DEREG RSP", "ASP Down Ack"}) {
		t.Fatalf("inbound response order = %v", order)
	}
	assertLifetimeRetired(t, association, rc)
	assertLifetimeDeregistrationCompleted(t, association, rc)
}

func TestIPSPASPSMKeepsDynamicRegistration(t *testing.T) {
	for _, model := range ipspDownModels {
		for _, activeUp := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/active-up=%v", model.name, activeUp), func(t *testing.T) {
				endpoint, err := NewEndpoint(EndpointConfig{Role: RoleIPSP, RoutingKeyManagement: &RoutingKeyManagementConfig{
					AuthorizeRegistration:   func(RoutingKeyRegistrationRequest) RegistrationStatus { return RegistrationSuccessfullyRegistered },
					AllowDynamicRoutingKeys: true, RemoveUnusedRoutingKeys: true,
				}})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = endpoint.Close() })
				association, sent := newIPSPDownAssociationForTest(t, model.exchange, model.aspsm, StateASPDown)
				association.cfg.SetApplicationServers()
				association.endpoint, association.as = endpoint, endpoint.as
				if err := association.handleAspUp(messages.NewAspUp(nil, nil)); err != nil {
					t.Fatal(err)
				}
				if association.State() != StateASPInactive {
					t.Fatal("IPSP ASP Up did not make the registration fixture inactive")
				}
				rc := registerLifetimeKey(t, association, sent, 1, testRoutingKey(10, 100, 3))
				association.commitState(StateASPActive)
				if activeUp {
					err := association.handleAspUp(messages.NewAspUp(nil, nil))
					if err != nil && !isLifetimeUnexpectedMessage(err) {
						t.Fatal(err)
					}
				} else if err := association.handleAspDown(messages.NewAspDown(nil)); err != nil {
					t.Fatal(err)
				}
				assertLifetimeRegistered(t, association, rc)
				key, _ := association.dynamicASKey(rc, false)
				as, ok := endpoint.as.lookup(key)
				if !ok {
					t.Fatal("IPSP AS retired")
				}
				as.mu.Lock()
				_, member := as.asps[association]
				as.mu.Unlock()
				if !member {
					t.Fatal("IPSP AS membership retired")
				}
			})
		}
	}
}
