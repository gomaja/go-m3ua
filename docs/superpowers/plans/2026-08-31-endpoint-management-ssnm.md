# Endpoint Management and SSNM Operations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete RFC 4666 Layer Management status, ASP procedure, typed SSNM, and indication APIs without coupling protocol roles to SCTP initiation.

**Architecture:** Endpoint assigns stable association identities and owns deterministic node-wide snapshots. Association owns explicit procedures addressed to one peer, while SGP-originated SSNM fan-out remains an Endpoint operation over the shared AS registry. Existing association-scoped indication channels retain fail-closed overflow behavior.

**Tech Stack:** Go 1.23+, `github.com/gomaja/go-sctp` v1.0.2, RFC 4666, standard `context` and synchronization primitives.

**Spec:** `docs/design/endpoint-management-and-ssnm.md`

## Global Constraints

- RFC 4666 is current; no RFC updates or verified errata apply as of 2026-08-31.
- Keep M3UA roles named ASP, SGP, IPSP, AS, SG, and Association; Dial, Listen, and Accept describe SCTP only.
- Keep `github.com/gomaja/go-sctp` at v1.0.2 and stop for a proven missing upstream capability rather than adding a workaround.
- Preserve exact `ASKey` Network Appearance and Routing Context presence semantics, including contextless AS.
- Reject role-invalid operations before any message write.
- Deep-copy every public snapshot and selected configuration value.
- Run the complete Go and Linux SCTP validation gates before opening the pull request.

---

### Task 1: Stable identities and keyed snapshots

**Files:**
- Create: `endpoint_status.go`
- Modify: `endpoint.go`
- Modify: `association.go`
- Modify: `as.go`
- Modify: `asp_routes.go`
- Modify: `ssnm.go`
- Test: `endpoint_status_test.go`

**Interfaces:**
- Consumes: existing `Endpoint.associations`, `applicationServers`, `aspRoutes`, `destinations`, `Association.AssociationStatus`, and `ASKey`.
- Produces: `AssociationID`, `Association.ID`, `AssociationSnapshot`, `ASPStatusKey`, `ASPStatus`, `ApplicationServerStatus`, `MTPRouteStatus`, `DestinationStatusKey`, `DestinationStatusSnapshot`, and their Endpoint query methods from the design.

- [ ] **Step 1: Write identity and association snapshot tests**

```go
func TestEndpointAssociationStatusUsesStableOwnedIDs(t *testing.T) {
    endpoint := mustEndpoint(t, RoleASP)
    first := newTestAssociation(RoleASP)
    second := newTestAssociation(RoleASP)
    if !endpoint.trackAssociation(first) || !endpoint.trackAssociation(second) {
        t.Fatal("track association")
    }
    if first.ID() == 0 || second.ID() <= first.ID() {
        t.Fatalf("IDs = %d, %d", first.ID(), second.ID())
    }
    statuses := endpoint.AssociationStatuses()
    if len(statuses) != 2 || statuses[0].Association != first.ID() || statuses[1].Association != second.ID() {
        t.Fatalf("statuses = %+v", statuses)
    }
}
```

- [ ] **Step 2: Run the identity test and confirm it fails**

Run: `GOTOOLCHAIN=go1.25.10 go test . -run TestEndpointAssociationStatusUsesStableOwnedIDs -count=1`

Expected: compile failure because `AssociationID`, `ID`, and Endpoint status methods do not exist.

- [ ] **Step 3: Implement monotonic Endpoint association IDs**

Add `nextAssociationID AssociationID` to `Endpoint`, `managementID AssociationID` to `Association`, assign it while `Endpoint.mu` is held in `trackAssociation`, and expose a read-only `ID()` method. Keep the existing atomic kernel `assocID` private and unchanged.

- [ ] **Step 4: Implement association status snapshots**

Snapshot tracked association pointers under `Endpoint.mu`, release the lock, then copy M3UA state, IPSP state, peer ASP identifier, SCTP addresses, and `AssociationStatus`. Sort by `AssociationID`; retain a racing SCTP query error in `SCTPError`.

- [ ] **Step 5: Add exact AS and ASP status tests**

```go
func TestEndpointStatusesSeparateSameRoutingContextByNetworkAppearance(t *testing.T) {
    first := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
    second := ASKey{NetworkAppearance: 20, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
    endpoint := mustSGPEndpointWithASKeys(t, first, second)
    statuses := endpoint.ApplicationServerStatuses()
    if len(statuses) != 2 || statuses[0].AS == statuses[1].AS {
        t.Fatalf("statuses = %+v", statuses)
    }
}
```

Cover contextless AS, local and peer ASP Identifiers, local ASP, remote ASP,
IPSP Single Exchange, and both IPSP Double Exchange directions.

- [ ] **Step 6: Implement ASP and Application Server snapshots**

Read per-AS ASP state under the existing AS and association locks. Return local/peer presence bits exactly as the design specifies. Copy active association IDs and sort AS keys with `compareASKey`.

- [ ] **Step 7: Add route and destination snapshot tests**

Test deterministic MTP Route ordering, mixed destination ranges, congestion-level presence, exact Network Appearance/RC matching, unknown keys, role-invalid endpoints, and caller mutation of returned slices.

- [ ] **Step 8: Implement route and destination snapshots**

Add locked internal snapshot helpers to `aspRoutes` and `destinations`. Preserve the newest covering destination record, including congestion-level presence, and never expose internal slices.

- [ ] **Step 9: Run focused tests and diagnostics**

Run:

```bash
GOTOOLCHAIN=go1.25.10 go test . -run 'TestEndpoint(Association|ASP|ApplicationServer|MTPRoute|Destination)Status' -count=1
~/go/bin/gopls check endpoint_status.go endpoint.go association.go as.go asp_routes.go ssnm.go
```

Expected: PASS with no gopls diagnostics.

- [ ] **Step 10: Commit the snapshot unit**

```bash
git add endpoint_status.go endpoint_status_test.go endpoint.go association.go as.go asp_routes.go ssnm.go docs/design/endpoint-management-and-ssnm.md docs/superpowers/plans/2026-08-31-endpoint-management-ssnm.md
git commit -m "Add keyed endpoint management status"
```

### Task 2: Automatic and explicit ASP procedures

**Files:**
- Create: `asp_procedure.go`
- Modify: `config.go`
- Modify: `association.go`
- Modify: `fsm.go`
- Modify: `aspsm.go`
- Modify: `asptm.go`
- Modify: `tack.go`
- Modify: `dial.go`
- Modify: `listener.go`
- Test: `asp_procedure_test.go`
- Test: `asp_procedure_integration_test.go`

**Interfaces:**
- Consumes: `AssociationID` from Task 1 and existing `pendingRequest`, T(ack), state, exact routing-context, and IPSP direction helpers.
- Produces: `ASPProcedureMode`, `ASPProcedurePolicy`, `AssociationConfig.ASPProcedures`, `ASPUp`, `ASPDown`, `ASPActive`, and `ASPInactive`.

- [ ] **Step 1: Write config validation and snapshot tests**

Test nil historical policy, all four explicit modes, incomplete policy rejection, SGP rejection, deep snapshot ownership, ASP defaults, and legacy IPSP initiation mapping.

- [ ] **Step 2: Run the config tests and confirm they fail**

Run: `GOTOOLCHAIN=go1.25.10 go test . -run TestASPProcedurePolicy -count=1`

Expected: compile failure because the policy types do not exist.

- [ ] **Step 3: Implement immutable procedure policy**

Add the design types, snapshot them in `snapshotAssociationConfig`, validate complete non-nil policies, and centralize role-aware mode resolution in methods on `Association`.

- [ ] **Step 4: Write explicit ASP Up and ASP Down tests**

For each method assert correct message type, matching Ack completion, timeout, context cancellation, invalid state, SGP rejection, IPSP exchange constraints, and zero writes on every rejected path.

- [ ] **Step 5: Refactor ASPSM initiation to return pending requests**

Change the internal ASP Up start path to return `*pendingRequest`, retain automatic callers through a wrapper, and make public operations wait with `waitTAck`. Publish a local M-ERROR on unsuccessful explicit operations.

- [ ] **Step 6: Write exact ASP Active and ASP Inactive tests**

Cover one ASKey, multiple RCs grouped by Traffic Mode, contextless AS, same RC under wrong Network Appearance, partial Acks, overlapping scopes, cancellation, IPSP Double Exchange `TrafficToLocal`, and zero writes for SGP or invalid state.

- [ ] **Step 7: Refactor ASPTM initiation to return all pending requests**

Add a `beginASPActive` helper returning every request produced by traffic-mode grouping, wait for all exact requests, and cancel the remaining requests if one wait fails. Reuse `beginASPInactive` for the explicit method.

- [ ] **Step 8: Write readiness and shutdown policy tests**

Test return at ASP-DOWN, ASP-INACTIVE, or ASP-ACTIVE according to policy; BEAT remains disabled before active; `ShutdownContext` sends only automatic ASPIA/ASPDN procedures; `Close` sends neither.

- [ ] **Step 9: Implement policy-driven readiness and shutdown**

Replace the one-state establishment signal with a policy-selected readiness target. Notify it after the opening ASP-DOWN transition or the matching committed state. Apply ASP Inactive and ASP Down policy independently during shutdown.

- [ ] **Step 10: Run focused and race tests**

```bash
GOTOOLCHAIN=go1.25.10 go test . -run 'TestASPProcedure|TestAssociationReadiness|TestShutdownProcedurePolicy' -count=1
GOTOOLCHAIN=go1.25.10 go test . -run 'TestASPProcedure|TestAssociationReadiness|TestShutdownProcedurePolicy' -race -count=1
~/go/bin/gopls check asp_procedure.go config.go association.go fsm.go aspsm.go asptm.go tack.go dial.go listener.go
```

- [ ] **Step 11: Commit the procedure unit**

```bash
git add asp_procedure.go asp_procedure_test.go asp_procedure_integration_test.go config.go association.go fsm.go aspsm.go asptm.go tack.go dial.go listener.go
git commit -m "Add explicit ASP management procedures"
```

### Task 3: Typed DAUD, SCON, and DUPU operations

**Files:**
- Create: `ssnm_operations.go`
- Modify: `ssnm.go`
- Modify: `mtp3restart.go`
- Modify: `errors.go`
- Test: `ssnm_operations_test.go`
- Test: `ssnm_operations_integration_test.go`

**Interfaces:**
- Consumes: Task 1 association IDs and SGP status registry, existing `activeSSNMTargets`, mandatory control writer, and message/parameter constructors.
- Produces: `SSNMScope`, `PointCodeRange`, all three request structs, both `SignallingCongestion` methods, `DestinationStateAudit`, `DestinationUserPartUnavailable`, and a typed partial fan-out error.

- [ ] **Step 1: Write request validation tests**

Test omitted and explicit-zero Network Appearance, omitted/contextless and explicit RC, empty/duplicate RC lists, mismatched AS inventory, point-code and mask bounds, empty destinations, congestion levels 0 to 4, invalid Concerned Destination direction, Info String size/UTF-8, and DUPU User/Cause tables.

- [ ] **Step 2: Run validation tests and confirm they fail**

Run: `GOTOOLCHAIN=go1.25.10 go test . -run TestSSNMOperationValidation -count=1`

Expected: compile failure because the typed request API does not exist.

- [ ] **Step 3: Implement canonical parameter builders**

Build owned Network Appearance, Routing Context, Affected Point Code, Info String, Congestion Indications, Concerned Destination, and User/Cause parameters only after role and request validation. Encode each affected point code as `uint32(mask)<<24 | pointCode`.

- [ ] **Step 4: Write ASP-to-SGP DAUD and SCON tests**

Assert exact parameter presence, values, order, state gate, ASP-only direction, SCON Concerned Destination, and zero writes for role-invalid or malformed requests.

- [ ] **Step 5: Implement Association DAUD and SCON**

Require RoleASP and an active permitted AS scope. Write one complete message through `WriteSignal`; do not modify the ASP route state from a locally originated request.

- [ ] **Step 6: Write SGP fan-out SCON and DUPU tests**

Cover exact concerned active ASP selection, multi-AS de-duplication, inactive exclusion, same RC/different NA, contextless AS, congestion omitted/zero/1-3, one unmasked DUPU destination, partial write failure, and concurrent association close.

- [ ] **Step 7: Implement Endpoint SCON and DUPU fan-out**

Snapshot `activeSSNMTargets`, group Routing Contexts per association, release AS locks, and perform concurrent mandatory writes. SCON atomically records its state and congestion level before fan-out. Return successful and failed Association IDs without replay.

- [ ] **Step 8: Write DAUD round-trip and SSNM fuzz seeds**

Exercise typed DAUD against the SGP destination registry and parse every typed message. Seed omitted/present optional parameters, explicit zeros, masks 0/24, maximum valid Info String, and every legal congestion level.

- [ ] **Step 9: Run focused and race tests**

```bash
GOTOOLCHAIN=go1.25.10 go test . ./messages/... -run 'TestSSNMOperation|FuzzTypedSSNM' -count=1
GOTOOLCHAIN=go1.25.10 go test . -run 'TestSSNMOperation' -race -count=1
~/go/bin/gopls check ssnm_operations.go ssnm.go mtp3restart.go errors.go
```

- [ ] **Step 10: Commit the SSNM unit**

```bash
git add ssnm_operations.go ssnm_operations_test.go ssnm_operations_integration_test.go ssnm.go mtp3restart.go errors.go
git commit -m "Add typed SSNM management operations"
```

### Task 4: Complete management indication scope

**Files:**
- Modify: `association.go`
- Modify: `management.go`
- Modify: `aspsm.go`
- Modify: `asptm.go`
- Modify: `errors.go`
- Test: `management_scope_test.go`
- Test: `indication_overflow_test.go`

**Interfaces:**
- Consumes: `AssociationID`, exact `ASKey` resolution, explicit procedure errors, and existing management queue.
- Produces: `ManagementIndication.Association`, `ASKeys`, `AffectedDestinations`, and `Cause`.

- [ ] **Step 1: Write full-scope indication tests**

Cover Notify explicit RC, omitted RC implied memberships, same RC/different NA, contextless AS, Error with Network Appearance/RC/APC mask, peer Error code, local procedure timeout/cancellation, SCTP release cause, and caller mutation.

- [ ] **Step 2: Run scope tests and confirm they fail**

Run: `GOTOOLCHAIN=go1.25.10 go test . -run TestManagementIndicationFullScope -count=1`

Expected: compile failure because the new fields do not exist.

- [ ] **Step 3: Implement exact management scope projection**

Populate Association ID on every indication. Resolve complete `ASKeys` from explicit or implied scope, copy every affected point code and mask into `AffectedDestinations`, and set `Cause` for local M-ERROR and M-SCTP_RELEASE. Keep legacy fields as derived projections.

- [ ] **Step 4: Strengthen overflow tests**

Fill state and management queues concurrently, assert association closure, `ErrIndicationQueueFull`, final channel closure, no panic, and no event represented as delivered after overflow.

- [ ] **Step 5: Run focused race tests and diagnostics**

```bash
GOTOOLCHAIN=go1.25.10 go test . -run 'TestManagementIndication|Test.*Indication.*Overflow' -race -count=1
~/go/bin/gopls check association.go management.go aspsm.go asptm.go errors.go
```

- [ ] **Step 6: Commit the indication unit**

```bash
git add association.go management.go aspsm.go asptm.go errors.go management_scope_test.go indication_overflow_test.go
git commit -m "Preserve complete management indication scope"
```

### Task 5: Migration, conformance, and complete validation

**Files:**
- Modify: `docs/migration-v1.2.md`
- Modify: `docs/rfc4666-conformance.md`
- Modify: `docs/standards.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: every API from Tasks 1 through 4.
- Produces: v1.2.0 migration guidance and an exact conformance record for issue #19.

- [ ] **Step 1: Document the public migration**

Add complete examples for keyed snapshots, explicit ASP lifecycle, ASP-to-SGP DAUD/SCON, SGP fan-out SCON/DUPU, contextless AS, IPSP Double Exchange, error handling, and indication resynchronization.

- [ ] **Step 2: Update the conformance matrix**

Replace issue #19 partial entries with implemented status and operation APIs. Cite RFC 4666 Sections 1.6.3, 3.4.3 to 3.4.5, 4.2, 4.3.4.1 to 4.3.4.4, and 4.5.1 to 4.5.3.

- [ ] **Step 3: Run gopls and the mandatory Go gate**

```bash
~/go/bin/gopls check $(git diff --name-only origin/main -- '*.go')
GOTOOLCHAIN=go1.25.10 go build ./...
GOTOOLCHAIN=go1.25.10 go test ./... -count=1
GOTOOLCHAIN=go1.25.10 go vet ./...
GOTOOLCHAIN=go1.25.10 ~/go/bin/staticcheck ./...
GOTOOLCHAIN=go1.25.10 ~/go/bin/golangci-lint run ./...
```

Expected: every command exits zero.

- [ ] **Step 4: Run race, fuzz smoke, portability, and security gates**

```bash
GOTOOLCHAIN=go1.25.10 go test ./... -race -count=1
GOTOOLCHAIN=go1.25.10 go test ./messages/... -run '^$' -fuzz 'FuzzTypedSSNM' -fuzztime=30s
GOOS=linux GOARCH=386 GOTOOLCHAIN=go1.25.10 go build ./...
GOOS=darwin GOARCH=amd64 GOTOOLCHAIN=go1.25.10 go build ./...
GOOS=freebsd GOARCH=amd64 GOTOOLCHAIN=go1.25.10 go build ./...
GOOS=windows GOARCH=amd64 GOTOOLCHAIN=go1.25.10 go build ./...
GOTOOLCHAIN=go1.25.10 ~/go/bin/govulncheck ./...
~/go/bin/gitleaks detect --source . --no-banner --redact
```

Expected: every command exits zero with no vulnerability or secret finding.

- [ ] **Step 5: Run Linux SCTP integration**

Build the repository in a privileged Linux container with SCTP available. Run the full suite and dedicated integration tests for policy readiness, cancellation, exact request/ack sequencing, SGP fan-out, multi-homing addresses, and repeated start/stop leak checks.

- [ ] **Step 6: Mutation-check critical guards**

Temporarily remove each role check, ASKey Network Appearance comparison, explicit-ready state gate, congestion-level bound, DUPU mask check, and overflow close. Run the exact regression test and confirm it fails for each mutation, restoring the source between mutations.

- [ ] **Step 7: Commit documentation and validation fixes**

```bash
git add README.md docs/migration-v1.2.md docs/rfc4666-conformance.md docs/standards.md
git commit -m "Document endpoint management operations"
```

- [ ] **Step 8: Push and open the issue #19 pull request**

Push `feat/endpoint-management-ssnm` and open a pull request titled `Complete endpoint management and SSNM APIs`, linking issue #19 and summarizing standards, API, tests, and migration impact in professional prose.

- [ ] **Step 9: Complete the review cycle**

Wait for every required check and automated review. Trigger a review manually if none starts. Fix or explicitly disprove each finding, rerun the affected local gate, resolve every conversation, and recheck the exact head SHA, mergeability, and checks.

- [ ] **Step 10: Squash merge and remove the branch**

Squash merge only after all required checks are green and every review conversation is resolved. Verify `main` contains the squash commit, close issue #19, and delete the remote and local feature branch/worktree.
