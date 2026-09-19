// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua_test

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-m3ua/messages/params"
)

// The v1.2 contract removed a large part of the v1.1 surface: the untyped
// association I/O, the destination and restart methods that lived on Listener
// and Association, the association-wide message defaults, and the route and
// SSNM types that named a peer's wire label as though it were an identity.
//
// A hand-maintained list of what is gone rots the moment someone adds an
// export, so this file derives the inventory from the source instead. Absence
// is proved by parsing the package: a removed identifier cannot be named in Go
// code, so naming it would not compile. Presence is proved by naming it,
// because that is exactly what a downstream caller does. Between the two, the
// whole exported surface is reconciled against a checked-in inventory, so an
// export that appears or disappears has to be an act rather than an accident.

// exportedAPIInventory is the checked-in exact-head inventory. It is
// regenerated deliberately, never automatically: run
//
//	M3UA_UPDATE_EXPORT_INVENTORY=1 go test -run TestExportedSurfaceMatchesTheCheckedInInventory .
//
// in the same change that alters the API, and read the diff before committing
// it. An inventory that regenerates itself on every run proves nothing.
const exportedAPIInventory = "testdata/exported-api.txt"

// updateExportedAPIInventory is the environment variable that rewrites it.
const updateExportedAPIInventory = "M3UA_UPDATE_EXPORT_INVENTORY"

// inventoryLines reads the checked-in inventory as lines, tolerating a CRLF
// checkout. Git converts line endings on Windows unless told otherwise, and a
// trailing carriage return makes every entry miss, which reports the whole
// exported surface as undeclared rather than naming a real change. The
// .gitattributes entry keeps the file LF; this makes the test correct even
// where that is overridden.
func inventoryLines(t *testing.T) []string {
	t.Helper()

	raw, err := os.ReadFile(exportedAPIInventory)
	if err != nil {
		t.Fatalf("reading %s: %v", exportedAPIInventory, err)
	}
	text := strings.ReplaceAll(string(raw), "\r\n", "\n")
	return strings.Split(strings.TrimSpace(text), "\n")
}

// packageSurface is the exported declaration set of one package directory,
// read from its non-test source.
type packageSurface struct {
	lines []string
	doc   map[string]string
	pos   map[string]token.Position
}

func readPackageSurface(t *testing.T, dir string) packageSurface {
	t.Helper()

	fileSet := token.NewFileSet()
	packages, err := parser.ParseDir(fileSet, dir, func(info fs.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing %s: %v", dir, err)
	}

	surface := packageSurface{
		doc: map[string]string{},
		pos: map[string]token.Position{},
	}
	record := func(line string, at token.Pos, doc *ast.CommentGroup) {
		surface.lines = append(surface.lines, line)
		surface.pos[line] = fileSet.Position(at)
		if doc != nil {
			surface.doc[line] = doc.Text()
		}
	}

	for name, pkg := range packages {
		if strings.HasSuffix(name, "_test") {
			continue
		}
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				switch declaration := decl.(type) {
				case *ast.FuncDecl:
					recordFunc(declaration, record)
				case *ast.GenDecl:
					recordGenDecl(declaration, record)
				}
			}
		}
	}
	sort.Strings(surface.lines)
	return surface
}

func recordFunc(declaration *ast.FuncDecl, record func(string, token.Pos, *ast.CommentGroup)) {
	if !declaration.Name.IsExported() {
		return
	}
	if declaration.Recv == nil || len(declaration.Recv.List) == 0 {
		record("func "+declaration.Name.Name, declaration.Pos(), declaration.Doc)
		return
	}
	receiver := receiverTypeName(declaration.Recv.List[0].Type)
	if !ast.IsExported(receiver) {
		return
	}
	record("method "+receiver+"."+declaration.Name.Name, declaration.Pos(), declaration.Doc)
}

func recordGenDecl(declaration *ast.GenDecl, record func(string, token.Pos, *ast.CommentGroup)) {
	for _, spec := range declaration.Specs {
		switch specification := spec.(type) {
		case *ast.TypeSpec:
			if !specification.Name.IsExported() {
				continue
			}
			doc := specification.Doc
			if doc == nil {
				doc = declaration.Doc
			}
			switch underlying := specification.Type.(type) {
			case *ast.StructType:
				record("type "+specification.Name.Name+" struct", specification.Pos(), doc)
				recordStructFields(specification.Name.Name, underlying, record)
			case *ast.InterfaceType:
				record("type "+specification.Name.Name+" interface", specification.Pos(), doc)
				for _, method := range underlying.Methods.List {
					for _, name := range method.Names {
						record("imethod "+specification.Name.Name+"."+name.Name, name.Pos(), method.Doc)
					}
				}
			default:
				record("type "+specification.Name.Name, specification.Pos(), doc)
			}
		case *ast.ValueSpec:
			kind := "var"
			if declaration.Tok == token.CONST {
				kind = "const"
			}
			for _, name := range specification.Names {
				if !name.IsExported() {
					continue
				}
				doc := specification.Doc
				if doc == nil {
					doc = declaration.Doc
				}
				record(kind+" "+name.Name, name.Pos(), doc)
			}
		}
	}
}

func recordStructFields(typeName string, structure *ast.StructType, record func(string, token.Pos, *ast.CommentGroup)) {
	for _, field := range structure.Fields.List {
		if len(field.Names) == 0 {
			record("field "+typeName+".<embedded "+embeddedTypeName(field.Type)+">", field.Pos(), field.Doc)
			continue
		}
		for _, name := range field.Names {
			if !name.IsExported() {
				continue
			}
			record("field "+typeName+"."+name.Name, name.Pos(), field.Doc)
		}
	}
}

func receiverTypeName(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.StarExpr:
		return receiverTypeName(typed.X)
	case *ast.Ident:
		return typed.Name
	case *ast.IndexExpr:
		return receiverTypeName(typed.X)
	default:
		return ""
	}
}

func embeddedTypeName(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.StarExpr:
		return "*" + embeddedTypeName(typed.X)
	case *ast.SelectorExpr:
		return embeddedTypeName(typed.X) + "." + typed.Sel.Name
	default:
		return "?"
	}
}

// TestExportedSurfaceMatchesTheCheckedInInventory reconciles the exact head
// against the approved inventory. A deliberate API change updates
// testdata/exported-api.txt in the same commit; an accidental one fails here
// with the declaration named.
func TestExportedSurfaceMatchesTheCheckedInInventory(t *testing.T) {
	surface := readPackageSurface(t, ".")

	if os.Getenv(updateExportedAPIInventory) != "" {
		content := strings.Join(surface.lines, "\n") + "\n"
		if err := os.WriteFile(exportedAPIInventory, []byte(content), 0o600); err != nil {
			t.Fatalf("writing %s: %v", exportedAPIInventory, err)
		}
		t.Skipf("rewrote %s with %d declarations; review the diff before committing it",
			exportedAPIInventory, len(surface.lines))
	}

	approved := inventoryLines(t)
	sort.Strings(approved)

	approvedSet := make(map[string]struct{}, len(approved))
	for _, line := range approved {
		approvedSet[line] = struct{}{}
	}
	currentSet := make(map[string]struct{}, len(surface.lines))
	for _, line := range surface.lines {
		currentSet[line] = struct{}{}
	}

	for _, line := range surface.lines {
		if _, ok := approvedSet[line]; !ok {
			t.Errorf("%s at %s is exported but is not in %s; add it deliberately or unexport it",
				line, surface.pos[line], exportedAPIInventory)
		}
	}
	for _, line := range approved {
		if _, ok := currentSet[line]; !ok {
			t.Errorf("%s is in %s but no longer exists; removing an export is a breaking change",
				line, exportedAPIInventory)
		}
	}
}

// removedMethods names, per receiver, the methods the v1.2 contract removed.
// They are named individually rather than only matched by pattern so that a
// method returning on the type it used to live on is reported against that
// type.
var removedMethods = map[string][]string{
	"Association": {
		// Replaced by WriteData and ReadData taking a DataRequest and
		// returning a DataMessage.
		"Read", "ReadPD", "Write", "WritePD", "WritePDToStream",
		"WritePDToStreamWithRoutingContext", "WritePDWithRoutingContext",
		"WriteToStream", "WriteToStreamWithRoutingContext", "WriteWithRoutingContext",
		// Association-wide scope selection: a DataRequest names its own.
		"SelectRoutingContext",
		// Replaced by DeregisterApplicationServers, which names the exact scope.
		"DeregisterRoutingContexts",
		// Publication and restart moved to Endpoint.
		"BeginMTP3Restart",
		"DestinationRanges", "DestinationRangesForNetwork",
		"DestinationRangesForNetworkAndRoutingContext",
		"DestinationState", "DestinationStateForNetwork",
		"DestinationStateForNetworkAndRoutingContext",
		"DestinationStates", "DestinationStatesForNetwork",
		"ReportDestinationRange", "ReportDestinationRangeForNetwork",
		"ReportDestinationRangeForNetworkAndRoutingContext",
		"ReportDestinationState", "ReportDestinationStateForNetwork",
		"ReportDestinationStateForNetworkAndRoutingContext",
		"SetDestinationRange", "SetDestinationRangeForNetwork",
		"SetDestinationRangeForNetworkAndRoutingContext",
		"SetDestinationState", "SetDestinationStateForNetwork",
		"SetDestinationStateForNetworkAndRoutingContext",
		"PeerCongestionLevel",
	},
	"Listener": {
		"BeginMTP3Restart",
		"DestinationRanges", "DestinationRangesForNetwork",
		"DestinationRangesForNetworkAndRoutingContext",
		"DestinationState", "DestinationStateForNetwork",
		"DestinationStateForNetworkAndRoutingContext",
		"ReportDestinationRange", "ReportDestinationRangeForNetwork",
		"ReportDestinationRangeForNetworkAndRoutingContext",
		"ReportDestinationState", "ReportDestinationStateForNetwork",
		"ReportDestinationStateForNetworkAndRoutingContext",
		"SetDestinationRange", "SetDestinationRangeForNetwork",
		"SetDestinationRangeForNetworkAndRoutingContext",
		"SetDestinationState", "SetDestinationStateForNetwork",
		"SetDestinationStateForNetworkAndRoutingContext",
	},
	"AssociationConfig": {
		// Per-message data is per-message: the association holds no defaults.
		"SetCorrelationID", "SetNetworkAppearance", "SetRoutingContexts",
		"SetTrafficModeType",
	},
	"DestinationState": {"String"},
}

// removedTypes names the types the contract removed outright.
var removedTypes = []string{
	"DestinationState", // split into DestinationAvailability and CongestionState
	"SSNMScope",        // renamed WireScope
	"SGPRoute",         // replaced by MTPRoutePath candidates
	"MTPRouteBinding",  // replaced by MTPRoutePath candidates
}

// removedValues names the package-level constants and variables removed.
var removedValues = []string{
	"DestinationCongested",       // congestion is a separate dimension now
	"ErrAmbiguousRoutingContext", // ambiguity is refused by the keyed API instead
}

// removedFields names, per type, the exported struct fields removed. Message
// defaults held on the association, and the route and SSNM projections that
// reported only the first Routing Context or the first unmasked destination,
// are the two families that mattered.
var removedFields = map[string][]string{
	"AssociationConfig": {
		"CorrelationID", "DestinationPointCode", "MessagePriority",
		"NetworkAppearance", "NetworkIndicator", "OriginatingPointCode",
		"RoutingContexts", "ServiceIndicator", "SignallingLinkSelection",
		"TrafficModeType", "TrafficModes",
	},
	"ASPConfig": {
		"CongestionPolicy", "MTPRoutes", "SignallingGatewaySelection",
		"TransferFlowCacheEntries",
	},
	"SignallingGatewayConfig":        {"SGPSelection"},
	"SignallingGatewayProcessConfig": {"Routes"},
	"IPSPConfig":                     {"InitiateASPSM", "InitiateASPTM"},
	"IPSPTrafficConfig": {
		"NetworkAppearance", "RoutingContexts", "TrafficModeType", "TrafficModes",
	},
	"DataMessage": {
		"NetworkAppearance", "NetworkAppearanceSet", "RoutingContext", "RoutingContextSet",
	},
	"ManagementIndication": {
		"AffectedPointCodes", "RoutingContext", "RoutingContextSet",
	},
	"MTPTransferResult":         {"TransmittedAssociations"},
	"MTPTransferError":          {"SuccessfulSGPs"},
	"MTPTransferFailure":        {"SGP"},
	"DestinationRange":          {"CongestionLevel", "CongestionLevelSet"},
	"DestinationStatus":         {"CongestionLevel"},
	"DestinationStatusSnapshot": {"CongestionLevel", "CongestionLevelSet"},
	"HeartbeatInfo":             {"Data"},
}

// TestRemovedExportsStayRemoved reads the declarations rather than calling
// anything, because a removed identifier cannot be called.
func TestRemovedExportsStayRemoved(t *testing.T) {
	surface := readPackageSurface(t, ".")
	present := make(map[string]struct{}, len(surface.lines))
	for _, line := range surface.lines {
		present[line] = struct{}{}
	}

	assertAbsent := func(line, replacement string) {
		t.Helper()
		if _, exists := present[line]; exists {
			t.Errorf("%s exists again at %s; %s replaced it",
				line, surface.pos[line], replacement)
		}
	}

	for receiver, methods := range removedMethods {
		for _, method := range methods {
			assertAbsent("method "+receiver+"."+method, "the typed v1.2 operation")
		}
	}
	for _, name := range removedTypes {
		for _, form := range []string{"type " + name, "type " + name + " struct", "type " + name + " interface"} {
			assertAbsent(form, "its v1.2 replacement")
		}
	}
	for _, name := range removedValues {
		assertAbsent("const "+name, "its v1.2 replacement")
		assertAbsent("var "+name, "its v1.2 replacement")
	}
	for typeName, fields := range removedFields {
		for _, field := range fields {
			assertAbsent("field "+typeName+"."+field, "the per-message or keyed v1.2 form")
		}
	}
}

// TestNoSuperseded NamingPatternReturns expresses the same removal as rules, so
// a method reintroduced on a type the tables do not name is still reported.
//
//nolint:revive // the name documents the rule it enforces.
func TestNoSupersededNamingPatternReturns(t *testing.T) {
	surface := readPackageSurface(t, ".")
	for _, line := range surface.lines {
		if !strings.HasPrefix(line, "method ") {
			continue
		}
		name := line[strings.LastIndex(line, ".")+1:]
		switch {
		case strings.HasSuffix(name, "WithRoutingContext"):
			t.Errorf("%s at %s reintroduces an association-wide scope variant; "+
				"DataRequest.AS names the scope per message", line, surface.pos[line])
		case strings.HasPrefix(name, "WritePD") || name == "ReadPD":
			t.Errorf("%s at %s reintroduces the untyped Protocol Data API; "+
				"WriteData and ReadData replaced it", line, surface.pos[line])
		case strings.HasPrefix(name, "DeregisterRoutingContext"):
			t.Errorf("%s at %s reintroduces Routing-Context-keyed deregistration; "+
				"DeregisterApplicationServers names the exact ASKey", line, surface.pos[line])
		}
	}
}

// TestExportedDeclarationsAreDocumented holds the GoDoc floor. Struct fields
// are exempt because many are documented as a group under their type, and the
// conventional String, Error, Unwrap and Is implementations are exempt because
// their contract is the interface they satisfy.
func TestExportedDeclarationsAreDocumented(t *testing.T) {
	surface := readPackageSurface(t, ".")
	conventional := map[string]struct{}{"String": {}, "Error": {}, "Unwrap": {}, "Is": {}}

	for _, line := range surface.lines {
		if strings.HasPrefix(line, "field ") || strings.HasPrefix(line, "imethod ") {
			continue
		}
		if strings.HasPrefix(line, "method ") {
			name := line[strings.LastIndex(line, ".")+1:]
			if _, skip := conventional[name]; skip {
				continue
			}
		}
		if strings.TrimSpace(surface.doc[line]) == "" {
			t.Errorf("%s at %s has no doc comment", line, surface.pos[line])
		}
	}
}

// futurePromises are phrases that describe something the package does not do.
// GoDoc here states implemented behaviour; a capability that does not exist is
// either said plainly not to exist or left out.
var futurePromises = []string{
	"reserved for future",
	"not yet implemented",
	"not yet supported",
	"will be implemented",
	"will be supported",
	"will be added",
	"in a future release",
	"in a future version",
	"is planned",
	"coming soon",
	"TODO",
	"FIXME",
}

// TestGoDocDescribesImplementedBehaviour scans every exported doc comment,
// fields included, for a promise about work that has not happened.
func TestGoDocDescribesImplementedBehaviour(t *testing.T) {
	surface := readPackageSurface(t, ".")
	for _, line := range surface.lines {
		doc := surface.doc[line]
		if doc == "" {
			continue
		}
		lowered := strings.ToLower(doc)
		for _, phrase := range futurePromises {
			if strings.Contains(lowered, strings.ToLower(phrase)) {
				t.Errorf("%s at %s documents %q, which is a promise rather than behaviour",
					line, surface.pos[line], phrase)
			}
		}
	}
}

// documentedGoFence matches a fenced Go block in the published Markdown.
var documentedGoFence = regexp.MustCompile("(?s)```go\\n(.*?)```")

// qualifiedExport matches an m3ua-qualified identifier inside such a block.
var qualifiedExport = regexp.MustCompile(`\bm3ua\.([A-Z][A-Za-z0-9_]*)`)

// goIdentifier matches any identifier, for the removed-name sweep.
var goIdentifier = regexp.MustCompile(`\b[A-Za-z_][A-Za-z0-9_]*\b`)

func publishedMarkdown(t *testing.T) []string {
	t.Helper()
	files := []string{"README.md"}
	err := filepath.WalkDir("docs", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(path, ".md") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking docs: %v", err)
	}
	sort.Strings(files)
	return files
}

// TestPublishedGoExamplesNameOnlyCurrentExports is the anti-rot rule for the
// documentation itself. Prose may discuss a removed v1.1 name — the migration
// map has to — but a fenced Go block is code a reader will paste, so every
// m3ua-qualified name in one must exist at this head, and no removed name may
// appear at all.
func TestPublishedGoExamplesNameOnlyCurrentExports(t *testing.T) {
	surface := readPackageSurface(t, ".")
	exported := make(map[string]struct{}, len(surface.lines))
	for _, line := range surface.lines {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "type", "func", "const", "var":
			exported[fields[1]] = struct{}{}
		}
	}

	// A field name removed from one type is often still a field name on
	// another — ASPRoutingConfig kept MTPRoutes and CongestionPolicy, and a
	// Protocol Data payload still has Data — so only names that exist nowhere
	// in the packages a documented example may use are swept by name. The
	// fenced blocks are checked as text, which is all a reader gets.
	liveNames := map[string]struct{}{}
	for _, dir := range []string{".", "messages", "messages/params"} {
		for _, line := range readPackageSurface(t, dir).lines {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			name := fields[1]
			if index := strings.LastIndex(name, "."); index >= 0 {
				name = name[index+1:]
			}
			liveNames[strings.TrimSuffix(name, ">")] = struct{}{}
		}
	}

	removedNames := map[string]string{}
	remove := func(name, reason string) {
		if _, live := liveNames[name]; live {
			return
		}
		removedNames[name] = reason
	}
	for _, name := range removedTypes {
		remove(name, "type removed in v1.2")
	}
	for _, name := range removedValues {
		remove(name, "value removed in v1.2")
	}
	for receiver, methods := range removedMethods {
		for _, method := range methods {
			remove(method, "method removed from "+receiver+" in v1.2")
		}
	}
	for typeName, fields := range removedFields {
		for _, field := range fields {
			remove(field, "field removed from "+typeName+" in v1.2")
		}
	}

	for _, file := range publishedMarkdown(t) {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("reading %s: %v", file, err)
		}
		for _, fence := range documentedGoFence.FindAllStringSubmatch(string(raw), -1) {
			block := fence[1]
			for _, match := range qualifiedExport.FindAllStringSubmatch(block, -1) {
				if _, exists := exported[match[1]]; !exists {
					t.Errorf("%s: fenced Go block names m3ua.%s, which this package does not export",
						file, match[1])
				}
			}
			for _, identifier := range goIdentifier.FindAllString(block, -1) {
				if reason, removed := removedNames[identifier]; removed {
					t.Errorf("%s: fenced Go block names %s (%s)", file, identifier, reason)
				}
			}
		}
	}
}

// TestApprovedExportsCompile is the presence half of the reconciliation.
// Every identifier the migration map offers as the replacement for something
// removed is named here in ordinary Go, so a change to its name, its shape or
// its signature fails the build rather than the documentation.
func TestApprovedExportsCompile(t *testing.T) {
	// #37: peers and Application Server inventory, canonical identity.
	asKey := m3ua.ASKey{
		NetworkAppearance: 7, NetworkAppearanceSet: true,
		RoutingContext: 1, RoutingContextSet: true,
	}
	peer := m3ua.SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	canonical := m3ua.SGASKey{SignallingGateway: "sg-a", ApplicationServer: "as-core"}
	aspConfig := m3ua.ASPConfig{
		SignallingGateways: []m3ua.SignallingGatewayConfig{{
			ID: peer.SignallingGateway,
			SGPs: []m3ua.SignallingGatewayProcessConfig{{
				ID: peer.SignallingGatewayProcess,
				ApplicationServers: []m3ua.RemoteASConfig{{
					ID:    canonical.ApplicationServer,
					ASKey: &asKey,
				}},
			}},
		}},
	}

	// #40: path candidates, unknown-destination policy, per-path results.
	routing := m3ua.ASPRoutingConfig{
		SignallingGatewaySelection: m3ua.RouteSelectionPrimaryBackup,
		SignallingGatewayProcessSelection: map[m3ua.SignallingGatewayID]m3ua.RouteSelectionMode{
			peer.SignallingGateway: m3ua.RouteSelectionPrimaryBackup,
		},
		Paths: []m3ua.MTPRoutePath{{
			ID:                 "via-sg-a",
			SignallingGateway:  peer.SignallingGateway,
			ApplicationServers: []m3ua.RemoteASID{canonical.ApplicationServer},
		}},
		MTPRoutes: []m3ua.MTPRouteConfig{{
			ID:                   "sccp",
			DestinationPointCode: 0x222222,
			ServiceIndicators:    []uint8{params.ServiceIndSCCP},
			Paths:                []m3ua.MTPRoutePathID{"via-sg-a"},
		}},
		AllowUnknownDestinations: false,
	}
	aspConfig.Routing = &routing

	endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleASP, ASP: &aspConfig})
	if err != nil {
		t.Fatalf("NewEndpoint() error = %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	// #39: typed per-message DATA, and the inventory that replaced the
	// association-wide Routing Context, Network Appearance and Traffic Mode.
	associationConfig := m3ua.NewAssociationConfig().
		EnableHeartbeat(3*time.Second, 10*time.Second).
		SetASPIdentifier(1).
		SetApplicationServers(m3ua.ASConfig{ASKey: asKey, TrafficMode: params.TrafficModeLoadshare})
	associationConfig.PeerSGP = &peer
	_ = m3ua.DataRequest{AS: asKey, ProtocolData: params.ProtocolDataPayload{}}
	_ = m3ua.DataMessage{Scope: m3ua.WireScope{}, AS: asKey}
	_ = m3ua.DataWriteError{Outcome: m3ua.DataNotSent, AS: asKey}
	_ = []m3ua.DataSendOutcome{m3ua.DataNotSent, m3ua.DataSendIndeterminate}
	_ = associationConfig

	// #38: route-independent SSNM knowledge, atomic snapshot plus subscription.
	snapshot, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("SubscribeSSNM() error = %v", err)
	}
	defer func() { _ = subscription.Close() }()
	if snapshot.Revision != endpoint.SSNMKnowledge().Revision {
		t.Errorf("SubscribeSSNM revision %d disagrees with SSNMKnowledge", snapshot.Revision)
	}
	resynced, err := subscription.Resync()
	if err != nil {
		t.Fatalf("Resync() error = %v", err)
	}
	if resynced.Revision != snapshot.Revision {
		t.Errorf("Resync revision = %d, want %d", resynced.Revision, snapshot.Revision)
	}
	_ = m3ua.SSNMPartition{Kind: m3ua.SSNMCanonicalPartition}
	_ = m3ua.SSNMPartitionKnowledge{}
	_ = m3ua.SSNMDestinationKnowledge{}
	_ = m3ua.SSNMAvailability{}
	_ = m3ua.SSNMCongestion{}
	_ = m3ua.SSNMEvent{Kind: m3ua.SSNMReportEvent}
	_ = m3ua.SSNMBinding{}
	_ = m3ua.SSNMStateConfig{}

	// #41: publication and restart on Endpoint; availability and congestion
	// separated.
	_ = []m3ua.DestinationAvailability{
		m3ua.DestinationAvailable, m3ua.DestinationRestricted, m3ua.DestinationUnavailable,
	}
	_ = m3ua.CongestionState{}
	_ = m3ua.DestinationAvailabilityRequest{}
	_ = []func(m3ua.DestinationAvailabilityRequest) error{endpoint.ReportDestinationAvailability}
	_ = []func(...m3ua.AffectedDestination) (*m3ua.MTP3Restart, error){endpoint.BeginMTP3Restart}

	// #42: the canonical message operations live in the messages package; the
	// management indication keeps exact scope rather than a first-value
	// projection.
	var indication m3ua.ManagementIndication
	_ = indication.ASKeys
	_ = indication.AffectedDestinations

	// The route selection result and its refusal reasons.
	_ = m3ua.MTPTransferResult{SuccessfulPaths: []m3ua.MTPTransferPath{}}
	_ = m3ua.MTPSelectionError{Rejections: []m3ua.MTPCandidateRejection{}}
	_ = []m3ua.MTPSelectionReason{
		m3ua.MTPCandidateNotBound, m3ua.MTPCandidateNotActive, m3ua.MTPCandidateStateUnknown,
		m3ua.MTPCandidateUnavailable, m3ua.MTPCandidateCongested,
	}
	_ = m3ua.ErrDestinationStateUnknown

	// Deregistration by exact Application Server scope. The slice's element
	// type is the assertion: a change to the signature fails to compile here.
	_ = []func(context.Context, ...m3ua.ASKey) ([]m3ua.RoutingKeyDeregistrationResult, error){
		(*m3ua.Association)(nil).DeregisterApplicationServers,
	}

	// The Routing Key's sole representation is its groups.
	_ = m3ua.RoutingKey{Groups: []m3ua.RoutingKeyGroup{{DestinationPointCode: 0x222222}}}
}

// TestMessagesInventoryStillGuardsTheCodec keeps the two inventories linked:
// the codec surface has its own reconciliation, and this package's inventory
// deliberately does not duplicate it.
func TestMessagesInventoryStillGuardsTheCodec(t *testing.T) {
	const guard = "messages/export_inventory_test.go"
	if _, err := os.Stat(guard); err != nil {
		t.Fatalf("%s is missing: the codec export inventory is part of this contract (%v)", guard, err)
	}
}

func TestInventoryFileIsSortedAndUnique(t *testing.T) {
	lines := inventoryLines(t)
	seen := make(map[string]struct{}, len(lines))
	for index, line := range lines {
		if index > 0 && lines[index-1] > line {
			t.Fatalf("%s is not sorted at line %d (%q after %q)",
				exportedAPIInventory, index+1, line, lines[index-1])
		}
		if _, duplicate := seen[line]; duplicate {
			t.Errorf("%s repeats %q", exportedAPIInventory, line)
		}
		seen[line] = struct{}{}
	}
	if testing.Verbose() {
		fmt.Printf("%s holds %d approved exported declarations\n", exportedAPIInventory, len(lines))
	}
}

// citedTestName matches a backticked Go test-function name in the published
// documentation.
var citedTestName = regexp.MustCompile("`(Test[A-Za-z0-9_]+)`")

// declaredTestName matches a test function declaration.
var declaredTestName = regexp.MustCompile(`(?m)^func (Test[A-Za-z0-9_]+)\(`)

// TestEveryProcedureCoverageCitationExists keeps docs/procedure-coverage.md
// honest. It claims a procedure is supported by naming the test that
// demonstrates it, so a renamed or deleted test has to be noticed here rather
// than leaving the document asserting coverage that no longer exists.
func TestEveryProcedureCoverageCitationExists(t *testing.T) {
	const coverage = "docs/procedure-coverage.md"

	declared := map[string]string{}
	roots := []string{".", "messages", "messages/params", "pc"}
	for _, root := range roots {
		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatalf("reading %s: %v", root, err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(root, entry.Name())
			source, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading %s: %v", path, err)
			}
			for _, match := range declaredTestName.FindAllStringSubmatch(string(source), -1) {
				declared[match[1]] = path
			}
		}
	}

	raw, err := os.ReadFile(coverage)
	if err != nil {
		t.Fatalf("reading %s: %v", coverage, err)
	}
	cited := 0
	for _, match := range citedTestName.FindAllStringSubmatch(string(raw), -1) {
		name := match[1]
		if name == "TestEveryProcedureCoverageCitationExists" {
			continue
		}
		cited++
		if _, exists := declared[name]; !exists {
			t.Errorf("%s cites %s, which no test file declares", coverage, name)
		}
	}
	if cited < 50 {
		t.Errorf("%s cites only %d tests; the coverage claims are supposed to be evidenced", coverage, cited)
	}
}

// TestPublishedDocumentationLinksResolve checks the relative Markdown links in
// the published documentation, because a coverage document nobody can follow
// from the README is no better than an absent one.
func TestPublishedDocumentationLinksResolve(t *testing.T) {
	link := regexp.MustCompile(`\]\(([^)#]+)(#[^)]*)?\)`)
	for _, file := range publishedMarkdown(t) {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("reading %s: %v", file, err)
		}
		base := filepath.Dir(file)
		for _, match := range link.FindAllStringSubmatch(string(raw), -1) {
			target := match[1]
			if strings.Contains(target, "://") || strings.HasPrefix(target, "mailto:") {
				continue
			}
			resolved := filepath.Join(base, target)
			if _, err := os.Stat(resolved); err != nil {
				t.Errorf("%s links to %s, which does not exist", file, target)
			}
		}
	}
}
