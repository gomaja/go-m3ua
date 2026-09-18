// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package messages_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// The codec had two spellings of every operation: the canonical encoding.Binary
// names and a family of wrappers -- Serialize, SerializeTo, DecodeX,
// DecodeFromBytes and Len -- that logged a deprecation line and forwarded. Two
// spellings of one operation is two things a reader has to check against the
// wire, and the forwarding layer wrote to the standard logger from inside a
// parse, which no library should do to its host process.
//
// This file is the inventory that keeps the removal honest in both directions:
// the wrappers must be gone from the source, and every canonical operation the
// removal was allowed to lean on must still be there. Absence is proved by
// parsing the package source, because a removed identifier cannot be named in
// Go code; presence is proved by naming the identifier, because that is what a
// downstream caller does.

// removedMethodFamilies lists, per package directory and receiver type, the
// wrapper methods that existed before the cleanup. Naming them individually
// rather than only scanning for the pattern means a wrapper that comes back on
// the type it used to live on is reported against that type.
var removedMethodFamilies = map[string]map[string][]string{
	".": {
		"AspActive":                      {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"AspActiveAck":                   {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"AspDown":                        {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"AspDownAck":                     {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"AspInactive":                    {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"AspInactiveAck":                 {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"AspUp":                          {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"AspUpAck":                       {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"Data":                           {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"DestinationAvailable":           {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"DestinationRestricted":          {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"DestinationStateAudit":          {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"DestinationUnavailable":         {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"Error":                          {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"Generic":                        {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"Header":                         {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"Heartbeat":                      {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"HeartbeatAck":                   {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"Notify":                         {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"DestinationUserPartUnavailable": {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"SignallingCongestion":           {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"RegistrationRequest":            {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"RegistrationResponse":           {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"DeregistrationRequest":          {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"DeregistrationResponse":         {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
	},
	"params": {
		"Param":                     {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"ProtocolDataPayload":       {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"RoutingKeyPayload":         {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"RegistrationResultPayload": {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
		"DeregResultPayload":        {"Serialize", "SerializeTo", "DecodeFromBytes", "Len"},
	},
}

// removedPackageFunctions lists the package-level wrappers per package
// directory.
var removedPackageFunctions = map[string][]string{
	".": {
		"Decode",
		"DecodeAspActive", "DecodeAspActiveAck", "DecodeAspDown", "DecodeAspDownAck",
		"DecodeAspInactive", "DecodeAspInactiveAck", "DecodeAspUp", "DecodeAspUpAck",
		"DecodeData", "DecodeDestinationAvailable", "DecodeDestinationRestricted",
		"DecodeDestinationStateAudit", "DecodeDestinationUnavailable",
		"DecodeDestinationUserPartUnavailable", "DecodeError", "DecodeGeneric",
		"DecodeHeader", "DecodeHeartbeat", "DecodeHeartbeatAck", "DecodeNotify",
		"DecodeRegistrationRequest", "DecodeRegistrationResponse",
		"DecodeDeregistrationRequest", "DecodeDeregistrationResponse",
		"DecodeSignallingCongestion",
	},
	"params": {
		"Decode", "DecodeMultiParams", "SerializeMultiParams",
		"DecodeProtocolDataPayload", "DecodeRoutingKeyPayload",
		"DecodeRegistrationResultPayload", "DecodeDeregResultPayload",
	},
}

// packageAPI is the declared surface of one package directory, read from its
// non-test source.
type packageAPI struct {
	funcs    map[string]token.Position
	methods  map[string]map[string]token.Position
	doc      map[string]string
	files    []string
	fileSet  *token.FileSet
	packName string
}

func readPackageAPI(t *testing.T, dir string) packageAPI {
	t.Helper()

	fileSet := token.NewFileSet()
	pkgs, err := parser.ParseDir(fileSet, dir, func(info fs.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing %s: %v", dir, err)
	}

	api := packageAPI{
		funcs:   map[string]token.Position{},
		methods: map[string]map[string]token.Position{},
		doc:     map[string]string{},
		fileSet: fileSet,
	}
	for name, pkg := range pkgs {
		if strings.HasSuffix(name, "_test") {
			continue
		}
		api.packName = name
		for path, file := range pkg.Files {
			api.files = append(api.files, path)
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok {
					continue
				}
				position := fileSet.Position(fn.Pos())
				docText := ""
				if fn.Doc != nil {
					docText = fn.Doc.Text()
				}
				if fn.Recv == nil || len(fn.Recv.List) == 0 {
					api.funcs[fn.Name.Name] = position
					api.doc["func "+fn.Name.Name] = docText
					continue
				}
				receiver := receiverTypeName(fn.Recv.List[0].Type)
				if api.methods[receiver] == nil {
					api.methods[receiver] = map[string]token.Position{}
				}
				api.methods[receiver][fn.Name.Name] = position
				api.doc[receiver+"."+fn.Name.Name] = docText
			}
		}
	}
	if api.packName == "" {
		t.Fatalf("no non-test package found in %s", dir)
	}
	return api
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

// TestDeprecatedCodecWrappersAreRemoved proves the targeted wrapper families no
// longer exist in the source of either codec package. It reads the declarations
// rather than calling anything, because a removed identifier cannot be called.
func TestDeprecatedCodecWrappersAreRemoved(t *testing.T) {
	for _, dir := range []string{".", "params"} {
		api := readPackageAPI(t, dir)

		for receiver, methods := range removedMethodFamilies[dir] {
			for _, method := range methods {
				if position, exists := api.methods[receiver][method]; exists {
					t.Errorf("%s: (*%s).%s still exists at %s; the canonical operation replaced it",
						api.packName, receiver, method, position)
				}
			}
		}
		for _, name := range removedPackageFunctions[dir] {
			if position, exists := api.funcs[name]; exists {
				t.Errorf("%s: %s still exists at %s; the canonical operation replaced it",
					api.packName, name, position)
			}
		}

		// The same removal expressed as a rule, so a wrapper reintroduced on a
		// type the table does not name is still reported.
		for receiver, methods := range api.methods {
			for method, position := range methods {
				switch method {
				case "Serialize", "SerializeTo", "DecodeFromBytes", "Len":
					t.Errorf("%s: (*%s).%s at %s reintroduces a removed codec wrapper",
						api.packName, receiver, method, position)
				}
			}
		}
		for name, position := range api.funcs {
			if name == "Decode" || (strings.HasPrefix(name, "Decode") && name != "DecodeString") ||
				strings.HasPrefix(name, "Serialize") {
				t.Errorf("%s: %s at %s reintroduces a removed codec wrapper",
					api.packName, name, position)
			}
		}
	}
}

// TestNoDeprecationLoggingSurvivesInTheCodec makes the second half of the
// removal explicit: no alias, forwarding wrapper or deprecation log line
// survives. A parser that writes to the standard logger turns a peer's
// malformed message into output on the host process's stderr.
func TestNoDeprecationLoggingSurvivesInTheCodec(t *testing.T) {
	for _, dir := range []string{".", "params"} {
		api := readPackageAPI(t, dir)
		for symbol, doc := range api.doc {
			if strings.Contains(strings.ToUpper(doc), "DEPRECATED") {
				t.Errorf("%s: %s is still documented as deprecated:\n%s", api.packName, symbol, doc)
			}
		}
		for _, path := range api.files {
			source := readSource(t, path)
			if strings.Contains(source, `log.Println("DEPRECATED`) {
				t.Errorf("%s still logs a deprecation line from inside the codec", path)
			}
		}
	}
}

// canonicalMessage names one message type together with the constructor and the
// typed parser that the public API promises for it. Building the table is the
// test: every identifier in it has to exist, and has to keep its signature, for
// this file to compile.
type canonicalMessage struct {
	name        string
	message     messages.M3UA
	parse       func([]byte) (messages.M3UA, error)
	class, kind uint8
}

func canonicalMessages() []canonicalMessage {
	protocolData := params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("payload"))
	affected := params.NewAffectedPointCode(1)
	routingKey := params.NewRoutingKey(params.NewRoutingKeyPayload(
		params.NewLocalRoutingKeyIdentifier(1), nil, nil, nil,
		params.NewRoutingKeyGroup(params.NewDestinationPointCode(3), nil, nil),
	))
	registrationResult := params.NewRegistrationResult(params.NewRegistrationResultPayload(
		params.NewLocalRoutingKeyIdentifier(1),
		params.NewRegistrationStatus(params.SuccessfullyRegistered),
		params.NewRoutingContext(1),
	))
	deregistrationResult := params.NewDeregistrationResult(params.NewDeregResultPayload(
		params.NewRoutingContext(1),
		params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
	))

	return []canonicalMessage{
		{"Data", messages.NewData(nil, nil, protocolData, nil),
			func(b []byte) (messages.M3UA, error) { return messages.ParseData(b) },
			messages.MsgClassTransfer, messages.MsgTypePayloadData},
		{"DestinationUnavailable", messages.NewDestinationUnavailable(nil, nil, affected, nil),
			func(b []byte) (messages.M3UA, error) { return messages.ParseDestinationUnavailable(b) },
			messages.MsgClassSSNM, messages.MsgTypeDestinationUnavailable},
		{"DestinationAvailable", messages.NewDestinationAvailable(nil, nil, affected, nil),
			func(b []byte) (messages.M3UA, error) { return messages.ParseDestinationAvailable(b) },
			messages.MsgClassSSNM, messages.MsgTypeDestinationAvailable},
		{"DestinationStateAudit", messages.NewDestinationStateAudit(nil, nil, affected, nil),
			func(b []byte) (messages.M3UA, error) { return messages.ParseDestinationStateAudit(b) },
			messages.MsgClassSSNM, messages.MsgTypeDestinationStateAudit},
		{"SignallingCongestion", messages.NewSignallingCongestion(nil, nil, affected, nil, nil, nil),
			func(b []byte) (messages.M3UA, error) { return messages.ParseSignallingCongestion(b) },
			messages.MsgClassSSNM, messages.MsgTypeSignallingCongestion},
		{"DestinationUserPartUnavailable", messages.NewDestinationUserPartUnavailable(
			nil, nil, affected, params.NewUserCause(params.SCCP, params.Unequipped), nil),
			func(b []byte) (messages.M3UA, error) {
				return messages.ParseDestinationUserPartUnavailable(b)
			},
			messages.MsgClassSSNM, messages.MsgTypeDestinationUserPartUnavailable},
		{"DestinationRestricted", messages.NewDestinationRestricted(nil, nil, affected, nil),
			func(b []byte) (messages.M3UA, error) { return messages.ParseDestinationRestricted(b) },
			messages.MsgClassSSNM, messages.MsgTypeDestinationRestricted},
		{"AspUp", messages.NewAspUp(params.NewAspIdentifier(1), nil),
			func(b []byte) (messages.M3UA, error) { return messages.ParseAspUp(b) },
			messages.MsgClassASPSM, messages.MsgTypeAspUp},
		{"AspUpAck", messages.NewAspUpAck(params.NewAspIdentifier(1), nil),
			func(b []byte) (messages.M3UA, error) { return messages.ParseAspUpAck(b) },
			messages.MsgClassASPSM, messages.MsgTypeAspUpAck},
		{"AspDown", messages.NewAspDown(nil),
			func(b []byte) (messages.M3UA, error) { return messages.ParseAspDown(b) },
			messages.MsgClassASPSM, messages.MsgTypeAspDown},
		{"AspDownAck", messages.NewAspDownAck(nil),
			func(b []byte) (messages.M3UA, error) { return messages.ParseAspDownAck(b) },
			messages.MsgClassASPSM, messages.MsgTypeAspDownAck},
		{"Heartbeat", messages.NewHeartbeat(params.NewHeartbeatData([]byte("beat"))),
			func(b []byte) (messages.M3UA, error) { return messages.ParseHeartbeat(b) },
			messages.MsgClassASPSM, messages.MsgTypeHeartbeat},
		{"HeartbeatAck", messages.NewHeartbeatAck(params.NewHeartbeatData([]byte("beat"))),
			func(b []byte) (messages.M3UA, error) { return messages.ParseHeartbeatAck(b) },
			messages.MsgClassASPSM, messages.MsgTypeHeartbeatAck},
		{"AspActive", messages.NewAspActive(
			params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(1), nil),
			func(b []byte) (messages.M3UA, error) { return messages.ParseAspActive(b) },
			messages.MsgClassASPTM, messages.MsgTypeAspActive},
		{"AspActiveAck", messages.NewAspActiveAck(
			params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(1), nil),
			func(b []byte) (messages.M3UA, error) { return messages.ParseAspActiveAck(b) },
			messages.MsgClassASPTM, messages.MsgTypeAspActiveAck},
		{"AspInactive", messages.NewAspInactive(params.NewRoutingContext(1), nil),
			func(b []byte) (messages.M3UA, error) { return messages.ParseAspInactive(b) },
			messages.MsgClassASPTM, messages.MsgTypeAspInactive},
		{"AspInactiveAck", messages.NewAspInactiveAck(params.NewRoutingContext(1), nil),
			func(b []byte) (messages.M3UA, error) { return messages.ParseAspInactiveAck(b) },
			messages.MsgClassASPTM, messages.MsgTypeAspInactiveAck},
		{"RegistrationRequest", messages.NewRegistrationRequest(routingKey),
			func(b []byte) (messages.M3UA, error) { return messages.ParseRegistrationRequest(b) },
			messages.MsgClassRKM, messages.MsgTypeRegistrationRequest},
		{"RegistrationResponse", messages.NewRegistrationResponse(registrationResult),
			func(b []byte) (messages.M3UA, error) { return messages.ParseRegistrationResponse(b) },
			messages.MsgClassRKM, messages.MsgTypeRegistrationResponse},
		{"DeregistrationRequest", messages.NewDeregistrationRequest(params.NewRoutingContext(1)),
			func(b []byte) (messages.M3UA, error) { return messages.ParseDeregistrationRequest(b) },
			messages.MsgClassRKM, messages.MsgTypeDeregistrationRequest},
		{"DeregistrationResponse", messages.NewDeregistrationResponse(deregistrationResult),
			func(b []byte) (messages.M3UA, error) { return messages.ParseDeregistrationResponse(b) },
			messages.MsgClassRKM, messages.MsgTypeDeregistrationResponse},
		{"Error", messages.NewError(params.NewErrorCode(params.UnexpectedMessageError), nil, nil, nil, nil),
			func(b []byte) (messages.M3UA, error) { return messages.ParseError(b) },
			messages.MsgClassManagement, messages.MsgTypeError},
		{"Notify", messages.NewNotify(params.NewStatus(params.AsStateActive), nil, nil, nil),
			func(b []byte) (messages.M3UA, error) { return messages.ParseNotify(b) },
			messages.MsgClassManagement, messages.MsgTypeNotify},
		{"Generic", messages.New(1, 0xff, 0xff, params.NewInfoString("generic")),
			func(b []byte) (messages.M3UA, error) { return messages.ParseGeneric(b) },
			0xff, 0xff},
	}
}

// canonicalMessageOperations is the operation set every typed message keeps.
var canonicalMessageOperations = []string{
	"MarshalBinary", "MarshalTo", "UnmarshalBinary", "MarshalLen", "SetLength",
	"String", "Version", "MessageClass", "MessageType", "MessageClassName", "MessageTypeName",
}

// TestCanonicalMessageOperationsAreRetained checks the other half of the
// cleanup: every message keeps its constructor, its typed parser, the
// encoding.Binary operations and the M3UA interface, and each of them still
// works on the wire rather than merely existing.
func TestCanonicalMessageOperationsAreRetained(t *testing.T) {
	inventory := canonicalMessages()
	if got, want := len(inventory), 24; got != want {
		t.Fatalf("inventory covers %d messages, want %d (23 typed messages and Generic)", got, want)
	}

	for _, entry := range inventory {
		t.Run(entry.name, func(t *testing.T) {
			messageType := reflect.TypeOf(entry.message)
			for _, operation := range canonicalMessageOperations {
				if _, exists := messageType.MethodByName(operation); !exists {
					t.Errorf("%s lost the canonical operation %s", entry.name, operation)
				}
			}

			wire, err := entry.message.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary() error = %v", err)
			}
			if got := entry.message.MarshalLen(); got != len(wire) {
				t.Errorf("MarshalLen() = %d, MarshalBinary produced %d octets", got, len(wire))
			}

			buffer := make([]byte, entry.message.MarshalLen())
			if err := entry.message.MarshalTo(buffer); err != nil {
				t.Fatalf("MarshalTo() error = %v", err)
			}

			typed, err := entry.parse(wire)
			if err != nil {
				t.Fatalf("Parse%s() error = %v", entry.name, err)
			}
			if got, want := typed.MessageClass(), entry.class; got != want {
				t.Errorf("MessageClass() = %d, want %d", got, want)
			}
			if got, want := typed.MessageType(), entry.kind; got != want {
				t.Errorf("MessageType() = %d, want %d", got, want)
			}

			// UnmarshalBinary is the operation the deleted DecodeFromBytes
			// forwarded to, so it has to work in place on a fresh value.
			fresh := reflect.New(messageType.Elem()).Interface().(messages.M3UA)
			if err := fresh.UnmarshalBinary(wire); err != nil {
				t.Fatalf("UnmarshalBinary() error = %v", err)
			}
			if got := fresh.MessageTypeName(); got != entry.message.MessageTypeName() {
				t.Errorf("UnmarshalBinary produced %q, want %q", got, entry.message.MessageTypeName())
			}
		})
	}
}

// TestCanonicalPackageEntryPointsAreRetained names the package-level operations
// the cleanup was not allowed to touch. Referencing them is the assertion.
func TestCanonicalPackageEntryPointsAreRetained(t *testing.T) {
	wire, err := messages.NewAspUp(params.NewAspIdentifier(7), params.NewInfoString("up")).MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}

	message, err := messages.Parse(wire)
	if err != nil {
		t.Fatalf("messages.Parse() error = %v", err)
	}
	if _, err := messages.ParseWithOptions(wire, messages.ParseOptions{}); err != nil {
		t.Fatalf("messages.ParseWithOptions() error = %v", err)
	}
	if _, err := messages.MarshalBinary(message); err != nil {
		t.Fatalf("messages.MarshalBinary() error = %v", err)
	}
	if !messages.IsSupported(messages.MsgClassASPSM, messages.MsgTypeAspUp) {
		t.Error("messages.IsSupported no longer recognises ASP Up")
	}
	header, err := messages.ParseHeader(wire)
	if err != nil {
		t.Fatalf("messages.ParseHeader() error = %v", err)
	}
	if _, err := messages.NewHeader(header.Version, header.Class, header.Type, header.Payload).MarshalBinary(); err != nil {
		t.Fatalf("messages.NewHeader(...).MarshalBinary() error = %v", err)
	}

	// The protocol extension and receive-tolerance surface is intentional and
	// stays: Generic keeps an unimplemented class/type addressable, and
	// ParseWithOptions keeps an explicit, caller-owned compatibility decision.
	var (
		_ messages.M3UA              = (*messages.Generic)(nil)
		_ messages.ProtocolTolerator = messages.ToleratorFunc(
			func(messages.ProtocolViolation) messages.ProtocolDecision { return messages.ProtocolReject })
		_ = []messages.ProtocolDecision{
			messages.ProtocolReject, messages.ProtocolAccept,
			messages.ProtocolDropParameter, messages.ProtocolUseLocalDefault,
		}
		_ = messages.ProtocolViolation{Kind: messages.ViolationInvalidOptionalInfoString}
	)

	// params keeps the same canonical shape.
	parameter, err := params.Parse(mustMarshalParam(t, params.NewRoutingContext(1, 2)))
	if err != nil {
		t.Fatalf("params.Parse() error = %v", err)
	}
	if got := parameter.RoutingContexts(); len(got) != 2 {
		t.Errorf("params.Parse lost the Routing Contexts: %v", got)
	}
	multi, err := params.MarshalMultiParams([]*params.Param{params.NewRoutingContext(1), params.NewInfoString("x")})
	if err != nil {
		t.Fatalf("params.MarshalMultiParams() error = %v", err)
	}
	parsed, err := params.ParseMultiParams(multi)
	if err != nil {
		t.Fatalf("params.ParseMultiParams() error = %v", err)
	}
	if len(parsed) != 2 {
		t.Fatalf("the multi-parameter round trip produced %d parameters, want 2", len(parsed))
	}
	if parsed[0].Tag != params.RoutingContext || parsed[1].Tag != params.InfoString {
		t.Errorf("round trip produced tags %#04x, %#04x, want %#04x, %#04x",
			parsed[0].Tag, parsed[1].Tag, params.RoutingContext, params.InfoString)
	}
	if got := parsed[0].RoutingContexts(); len(got) != 1 || got[0] != 1 {
		t.Errorf("round-tripped Routing Contexts = %v, want [1]", got)
	}
	if got := parsed[1].InfoString(); got != "x" {
		t.Errorf("round-tripped INFO String = %q, want %q", got, "x")
	}
}

// TestCanonicalParameterPayloadOperationsAreRetained covers the nested payload
// codecs, whose DecodeX and DecodeFromBytes wrappers went with the rest.
func TestCanonicalParameterPayloadOperationsAreRetained(t *testing.T) {
	protocolData := params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("payload"))
	payload, err := params.ParseProtocolDataPayload(protocolData.Data)
	if err != nil {
		t.Fatalf("params.ParseProtocolDataPayload() error = %v", err)
	}
	encoded, err := payload.MarshalBinary()
	if err != nil {
		t.Fatalf("ProtocolDataPayload.MarshalBinary() error = %v", err)
	}
	if got := payload.MarshalLen(); got != len(encoded) {
		t.Errorf("ProtocolDataPayload.MarshalLen() = %d, MarshalBinary produced %d", got, len(encoded))
	}
	if err := payload.MarshalTo(make([]byte, payload.MarshalLen())); err != nil {
		t.Fatalf("ProtocolDataPayload.MarshalTo() error = %v", err)
	}
	if err := new(params.ProtocolDataPayload).UnmarshalBinary(encoded); err != nil {
		t.Fatalf("ProtocolDataPayload.UnmarshalBinary() error = %v", err)
	}

	routingKey := params.NewRoutingKey(params.NewRoutingKeyPayload(
		params.NewLocalRoutingKeyIdentifier(1), nil, nil, nil,
		params.NewRoutingKeyGroup(params.NewDestinationPointCode(3), nil, nil),
	))
	if _, err := params.ParseRoutingKeyPayload(routingKey.Data); err != nil {
		t.Fatalf("params.ParseRoutingKeyPayload() error = %v", err)
	}
	if err := new(params.RoutingKeyPayload).UnmarshalBinary(routingKey.Data); err != nil {
		t.Fatalf("RoutingKeyPayload.UnmarshalBinary() error = %v", err)
	}

	registration := params.NewRegistrationResult(params.NewRegistrationResultPayload(
		params.NewLocalRoutingKeyIdentifier(1),
		params.NewRegistrationStatus(params.SuccessfullyRegistered),
		params.NewRoutingContext(1),
	))
	if _, err := params.ParseRegistrationResultPayload(registration.Data); err != nil {
		t.Fatalf("params.ParseRegistrationResultPayload() error = %v", err)
	}
	if err := new(params.RegistrationResultPayload).UnmarshalBinary(registration.Data); err != nil {
		t.Fatalf("RegistrationResultPayload.UnmarshalBinary() error = %v", err)
	}

	deregistration := params.NewDeregistrationResult(params.NewDeregResultPayload(
		params.NewRoutingContext(1),
		params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
	))
	if _, err := params.ParseDeregResultPayload(deregistration.Data); err != nil {
		t.Fatalf("params.ParseDeregResultPayload() error = %v", err)
	}
	if err := new(params.DeregResultPayload).UnmarshalBinary(deregistration.Data); err != nil {
		t.Fatalf("DeregResultPayload.UnmarshalBinary() error = %v", err)
	}

	parameterType := reflect.TypeOf(&params.Param{})
	for _, operation := range []string{
		"MarshalBinary", "MarshalTo", "UnmarshalBinary", "MarshalLen", "SetLength", "Copy", "Padding", "String",
	} {
		if _, exists := parameterType.MethodByName(operation); !exists {
			t.Errorf("params.Param lost the canonical operation %s", operation)
		}
	}
}

// TestM3UAInterfaceIsUnchanged pins the interface itself: the removal must not
// have widened or narrowed what a message has to implement.
func TestM3UAInterfaceIsUnchanged(t *testing.T) {
	interfaceType := reflect.TypeOf((*messages.M3UA)(nil)).Elem()
	got := make([]string, 0, interfaceType.NumMethod())
	for index := 0; index < interfaceType.NumMethod(); index++ {
		method := interfaceType.Method(index)
		got = append(got, fmt.Sprintf("%s%s", method.Name, strings.TrimPrefix(method.Type.String(), "func")))
	}
	sort.Strings(got)

	want := []string{
		"MarshalBinary() ([]uint8, error)",
		"MarshalLen() int",
		"MarshalTo([]uint8) error",
		"MessageClass() uint8",
		"MessageClassName() string",
		"MessageType() uint8",
		"MessageTypeName() string",
		"UnmarshalBinary([]uint8) error",
		"Version() uint8",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("M3UA method set =\n%v\nwant\n%v", got, want)
	}
}

func readSource(t *testing.T, path string) string {
	t.Helper()
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return string(source)
}

func mustMarshalParam(t *testing.T, parameter *params.Param) []byte {
	t.Helper()
	wire, err := parameter.MarshalBinary()
	if err != nil {
		t.Fatalf("Param.MarshalBinary() error = %v", err)
	}
	return wire
}
