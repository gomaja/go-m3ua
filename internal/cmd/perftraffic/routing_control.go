package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

const routingControlLimit = 64 << 10
const routingControlTimeout = 5 * time.Second

type routingTransportAddressDTO struct {
	Address netip.Addr `json:"address"`
	Port    uint16     `json:"port"`
}

type routingTransportDTO struct {
	SGP                m3ua.SGPIdentity           `json:"sgp"`
	Association        m3ua.AssociationID         `json:"association"`
	Epoch              uint64                     `json:"epoch"`
	ASPIdentifier      uint32                     `json:"asp_identifier"`
	ASPIdentifierSet   bool                       `json:"asp_identifier_set"`
	Role               m3ua.Role                  `json:"role"`
	State              m3ua.State                 `json:"state"`
	SCTPState          string                     `json:"sctp_state"`
	Local              routingTransportAddressDTO `json:"local"`
	Remote             routingTransportAddressDTO `json:"remote"`
	InboundStreams     uint16                     `json:"inbound_streams"`
	OutboundStreams    uint16                     `json:"outbound_streams"`
	MaxMessageStreamID uint16                     `json:"max_message_stream_id"`
}

func (transport *routingTransportDTO) UnmarshalJSON(encoded []byte) error {
	type plainTransport routingTransportDTO
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return err
	}
	for _, name := range []string{"sgp", "association", "epoch", "asp_identifier", "asp_identifier_set", "role", "state", "sctp_state", "local", "remote", "inbound_streams", "outbound_streams", "max_message_stream_id"} {
		if value, present := fields[name]; !present || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("routing transport field %s is missing or null", name)
		}
	}
	return decodeRoutingControlJSON(bytes.NewReader(encoded), (*plainTransport)(transport))
}

func routingTransportDTOFromSender(inventory routingSenderInventory) (routingTransportDTO, error) {
	return makeRoutingTransportDTO(inventory.SGP, inventory.Snapshot, inventory.Epoch, inventory.MaxMessageStreamID, inventory.ASPIdentifier, true, m3ua.RoleASP)
}

func routingTransportDTOFromPeer(inventory routingPeerInventory) (routingTransportDTO, error) {
	return makeRoutingTransportDTO(inventory.SGP, inventory.Snapshot, inventory.Epoch, inventory.MaxMessageStreamID, inventory.Snapshot.PeerASPIdentifier, inventory.Snapshot.PeerASPIdentifierSet, m3ua.RoleSGP)
}

func makeRoutingTransportDTO(sgp m3ua.SGPIdentity, snapshot m3ua.AssociationSnapshot, epoch uint64, maximum uint16, identifier uint32, identifierSet bool, role m3ua.Role) (routingTransportDTO, error) {
	addresses, err := validateRoutingInventorySnapshot(snapshot, role, epoch, maximum)
	if err != nil {
		return routingTransportDTO{}, err
	}
	transport := routingTransportDTO{
		SGP: sgp, Association: snapshot.Association, Epoch: epoch, ASPIdentifier: identifier, ASPIdentifierSet: identifierSet,
		Role: snapshot.Role, State: snapshot.State, SCTPState: snapshot.SCTP.State,
		Local: routingTransportAddressDTO(addresses.Local), Remote: routingTransportAddressDTO(addresses.Remote),
		InboundStreams: snapshot.SCTP.InboundStreams, OutboundStreams: snapshot.SCTP.OutboundStreams, MaxMessageStreamID: maximum,
	}
	return transport, transport.validate(role)
}

func (transport routingTransportDTO) validate(role m3ua.Role) error {
	if !routingControlIdentity(string(transport.SGP.SignallingGateway)) || !routingControlIdentity(string(transport.SGP.SignallingGatewayProcess)) || !transport.ASPIdentifierSet {
		return errors.New("routing transport identity is missing or oversized")
	}
	snapshot := m3ua.AssociationSnapshot{
		Association: transport.Association, Role: transport.Role, State: transport.State,
		LocalAddr:  &sctp.SCTPAddr{IPAddrs: []net.IPAddr{{IP: net.IP(transport.Local.Address.AsSlice()), Zone: transport.Local.Address.Zone()}}, Port: int(transport.Local.Port)},
		RemoteAddr: &sctp.SCTPAddr{IPAddrs: []net.IPAddr{{IP: net.IP(transport.Remote.Address.AsSlice()), Zone: transport.Remote.Address.Zone()}}, Port: int(transport.Remote.Port)},
		SCTP:       &m3ua.AssociationStatus{State: transport.SCTPState, InboundStreams: transport.InboundStreams, OutboundStreams: transport.OutboundStreams},
	}
	addresses, err := validateRoutingInventorySnapshot(snapshot, role, transport.Epoch, transport.MaxMessageStreamID)
	if err != nil {
		return err
	}
	if addresses.Local != (routingAddress(transport.Local)) || addresses.Remote != (routingAddress(transport.Remote)) {
		return errors.New("routing transport address is not canonical")
	}
	return nil
}

type routingScopedASPStatusDTO struct {
	SGP    m3ua.SGPIdentity `json:"sgp"`
	Status m3ua.ASPStatus   `json:"status"`
}

type routingInventoryDTO struct {
	Ready       bool                        `json:"ready"`
	Transports  []routingTransportDTO       `json:"transports"`
	ASPStatuses []routingScopedASPStatusDTO `json:"asp_statuses"`
}

type routingControlOperations struct {
	Inventory func(context.Context) (routingInventoryDTO, error)
	Prepare   func(context.Context, []routingTransportDTO) error
	Publish   func(context.Context, uint8) error
	Stop      func(context.Context) error
}

type routingControl struct {
	mutex         sync.Mutex
	preparationID string
	operations    routingControlOperations
	phase         string
	next          uint8
	activeCancel  context.CancelFunc
	stopStarted   bool
	stopFinished  bool
	stopError     error
}

func routingControlIdentity(value string) bool {
	return value != "" && len(value) <= 128 && utf8.ValidString(value) && !strings.ContainsAny(value, "\x00\r\n")
}

func newRoutingControl(preparationID string, operations routingControlOperations) (*routingControl, error) {
	if !routingControlIdentity(preparationID) || operations.Inventory == nil || operations.Prepare == nil || operations.Publish == nil || operations.Stop == nil {
		return nil, errors.New("routing control requires an identity and every bounded operation")
	}
	return &routingControl{preparationID: preparationID, operations: operations, phase: "idle"}, nil
}

func (control *routingControl) handler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		method := http.MethodPost
		switch request.URL.Path {
		case "/routing/ready", "/routing/inventory":
			method = http.MethodGet
		case "/routing/prepare", "/routing/publication", "/routing/stop":
		default:
			http.NotFound(writer, request)
			return
		}
		if request.Method != method {
			writer.Header().Set("Allow", method)
			routingControlStatusError(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if request.URL.RawQuery != "" {
			routingControlStatusError(writer, "routing control does not accept query parameters", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(request.Context(), routingControlTimeout)
		defer cancel()
		deadline, _ := ctx.Deadline()
		transport := http.NewResponseController(writer)
		if err := transport.SetReadDeadline(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
			routingControlError(writer, err)
			return
		}
		if err := transport.SetWriteDeadline(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
			routingControlError(writer, err)
			return
		}
		if method == http.MethodGet {
			control.inventory(writer, request.WithContext(ctx))
			return
		}
		control.command(writer, request.WithContext(ctx))
	})
}

func (control *routingControl) inventory(writer http.ResponseWriter, request *http.Request) {
	control.mutex.Lock()
	stopped := control.stopStarted
	control.mutex.Unlock()
	if stopped {
		routingControlStatusError(writer, "routing control stopped", http.StatusConflict)
		return
	}
	inventory, err := control.operations.Inventory(request.Context())
	if err == nil {
		err = request.Context().Err()
	}
	if err == nil && (len(inventory.Transports) > 8 || len(inventory.ASPStatuses) > 16 || (inventory.Ready && (len(inventory.Transports) != 8 || len(inventory.ASPStatuses) != 16))) {
		err = errors.New("routing inventory has incomplete or oversized membership")
	}
	if err == nil {
		for _, transport := range inventory.Transports {
			if err = transport.validate(m3ua.RoleSGP); err != nil {
				break
			}
		}
	}
	if err != nil {
		routingControlError(writer, err)
		return
	}
	control.mutex.Lock()
	stopped = control.stopStarted
	phase := control.phase
	control.mutex.Unlock()
	if stopped {
		routingControlStatusError(writer, "routing control stopped", http.StatusConflict)
		return
	}
	if request.URL.Path == "/routing/ready" {
		writeJSON(writer, http.StatusOK, readyResult{Ready: inventory.Ready && phase != "failed", Phase: phase, Associations: len(inventory.Transports), ExpectedAssociations: 8})
		return
	}
	inventory.Transports = append([]routingTransportDTO(nil), inventory.Transports...)
	inventory.ASPStatuses = append([]routingScopedASPStatusDTO(nil), inventory.ASPStatuses...)
	value := struct {
		PreparationID string `json:"preparation_id"`
		routingInventoryDTO
	}{PreparationID: control.preparationID, routingInventoryDTO: inventory}
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) > routingControlLimit {
		routingControlError(writer, errors.New("routing inventory encoding exceeds control bounds"))
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write(encoded)
}

func (control *routingControl) command(writer http.ResponseWriter, request *http.Request) {
	var command struct {
		PreparationID   string                `json:"preparation_id"`
		Ordinal         *uint8                `json:"ordinal"`
		SenderInventory []routingTransportDTO `json:"sender_inventory"`
	}
	if err := decodeRoutingControlJSON(request.Body, &command); err != nil {
		routingControlStatusError(writer, err.Error(), http.StatusBadRequest)
		return
	}
	if command.PreparationID != control.preparationID {
		routingControlStatusError(writer, "routing preparation identity mismatch", http.StatusConflict)
		return
	}
	if request.URL.Path == "/routing/stop" {
		if command.Ordinal != nil || command.SenderInventory != nil {
			routingControlStatusError(writer, "stop contains command fields", http.StatusBadRequest)
			return
		}
		control.stop(writer)
		return
	}
	if command.Ordinal == nil {
		routingControlStatusError(writer, "routing ordinal is required", http.StatusBadRequest)
		return
	}
	preparing := request.URL.Path == "/routing/prepare"
	if preparing {
		if len(command.SenderInventory) != 8 {
			routingControlStatusError(writer, "prepare requires eight sender transports", http.StatusBadRequest)
			return
		}
		for _, transport := range command.SenderInventory {
			if err := transport.validate(m3ua.RoleASP); err != nil {
				routingControlStatusError(writer, err.Error(), http.StatusBadRequest)
				return
			}
		}
	} else if command.SenderInventory != nil {
		routingControlStatusError(writer, "publication contains sender inventory", http.StatusBadRequest)
		return
	}
	control.mutex.Lock()
	if control.stopStarted || control.phase == "failed" || control.activeCancel != nil || *command.Ordinal != control.next || control.next > 8 || preparing != (control.next == 0) {
		control.mutex.Unlock()
		routingControlStatusError(writer, "routing command is out of order or terminal", http.StatusConflict)
		return
	}
	ctx, cancel := context.WithCancel(request.Context())
	control.activeCancel = cancel
	control.next++
	control.phase = "preparing"
	control.mutex.Unlock()
	defer cancel()
	var err error
	if preparing {
		err = control.operations.Prepare(ctx, command.SenderInventory)
	} else {
		err = control.operations.Publish(ctx, *command.Ordinal-1)
	}
	if err == nil {
		err = ctx.Err()
	}
	control.mutex.Lock()
	control.activeCancel = nil
	if !control.stopStarted {
		switch {
		case err != nil:
			control.phase = "failed"
		case control.next == 9:
			control.phase = "prepared"
		default:
			control.phase = "ready"
		}
	}
	control.mutex.Unlock()
	if err != nil {
		routingControlError(writer, err)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func (control *routingControl) stop(writer http.ResponseWriter) {
	control.mutex.Lock()
	if control.stopStarted {
		finished, err := control.stopFinished, control.stopError
		control.mutex.Unlock()
		if !finished {
			routingControlStatusError(writer, "routing stop is in progress", http.StatusConflict)
			return
		}
		if err != nil {
			routingControlError(writer, err)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	control.stopStarted = true
	control.phase = "stopping"
	if control.activeCancel != nil {
		control.activeCancel()
	}
	control.mutex.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), routingControlTimeout)
	defer cancel()
	err := control.operations.Stop(ctx)
	if err == nil {
		err = ctx.Err()
	}
	control.mutex.Lock()
	control.stopFinished, control.stopError, control.phase = true, err, "stopped"
	control.mutex.Unlock()
	if err != nil {
		routingControlError(writer, err)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func routingControlError(writer http.ResponseWriter, err error) {
	routingControlStatusError(writer, err.Error(), http.StatusInternalServerError)
}

func routingControlStatusError(writer http.ResponseWriter, message string, status int) {
	if len(message) > 4095 {
		message = message[:4095]
	}
	http.Error(writer, message, status)
}

func decodeRoutingControlJSON(reader io.Reader, target any) error {
	encoded, err := io.ReadAll(io.LimitReader(reader, routingControlLimit+1))
	if err != nil {
		return err
	}
	if len(encoded) > routingControlLimit || !utf8.Valid(encoded) {
		return errors.New("routing control body is oversized or invalid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := scanRoutingControlJSON(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("routing control body has trailing JSON")
	}
	decoder = json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func scanRoutingControlJSON(decoder *json.Decoder, depth int) error {
	if depth > 16 {
		return errors.New("routing control JSON is too deeply nested")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token == nil {
		return errors.New("routing control JSON does not accept null values")
	}
	delimiter, container := token.(json.Delim)
	if !container {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]bool)
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, valid := key.(string)
			for _, character := range name {
				if character > 127 {
					return errors.New("routing control JSON member names must be ASCII")
				}
			}
			if !valid || seen[strings.ToLower(name)] {
				return errors.New("routing control JSON has a duplicate member")
			}
			seen[strings.ToLower(name)] = true
			if err := scanRoutingControlJSON(decoder, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := scanRoutingControlJSON(decoder, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("routing control JSON has an unexpected delimiter")
	}
	_, err = decoder.Token()
	return err
}

func postRoutingControlJSON(ctx context.Context, target string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(encoded) > routingControlLimit {
		return errors.New("routing control request exceeds body limit")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewBuffer(encoded))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.GetBody = nil
	client := &http.Client{Timeout: routingControlTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("routing control returned %s: %s", response.Status, body)
	}
	return nil
}
