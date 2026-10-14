package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"unicode/utf8"
)

const routingDataControlLimit = 1 << 20

type routingDataControlOperations struct {
	Start    func(context.Context, string, uint64) error
	Complete func(context.Context) ([]routingDataReceiptDTO, error)
}

type routingDataPeerController struct {
	ctx       context.Context
	cancel    context.CancelFunc
	plane     *routingDataPeerPlane
	mutex     sync.Mutex
	session   *routingDataPeerSession
	started   bool
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

func newRoutingDataPeerController(ctx context.Context, plane *routingDataPeerPlane) (*routingDataPeerController, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if plane == nil || plane.close == nil {
		return nil, errors.New("routing DATA peer controller requires an owned peer plane")
	}
	lifetime, cancel := context.WithCancel(ctx)
	return &routingDataPeerController{ctx: lifetime, cancel: cancel, plane: plane}, nil
}

func (controller *routingDataPeerController) operations() routingDataControlOperations {
	return routingDataControlOperations{Start: controller.start, Complete: controller.complete}
}

func (controller *routingDataPeerController) start(requestContext context.Context, cohort string, seed uint64) error {
	if err := requestContext.Err(); err != nil {
		return err
	}
	controller.mutex.Lock()
	defer controller.mutex.Unlock()
	if controller.started || controller.closed || controller.ctx.Err() != nil {
		return errors.New("routing DATA peer controller is already started or closed")
	}
	session, err := newRoutingDataPeerSession(controller.ctx, controller.plane, cohort, seed)
	if err != nil {
		return err
	}
	if err := requestContext.Err(); err != nil {
		return errors.Join(err, session.Close())
	}
	controller.session = session
	controller.started = true
	return nil
}

func (controller *routingDataPeerController) complete(requestContext context.Context) ([]routingDataReceiptDTO, error) {
	controller.mutex.Lock()
	session := controller.session
	closed := controller.closed
	controller.mutex.Unlock()
	if session == nil || closed {
		return nil, errors.New("routing DATA peer controller is not active")
	}
	return session.Complete(requestContext)
}

func (controller *routingDataPeerController) Close() error {
	if controller == nil {
		return nil
	}
	controller.closeOnce.Do(func() {
		controller.mutex.Lock()
		controller.closed = true
		session := controller.session
		controller.mutex.Unlock()
		controller.cancel()
		var sessionErr error
		if session != nil {
			sessionErr = session.Close()
		}
		controller.closeErr = errors.Join(sessionErr, controller.plane.close())
	})
	return controller.closeErr
}

type routingDataControlHandler struct {
	base          http.Handler
	preparationID string
	operations    routingDataControlOperations
	mutex         sync.Mutex
	phase         string
}

func newRoutingDataControlHandler(base http.Handler, preparationID string, operations routingDataControlOperations) (http.Handler, error) {
	if base == nil || !routingControlIdentity(preparationID) || operations.Start == nil || operations.Complete == nil {
		return nil, errors.New("routing DATA control requires a base handler, identity and operations")
	}
	return &routingDataControlHandler{base: base, preparationID: preparationID, operations: operations, phase: "idle"}, nil
}

func (handler *routingDataControlHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/routing/data/start" && request.URL.Path != "/routing/data/complete" {
		handler.base.ServeHTTP(writer, request)
		return
	}
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		routingControlStatusError(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if request.URL.RawQuery != "" {
		routingControlStatusError(writer, "routing DATA control does not accept query parameters", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), routingControlTimeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	response := http.NewResponseController(writer)
	if err := response.SetReadDeadline(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
		routingControlError(writer, err)
		return
	}
	if err := response.SetWriteDeadline(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
		routingControlError(writer, err)
		return
	}
	var command struct {
		PreparationID string  `json:"preparation_id"`
		Cohort        *string `json:"cohort,omitempty"`
		Seed          *uint64 `json:"seed,omitempty"`
	}
	if err := decodeRoutingDataJSON(request.Body, &command); err != nil {
		routingControlStatusError(writer, err.Error(), http.StatusBadRequest)
		return
	}
	if command.PreparationID != handler.preparationID {
		routingControlStatusError(writer, "routing DATA preparation identity mismatch", http.StatusConflict)
		return
	}
	starting := request.URL.Path == "/routing/data/start"
	if starting != (command.Cohort != nil && command.Seed != nil) || (!starting && (command.Cohort != nil || command.Seed != nil)) || starting && !routingControlIdentity(*command.Cohort) {
		routingControlStatusError(writer, "routing DATA command fields are incomplete or contradictory", http.StatusBadRequest)
		return
	}
	handler.mutex.Lock()
	if starting && handler.phase != "idle" || !starting && handler.phase != "started" {
		handler.mutex.Unlock()
		routingControlStatusError(writer, "routing DATA command is out of order or terminal", http.StatusConflict)
		return
	}
	if starting {
		handler.phase = "starting"
	} else {
		handler.phase = "completing"
	}
	handler.mutex.Unlock()
	if starting {
		err := handler.operations.Start(ctx, *command.Cohort, *command.Seed)
		handler.mutex.Lock()
		if err != nil {
			handler.phase = "failed"
		} else {
			handler.phase = "started"
		}
		handler.mutex.Unlock()
		if err != nil {
			routingControlError(writer, err)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	receipts, err := handler.operations.Complete(ctx)
	if err == nil && len(receipts) != routingRouteCount {
		err = errors.New("routing DATA completion does not contain exactly one thousand receipts")
	}
	value := struct {
		PreparationID string                  `json:"preparation_id"`
		Receipts      []routingDataReceiptDTO `json:"receipts"`
	}{PreparationID: handler.preparationID, Receipts: receipts}
	var encoded []byte
	if err == nil {
		encoded, err = json.Marshal(value)
		if err == nil && len(encoded) > routingDataControlLimit {
			err = errors.New("routing DATA completion exceeds its response bound")
		}
	}
	handler.mutex.Lock()
	if err != nil {
		handler.phase = "failed"
	} else {
		handler.phase = "complete"
	}
	handler.mutex.Unlock()
	if err != nil {
		routingControlError(writer, err)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}

type routingDataHTTPClient struct {
	baseURL       string
	preparationID string
	preparation   *routingPreparationHTTPClient
}

func newRoutingDataHTTPClient(baseURL, preparationID string) (*routingDataHTTPClient, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") || !routingControlIdentity(preparationID) {
		return nil, errors.New("routing DATA control address or identity is invalid")
	}
	preparation, err := newRoutingPreparationHTTPClient(baseURL, preparationID)
	if err != nil {
		return nil, err
	}
	return &routingDataHTTPClient{baseURL: strings.TrimRight(baseURL, "/"), preparationID: preparationID, preparation: preparation}, nil
}

func (client *routingDataHTTPClient) Start(ctx context.Context, cohort string, seed uint64) error {
	if !routingControlIdentity(cohort) {
		return errors.New("routing DATA cohort is invalid")
	}
	return postRoutingDataJSON(ctx, client.baseURL+"/routing/data/start", struct {
		PreparationID string `json:"preparation_id"`
		Cohort        string `json:"cohort"`
		Seed          uint64 `json:"seed"`
	}{PreparationID: client.preparationID, Cohort: cohort, Seed: seed}, http.StatusNoContent, nil)
}

func (client *routingDataHTTPClient) Complete(ctx context.Context) ([]routingDataReceiptDTO, error) {
	var value struct {
		PreparationID *string                 `json:"preparation_id"`
		Receipts      []routingDataReceiptDTO `json:"receipts"`
	}
	err := postRoutingDataJSON(ctx, client.baseURL+"/routing/data/complete", struct {
		PreparationID string `json:"preparation_id"`
	}{PreparationID: client.preparationID}, http.StatusOK, &value)
	if err != nil {
		return nil, err
	}
	if value.PreparationID == nil || *value.PreparationID != client.preparationID || len(value.Receipts) != routingRouteCount {
		return nil, errors.New("routing DATA completion identity or receipt count differs")
	}
	return value.Receipts, nil
}

func (client *routingDataHTTPClient) Stop(ctx context.Context) error {
	return client.preparation.Stop(ctx)
}

func decodeRoutingDataJSON(reader io.Reader, target any) error {
	encoded, err := io.ReadAll(io.LimitReader(reader, routingDataControlLimit+1))
	if err != nil {
		return err
	}
	if len(encoded) > routingDataControlLimit || !utf8.Valid(encoded) {
		return errors.New("routing DATA control body is oversized or invalid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := scanRoutingControlJSON(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("routing DATA control body has trailing JSON")
	}
	decoder = json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func postRoutingDataJSON(ctx context.Context, target string, value any, expectedStatus int, result any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(encoded) > routingDataControlLimit {
		return errors.New("routing DATA control request exceeds body limit")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(encoded))
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
	if response.StatusCode != expectedStatus {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("routing DATA control returned %s: %s", response.Status, body)
	}
	if result == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(response.Body, 1))
		return err
	}
	return decodeRoutingDataJSON(response.Body, result)
}
