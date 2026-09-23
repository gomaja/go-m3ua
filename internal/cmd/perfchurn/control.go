package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// The ASP process drives every phase; the peer serves these operations. Each
// request is one JSON body and each response one JSON body.
const (
	operationReady        = "/ready"
	operationEstablish    = "/establish"
	operationPopulate     = "/populate"
	operationTrafficStart = "/traffic/start"
	operationTrafficStop  = "/traffic/stop"
	operationOverload     = "/overload"
	operationChurn        = "/churn"
	operationFinish       = "/finish"
)

type establishResponse struct {
	Stable int    `json:"stable"`
	Millis int64  `json:"ms"`
	Error  string `json:"error,omitempty"`
}

type populateResponse struct {
	Messages int      `json:"messages"`
	Errors   []string `json:"errors,omitempty"`
}

type trafficStartRequest struct {
	Epoch          uint32 `json:"epoch"`
	PerAssociation int    `json:"per_association"`
}

type trafficStopRequest struct {
	Epoch           uint32   `json:"epoch"`
	ASPSent         []uint64 `json:"asp_sent"`
	ASPWriteErrors  uint64   `json:"asp_write_errors"`
	ASPFirstError   string   `json:"asp_first_error,omitempty"`
	ASPRefused      uint64   `json:"asp_refused"`
	DrainWaitMillis int64    `json:"drain_wait_ms"`
}

type trafficStopResponse struct {
	PeerSent        []uint64     `json:"peer_sent"`
	PeerWriteErrors uint64       `json:"peer_write_errors"`
	PeerFirstError  string       `json:"peer_first_error,omitempty"`
	PeerRefused     uint64       `json:"peer_refused"`
	Ledger          ledgerResult `json:"ledger"`
	Error           string       `json:"error,omitempty"`
}

type overloadRequest struct {
	PerAssociation int `json:"per_association"`
	Toggles        int `json:"toggles"`
}

type overloadResponse struct {
	Sent            uint64   `json:"sent"`
	WriteErrors     uint64   `json:"write_errors"`
	FirstWriteError string   `json:"first_write_error,omitempty"`
	Refused         uint64   `json:"refused"`
	Reports         int      `json:"reports"`
	ReportErrors    []string `json:"report_errors,omitempty"`
}

type churnRequest struct {
	Plan       churnPlan `json:"plan"`
	HoldMillis int64     `json:"hold_ms"`
}

type churnResponse struct {
	Stats churnStats `json:"stats"`
	Error string     `json:"error,omitempty"`
}

// controlClient never keeps an idle connection: a keep-alive connection
// holds client goroutines and a descriptor, which would make the retained
// goroutine and descriptor counts depend on HTTP connection reuse.
type controlClient struct {
	base   string
	client *http.Client
}

func newControlClient(base string) *controlClient {
	transport := &http.Transport{DisableKeepAlives: true}
	return &controlClient{base: base, client: &http.Client{Transport: transport}}
}

func (client *controlClient) call(ctx context.Context, operation string, timeout time.Duration, request, response any) error {
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, client.base+operation, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpResponse, err := client.client.Do(httpRequest)
	if err != nil {
		return fmt.Errorf("peer %s: %w", operation, err)
	}
	defer func() { _ = httpResponse.Body.Close() }()
	content, err := io.ReadAll(io.LimitReader(httpResponse.Body, 64<<20))
	if err != nil {
		return fmt.Errorf("peer %s: read response: %w", operation, err)
	}
	if httpResponse.StatusCode != http.StatusOK {
		return fmt.Errorf("peer %s: HTTP %d: %s", operation, httpResponse.StatusCode, bytes.TrimSpace(content))
	}
	if err := json.Unmarshal(content, response); err != nil {
		return fmt.Errorf("peer %s: decode response: %w", operation, err)
	}
	return nil
}

// waitReady retries the peer's ready operation until it answers, so the two
// containers may start in either order. Only a refused or failed connection
// is retried; the operation itself changes nothing.
func (client *controlClient) waitReady(ctx context.Context, window time.Duration) error {
	deadline := time.Now().Add(window)
	for {
		err := client.call(ctx, operationReady, 5*time.Second, struct{}{}, &struct{}{})
		if err == nil || time.Now().After(deadline) || ctx.Err() != nil {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// handle adapts one typed operation to an HTTP handler.
func handle[Request, Response any](operation func(context.Context, Request) (Response, error)) http.HandlerFunc {
	return func(writer http.ResponseWriter, httpRequest *http.Request) {
		if httpRequest.Method != http.MethodPost {
			http.Error(writer, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var request Request
		if err := json.NewDecoder(io.LimitReader(httpRequest.Body, 1<<20)).Decode(&request); err != nil {
			http.Error(writer, "decode request: "+err.Error(), http.StatusBadRequest)
			return
		}
		response, err := operation(httpRequest.Context(), request)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(response)
	}
}
