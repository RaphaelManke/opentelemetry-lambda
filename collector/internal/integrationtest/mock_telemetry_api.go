// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package integrationtest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-lambda/collector/internal/extensionapi"
)

// TelemetryEvent represents a Lambda Telemetry API event delivered to the extension's listener.
type TelemetryEvent struct {
	Time   string         `json:"time"`
	Type   string         `json:"type"`
	Record map[string]any `json:"record"`
}

// MockTelemetryAPI simulates the AWS Lambda Telemetry API.
// It captures the subscription request and provides methods to deliver telemetry events
// to the extension's registered listener.
type MockTelemetryAPI struct {
	t      *testing.T
	server *httptest.Server

	mu              sync.Mutex
	subscribed      bool
	listenerURI     string
	extensionID     string
	subscribedTypes []string
}

// NewMockTelemetryAPI creates a mock Telemetry API server.
// The mock shares the same host:port as the Extension API since Lambda uses
// AWS_LAMBDA_RUNTIME_API for both. We handle this by embedding the telemetry
// route in the same mux or using a separate server on the same host.
func NewMockTelemetryAPI(t *testing.T) *MockTelemetryAPI {
	m := &MockTelemetryAPI{
		t: t,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/2022-07-01/telemetry", m.handleSubscribe)

	m.server = httptest.NewServer(mux)
	t.Cleanup(func() {
		m.server.Close()
	})

	return m
}

// Host returns the host:port of the mock server.
func (m *MockTelemetryAPI) Host() string {
	return m.server.URL[7:]
}

// IsSubscribed returns whether the extension has subscribed to telemetry.
func (m *MockTelemetryAPI) IsSubscribed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.subscribed
}

// ListenerURI returns the URI where the extension wants to receive telemetry events.
func (m *MockTelemetryAPI) ListenerURI() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.listenerURI
}

// WaitForSubscription blocks until the extension subscribes or timeout is reached.
func (m *MockTelemetryAPI) WaitForSubscription(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if m.IsSubscribed() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for telemetry subscription")
}

// DeliverEvents sends telemetry events to the extension's registered listener.
func (m *MockTelemetryAPI) DeliverEvents(t *testing.T, events []TelemetryEvent) {
	t.Helper()

	uri := m.ListenerURI()
	require.NotEmpty(t, uri, "extension has not subscribed to telemetry yet")

	body, err := json.Marshal(events)
	require.NoError(t, err)

	resp, err := http.Post(uri, "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
}

func (m *MockTelemetryAPI) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusInternalServerError)
		return
	}

	var req struct {
		SchemaVersion string `json:"schemaVersion"`
		EventTypes    []string `json:"types"`
		Buffering     struct {
			MaxItems  uint32 `json:"maxItems"`
			MaxBytes  uint32 `json:"maxBytes"`
			TimeoutMS uint32 `json:"timeoutMs"`
		} `json:"buffering"`
		Destination struct {
			Protocol string `json:"protocol"`
			URI      string `json:"URI"`
			Method   string `json:"method"`
			Encoding string `json:"encoding"`
		} `json:"destination"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	m.subscribed = true
	m.listenerURI = req.Destination.URI
	m.extensionID = r.Header.Get("Lambda-Extension-Identifier")
	m.subscribedTypes = req.EventTypes
	m.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`"OK"`))
}

// CombinedMockAPI provides both Extension API and Telemetry API on the same server,
// matching how Lambda exposes them on the same AWS_LAMBDA_RUNTIME_API endpoint.
type CombinedMockAPI struct {
	t      *testing.T
	server *httptest.Server

	Extension *MockExtensionAPIHandler
	Telemetry *MockTelemetryAPIHandler
}

// MockExtensionAPIHandler handles extension API requests within a combined server.
type MockExtensionAPIHandler struct {
	mu          sync.Mutex
	registered  bool
	extensionID string
	events      chan ExtensionEvent

	registerResponse extensionapi.RegisterResponse
	initErrors       []string
	exitErrors       []string
}

// MockTelemetryAPIHandler handles telemetry API requests within a combined server.
type MockTelemetryAPIHandler struct {
	mu              sync.Mutex
	subscribed      bool
	listenerURIs    []string
	extensionID     string
	subscribedTypes []string
}

// NewCombinedMockAPI creates a single server that handles both Extension API and Telemetry API routes.
func NewCombinedMockAPI(t *testing.T) *CombinedMockAPI {
	ext := &MockExtensionAPIHandler{
		extensionID: "test-extension-id-12345",
		events:      make(chan ExtensionEvent, 10),
		registerResponse: extensionapi.RegisterResponse{
			FunctionName:    "my-function",
			FunctionVersion: "$LATEST",
			Handler:         "index.handler",
		},
	}

	tel := &MockTelemetryAPIHandler{}

	c := &CombinedMockAPI{
		t:         t,
		Extension: ext,
		Telemetry: tel,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/2020-01-01/extension/register", c.handleRegister)
	mux.HandleFunc("/2020-01-01/extension/event/next", c.handleNextEvent)
	mux.HandleFunc("/2020-01-01/extension/init/error", c.handleInitError)
	mux.HandleFunc("/2020-01-01/extension/exit/error", c.handleExitError)
	mux.HandleFunc("/2022-07-01/telemetry", c.handleTelemetrySubscribe)

	c.server = httptest.NewServer(mux)
	t.Cleanup(func() {
		c.server.Close()
	})

	return c
}

// Host returns the host:port for AWS_LAMBDA_RUNTIME_API.
func (c *CombinedMockAPI) Host() string {
	return c.server.URL[7:]
}

// EnqueueEvent adds an event to be returned by the next /event/next call.
func (c *CombinedMockAPI) EnqueueEvent(event ExtensionEvent) {
	c.Extension.events <- event
}

// WaitForSubscription blocks until the extension subscribes to telemetry.
// It waits for at least 2 subscriptions: one from the lifecycle manager and
// one from the telemetryapireceiver inside the collector.
func (c *CombinedMockAPI) WaitForSubscription(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c.Telemetry.mu.Lock()
		count := len(c.Telemetry.listenerURIs)
		c.Telemetry.mu.Unlock()
		if count >= 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.Telemetry.mu.Lock()
	count := len(c.Telemetry.listenerURIs)
	c.Telemetry.mu.Unlock()
	// Accept 1 subscription too (in case config doesn't use telemetryapireceiver)
	if count >= 1 {
		return
	}
	t.Fatal("timed out waiting for telemetry subscription")
}

// DeliverEvents sends telemetry events to ALL registered extension listener URIs.
func (c *CombinedMockAPI) DeliverEvents(t *testing.T, events []TelemetryEvent) {
	t.Helper()

	c.Telemetry.mu.Lock()
	uris := append([]string{}, c.Telemetry.listenerURIs...)
	c.Telemetry.mu.Unlock()

	require.NotEmpty(t, uris, "extension has not subscribed to telemetry yet")

	body, err := json.Marshal(events)
	require.NoError(t, err)

	for _, uri := range uris {
		resp, err := http.Post(uri, "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)
	}
}

func (c *CombinedMockAPI) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	c.Extension.mu.Lock()
	c.Extension.registered = true
	c.Extension.mu.Unlock()

	w.Header().Set("Lambda-Extension-Identifier", c.Extension.extensionID)
	w.WriteHeader(http.StatusOK)

	resp := c.Extension.registerResponse
	resp.ExtensionID = c.Extension.extensionID
	body, _ := json.Marshal(resp)
	_, _ = w.Write(body)
}

func (c *CombinedMockAPI) handleNextEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	select {
	case event := <-c.Extension.events:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		body, _ := json.Marshal(event)
		_, _ = w.Write(body)
	case <-r.Context().Done():
		return
	}
}

func (c *CombinedMockAPI) handleInitError(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	errorType := r.Header.Get("Lambda-Extension-Function-Error-Type")
	c.Extension.mu.Lock()
	c.Extension.initErrors = append(c.Extension.initErrors, errorType)
	c.Extension.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"OK"}`))
}

func (c *CombinedMockAPI) handleExitError(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	errorType := r.Header.Get("Lambda-Extension-Function-Error-Type")
	c.Extension.mu.Lock()
	c.Extension.exitErrors = append(c.Extension.exitErrors, errorType)
	c.Extension.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"OK"}`))
}

func (c *CombinedMockAPI) handleTelemetrySubscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusInternalServerError)
		return
	}

	var req struct {
		SchemaVersion string   `json:"schemaVersion"`
		EventTypes    []string `json:"types"`
		Buffering     struct {
			MaxItems  uint32 `json:"maxItems"`
			MaxBytes  uint32 `json:"maxBytes"`
			TimeoutMS uint32 `json:"timeoutMs"`
		} `json:"buffering"`
		Destination struct {
			Protocol string `json:"protocol"`
			URI      string `json:"URI"`
			Method   string `json:"method"`
			Encoding string `json:"encoding"`
		} `json:"destination"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	c.Telemetry.mu.Lock()
	c.Telemetry.subscribed = true
	c.Telemetry.listenerURIs = append(c.Telemetry.listenerURIs, req.Destination.URI)
	c.Telemetry.extensionID = r.Header.Get("Lambda-Extension-Identifier")
	c.Telemetry.subscribedTypes = req.EventTypes
	c.Telemetry.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`"OK"`))
}
