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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/open-telemetry/opentelemetry-lambda/collector/internal/extensionapi"
)

// ExtensionEvent represents an event returned by the mock Extension API's /event/next endpoint.
type ExtensionEvent struct {
	EventType          extensionapi.EventType `json:"eventType"`
	DeadlineMs         int64                  `json:"deadlineMs"`
	RequestID          string                 `json:"requestId"`
	InvokedFunctionArn string                 `json:"invokedFunctionArn"`
}

// MockExtensionAPI simulates the AWS Lambda Extension API.
// It handles registration, event/next (long-polling), and error reporting.
type MockExtensionAPI struct {
	t      *testing.T
	server *httptest.Server

	mu          sync.Mutex
	registered  bool
	extensionID string

	// events is a channel that the test pushes events into.
	// The /event/next handler blocks until an event is available.
	events chan ExtensionEvent

	// registerResponse is the response returned by /register.
	registerResponse extensionapi.RegisterResponse

	// initErrors and exitErrors capture reported errors for assertions.
	initErrors []string
	exitErrors []string
}

// NewMockExtensionAPI creates a mock Extension API server.
func NewMockExtensionAPI(t *testing.T) *MockExtensionAPI {
	m := &MockExtensionAPI{
		t:           t,
		extensionID: "test-extension-id-12345",
		events:      make(chan ExtensionEvent, 10),
		registerResponse: extensionapi.RegisterResponse{
			FunctionName:    "my-function",
			FunctionVersion: "$LATEST",
			Handler:         "index.handler",
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/2020-01-01/extension/register", m.handleRegister)
	mux.HandleFunc("/2020-01-01/extension/event/next", m.handleNextEvent)
	mux.HandleFunc("/2020-01-01/extension/init/error", m.handleInitError)
	mux.HandleFunc("/2020-01-01/extension/exit/error", m.handleExitError)

	m.server = httptest.NewServer(mux)
	t.Cleanup(func() {
		m.server.Close()
	})

	return m
}

// Host returns the host:port of the mock server (for AWS_LAMBDA_RUNTIME_API).
func (m *MockExtensionAPI) Host() string {
	// Strip "http://" prefix
	return m.server.URL[7:]
}

// EnqueueEvent adds an event to be returned by the next /event/next call.
func (m *MockExtensionAPI) EnqueueEvent(event ExtensionEvent) {
	m.events <- event
}

// GetInitErrors returns all init errors reported by the extension.
func (m *MockExtensionAPI) GetInitErrors() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.initErrors...)
}

// GetExitErrors returns all exit errors reported by the extension.
func (m *MockExtensionAPI) GetExitErrors() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.exitErrors...)
}

func (m *MockExtensionAPI) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	m.mu.Lock()
	m.registered = true
	m.mu.Unlock()

	w.Header().Set("Lambda-Extension-Identifier", m.extensionID)
	w.WriteHeader(http.StatusOK)

	resp := m.registerResponse
	resp.ExtensionID = m.extensionID
	body, _ := json.Marshal(resp)
	_, _ = w.Write(body)
}

func (m *MockExtensionAPI) handleNextEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Block until an event is available or context is canceled
	select {
	case event := <-m.events:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		body, _ := json.Marshal(event)
		_, _ = w.Write(body)
	case <-r.Context().Done():
		// Client disconnected
		return
	}
}

func (m *MockExtensionAPI) handleInitError(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	errorType := r.Header.Get("Lambda-Extension-Function-Error-Type")
	m.mu.Lock()
	m.initErrors = append(m.initErrors, errorType)
	m.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"OK"}`))
}

func (m *MockExtensionAPI) handleExitError(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	errorType := r.Header.Get("Lambda-Extension-Function-Error-Type")
	m.mu.Lock()
	m.exitErrors = append(m.exitErrors, errorType)
	m.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"OK"}`))
}

// InvokeEvent creates an INVOKE extension event for the given request ID.
func InvokeEvent(requestID string) ExtensionEvent {
	return ExtensionEvent{
		EventType:          extensionapi.Invoke,
		DeadlineMs:         9999999999999,
		RequestID:          requestID,
		InvokedFunctionArn: fmt.Sprintf("arn:aws:lambda:us-east-1:123456789012:function:my-function"),
	}
}

// ShutdownEvent creates a SHUTDOWN extension event.
func ShutdownEvent() ExtensionEvent {
	return ExtensionEvent{
		EventType:  extensionapi.Shutdown,
		DeadlineMs: 9999999999999,
	}
}
