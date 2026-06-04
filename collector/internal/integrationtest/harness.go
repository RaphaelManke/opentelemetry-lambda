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
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/open-telemetry/opentelemetry-lambda/collector/internal/lifecycle"
	"github.com/open-telemetry/opentelemetry-lambda/collector/lambdalifecycle"
)

// InvocationScenario describes what happens during a single Lambda invocation:
// the extension event plus the telemetry events that follow.
type InvocationScenario struct {
	ExtensionEvent  ExtensionEvent
	TelemetryEvents []TelemetryEvent
}

// TestHarness orchestrates integration tests for the Lambda collector extension.
// It provides a builder pattern to configure mock API behavior, collector config,
// and invocation scenarios.
type TestHarness struct {
	t *testing.T

	// collectorConfigTemplate is a Go template for the collector config YAML.
	// Use {{.SinkEndpoint}} for the OTLP sink address and {{.TelemetryReceiverPort}} for the receiver port.
	collectorConfigTemplate string

	// invocations is the ordered list of invocations to simulate.
	invocations []InvocationScenario

	// Mock API and Sink are created during Run.
	MockAPI *CombinedMockAPI
	Sink    *OTLPSink

	// Additional env vars to set during the test.
	envVars map[string]string
}

// NewTestHarness creates a new integration test harness with sensible defaults.
func NewTestHarness(t *testing.T) *TestHarness {
	return &TestHarness{
		t:       t,
		envVars: make(map[string]string),
	}
}

// WithCollectorConfig sets a custom collector configuration template.
// Available template variables:
//   - {{.SinkEndpoint}} - the OTLP HTTP sink endpoint (host:port)
func (h *TestHarness) WithCollectorConfig(config string) *TestHarness {
	h.collectorConfigTemplate = config
	return h
}

// WithInvocation adds an invocation scenario.
func (h *TestHarness) WithInvocation(scenario InvocationScenario) *TestHarness {
	h.invocations = append(h.invocations, scenario)
	return h
}

// WithEnv sets an additional environment variable for the test.
func (h *TestHarness) WithEnv(key, value string) *TestHarness {
	h.envVars[key] = value
	return h
}

// defaultCollectorConfig returns the default collector config template.
// The telemetryapireceiver subscribes to the telemetry API independently
// and produces traces/metrics/logs from Lambda platform events.
func defaultCollectorConfig() string {
	return `receivers:
  telemetryapi:
    types: [platform, function, extension]
    port: {{.TelemetryReceiverPort}}

exporters:
  otlphttp:
    endpoint: http://{{.SinkEndpoint}}
    encoding: json
    compression: none
    tls:
      insecure: true

service:
  pipelines:
    traces:
      receivers: [telemetryapi]
      exporters: [otlphttp]
    metrics:
      receivers: [telemetryapi]
      exporters: [otlphttp]
    logs:
      receivers: [telemetryapi]
      exporters: [otlphttp]
  telemetry:
    metrics:
      level: none
`
}

// Run executes the integration test:
// 1. Starts the mock APIs and OTLP sink
// 2. Writes the collector config to a temp file
// 3. Sets environment variables
// 4. Starts the lifecycle manager (the extension under test)
// 5. Waits for subscription, then delivers events per invocation scenario
// 6. Sends a SHUTDOWN event to cleanly stop the extension
func (h *TestHarness) Run() {
	h.t.Helper()

	// Create infrastructure
	h.Sink = NewOTLPSink(h.t)
	h.MockAPI = NewCombinedMockAPI(h.t)

	// Determine config
	configTemplate := h.collectorConfigTemplate
	if configTemplate == "" {
		configTemplate = defaultCollectorConfig()
	}

	// Resolve template variables
	config := strings.ReplaceAll(configTemplate, "{{.SinkEndpoint}}", h.Sink.Endpoint())
	config = strings.ReplaceAll(config, "{{.TelemetryReceiverPort}}", fmt.Sprintf("%d", getFreePort(h.t)))

	// Write config to temp file
	configFile, err := os.CreateTemp("", "collector-config-*.yaml")
	require.NoError(h.t, err)
	_, err = configFile.WriteString(config)
	require.NoError(h.t, err)
	require.NoError(h.t, configFile.Close())
	h.t.Cleanup(func() {
		os.Remove(configFile.Name())
	})

	// Set environment variables
	h.setEnv("AWS_LAMBDA_RUNTIME_API", h.MockAPI.Host())
	h.setEnv("AWS_SAM_LOCAL", "true")
	h.setEnv("OPENTELEMETRY_COLLECTOR_CONFIG_URI", "file:"+configFile.Name())
	h.setEnv("AWS_LAMBDA_FUNCTION_NAME", "my-function")
	h.setEnv("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "128")
	h.setEnv("AWS_LAMBDA_FUNCTION_VERSION", "$LATEST")
	h.setEnv("AWS_REGION", "us-east-1")

	for k, v := range h.envVars {
		h.setEnv(k, v)
	}

	// Start the extension in a goroutine
	logger := zaptest.NewLogger(h.t)
	var wg sync.WaitGroup
	wg.Add(1)

	var runErr error
	go func() {
		defer wg.Done()
		ctx, lm := lifecycle.NewManager(context.Background(), logger, "test")
		lambdalifecycle.SetNotifier(lm)
		runErr = lm.Run(ctx)
	}()

	// Wait for the extension to subscribe to telemetry
	h.MockAPI.WaitForSubscription(h.t, 10*time.Second)

	// Allow a short settling time for the collector to fully start
	time.Sleep(200 * time.Millisecond)

	// Drive invocation scenarios
	for _, invocation := range h.invocations {
		// Enqueue the extension event (INVOKE)
		h.MockAPI.EnqueueEvent(invocation.ExtensionEvent)

		// Small delay to let the extension process the event
		time.Sleep(50 * time.Millisecond)

		// Deliver telemetry events to the extension's listener
		if len(invocation.TelemetryEvents) > 0 {
			h.MockAPI.DeliverEvents(h.t, invocation.TelemetryEvents)
		}

		// Wait for the extension to process the runtimeDone event
		time.Sleep(100 * time.Millisecond)
	}

	// Send SHUTDOWN to cleanly stop the extension
	h.MockAPI.EnqueueEvent(ShutdownEvent())

	// Wait for the extension to exit
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Extension exited cleanly
	case <-time.After(10 * time.Second):
		h.t.Fatal("timed out waiting for extension to shut down")
	}

	if runErr != nil {
		h.t.Logf("extension Run() returned error: %v", runErr)
	}
}

func (h *TestHarness) setEnv(key, value string) {
	h.t.Helper()
	old, existed := os.LookupEnv(key)
	require.NoError(h.t, os.Setenv(key, value))
	h.t.Cleanup(func() {
		if existed {
			os.Setenv(key, old)
		} else {
			os.Unsetenv(key)
		}
	})
}

// getFreePort returns a free TCP port on localhost.
func getFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}
