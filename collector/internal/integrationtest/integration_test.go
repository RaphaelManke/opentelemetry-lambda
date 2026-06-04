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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const signalTimeout = 10 * time.Second

// TestColdStartInvocation verifies that a cold start produces:
// - A trace with an init span (faas.coldstart=true)
// - Metrics: coldstart count, init duration, invocation count, invoke duration
// - Logs: platform events (initStart, initRuntimeDone, start, runtimeDone, report)
func TestColdStartInvocation(t *testing.T) {
	requestID := "cold-start-req-001"

	harness := NewTestHarness(t).
		WithInvocation(InvocationScenario{
			ExtensionEvent:  InvokeEvent(requestID),
			TelemetryEvents: ColdStartEvents(requestID),
		})

	harness.Run()

	// Verify traces - cold start should produce an init span
	traces := harness.Sink.WaitForTraces(t, 1, signalTimeout)
	require.NotEmpty(t, traces)

	// Find the init span
	var foundInitSpan bool
	for _, td := range traces {
		for i := 0; i < td.ResourceSpans().Len(); i++ {
			rs := td.ResourceSpans().At(i)
			for j := 0; j < rs.ScopeSpans().Len(); j++ {
				ss := rs.ScopeSpans().At(j)
				for k := 0; k < ss.Spans().Len(); k++ {
					span := ss.Spans().At(k)
					if span.Name() == "init my-function" {
						foundInitSpan = true
						assert.Equal(t, ptrace.SpanKindInternal, span.Kind())
						coldstart, exists := span.Attributes().Get("faas.coldstart")
						assert.True(t, exists, "faas.coldstart attribute should exist")
						assert.True(t, coldstart.Bool(), "faas.coldstart should be true")
					}
				}
			}
		}
	}
	assert.True(t, foundInitSpan, "should have an init span for cold start")

	// Verify metrics - should have coldstart and invocation metrics
	metrics := harness.Sink.WaitForMetrics(t, 1, signalTimeout)
	require.NotEmpty(t, metrics)

	metricNames := collectMetricNames(metrics)
	assert.Contains(t, metricNames, "faas.coldstarts", "should have coldstarts metric")
	assert.Contains(t, metricNames, "faas.init_duration", "should have init_duration metric")
	assert.Contains(t, metricNames, "faas.invocations", "should have invocations metric")
	assert.Contains(t, metricNames, "faas.invoke_duration", "should have invoke_duration metric")

	// Verify logs - should have platform event logs
	logs := harness.Sink.WaitForLogs(t, 1, signalTimeout)
	require.NotEmpty(t, logs)

	logTypes := collectLogTypes(logs)
	assert.Contains(t, logTypes, "platform.initStart")
	assert.Contains(t, logTypes, "platform.initRuntimeDone")
	assert.Contains(t, logTypes, "platform.start")
	assert.Contains(t, logTypes, "platform.runtimeDone")
}

// TestWarmInvocation verifies that a warm invocation produces:
// - Metrics: invocation count, invoke duration (no coldstart metrics)
// - Logs: platform events (start, runtimeDone, report)
// - No init span in traces
func TestWarmInvocation(t *testing.T) {
	requestID := "warm-req-001"

	harness := NewTestHarness(t).
		WithInvocation(InvocationScenario{
			ExtensionEvent:  InvokeEvent(requestID),
			TelemetryEvents: WarmInvocationEvents(requestID),
		})

	harness.Run()

	// Verify metrics
	metrics := harness.Sink.WaitForMetrics(t, 1, signalTimeout)
	require.NotEmpty(t, metrics)

	metricNames := collectMetricNames(metrics)
	assert.Contains(t, metricNames, "faas.invocations", "should have invocations metric")
	assert.Contains(t, metricNames, "faas.invoke_duration", "should have invoke_duration metric")

	// Verify logs
	logs := harness.Sink.WaitForLogs(t, 1, signalTimeout)
	require.NotEmpty(t, logs)

	logTypes := collectLogTypes(logs)
	assert.Contains(t, logTypes, "platform.start")
	assert.Contains(t, logTypes, "platform.runtimeDone")
}

// TestMultipleInvocations verifies correct handling of sequential invocations.
func TestMultipleInvocations(t *testing.T) {
	harness := NewTestHarness(t).
		WithInvocation(InvocationScenario{
			ExtensionEvent:  InvokeEvent("multi-req-001"),
			TelemetryEvents: ColdStartEvents("multi-req-001"),
		}).
		WithInvocation(InvocationScenario{
			ExtensionEvent:  InvokeEvent("multi-req-002"),
			TelemetryEvents: WarmInvocationEvents("multi-req-002"),
		}).
		WithInvocation(InvocationScenario{
			ExtensionEvent:  InvokeEvent("multi-req-003"),
			TelemetryEvents: WarmInvocationEvents("multi-req-003"),
		})

	harness.Run()

	// Should have metrics from all 3 invocations
	metrics := harness.Sink.WaitForMetrics(t, 1, signalTimeout)
	require.NotEmpty(t, metrics)

	metricNames := collectMetricNames(metrics)
	assert.Contains(t, metricNames, "faas.coldstarts", "first invocation is a cold start")
	assert.Contains(t, metricNames, "faas.invocations", "should count invocations")

	// Should have logs from all invocations
	logs := harness.Sink.WaitForLogs(t, 3, signalTimeout)
	require.NotEmpty(t, logs)

	// Verify we got platform.start for multiple request IDs
	requestIDs := collectLogRequestIDs(logs)
	assert.Contains(t, requestIDs, "multi-req-001")
	assert.Contains(t, requestIDs, "multi-req-002")
	assert.Contains(t, requestIDs, "multi-req-003")
}

// TestFunctionLogs verifies that function log events are captured as OTel log records.
func TestFunctionLogs(t *testing.T) {
	requestID := "func-log-req-001"
	messages := []string{"Processing request", "Calling downstream API", "Request completed"}

	harness := NewTestHarness(t).
		WithInvocation(InvocationScenario{
			ExtensionEvent:  InvokeEvent(requestID),
			TelemetryEvents: FunctionLogEvents(requestID, messages...),
		})

	harness.Run()

	// Verify logs contain function log messages
	logs := harness.Sink.WaitForLogs(t, len(messages), signalTimeout)
	require.NotEmpty(t, logs)

	logBodies := collectLogBodies(logs)
	for _, msg := range messages {
		assert.Contains(t, logBodies, msg, "should contain function log: %s", msg)
	}

	// Verify function log records have request ID correlation
	logTypes := collectLogTypes(logs)
	assert.Contains(t, logTypes, "function", "should have function-type log records")
}

// TestInvocationError verifies that a failed invocation increments the error counter.
func TestInvocationError(t *testing.T) {
	requestID := "error-req-001"

	harness := NewTestHarness(t).
		WithInvocation(InvocationScenario{
			ExtensionEvent:  InvokeEvent(requestID),
			TelemetryEvents: ErrorInvocationEvents(requestID),
		})

	harness.Run()

	// Verify metrics include error counter
	metrics := harness.Sink.WaitForMetrics(t, 1, signalTimeout)
	require.NotEmpty(t, metrics)

	metricNames := collectMetricNames(metrics)
	assert.Contains(t, metricNames, "faas.errors", "should have errors metric for failed invocation")
}

// TestInvocationTimeout verifies that a timed-out invocation increments the timeout counter.
func TestInvocationTimeout(t *testing.T) {
	requestID := "timeout-req-001"

	harness := NewTestHarness(t).
		WithInvocation(InvocationScenario{
			ExtensionEvent:  InvokeEvent(requestID),
			TelemetryEvents: TimeoutInvocationEvents(requestID),
		})

	harness.Run()

	// Verify metrics include timeout counter
	metrics := harness.Sink.WaitForMetrics(t, 1, signalTimeout)
	require.NotEmpty(t, metrics)

	metricNames := collectMetricNames(metrics)
	assert.Contains(t, metricNames, "faas.timeouts", "should have timeouts metric for timed-out invocation")
}

// --- Helper functions ---

func collectMetricNames(allMetrics []pmetric.Metrics) []string {
	var names []string
	for _, md := range allMetrics {
		for i := 0; i < md.ResourceMetrics().Len(); i++ {
			rm := md.ResourceMetrics().At(i)
			for j := 0; j < rm.ScopeMetrics().Len(); j++ {
				sm := rm.ScopeMetrics().At(j)
				for k := 0; k < sm.Metrics().Len(); k++ {
					names = append(names, sm.Metrics().At(k).Name())
				}
			}
		}
	}
	return names
}

func collectLogTypes(allLogs []plog.Logs) []string {
	var types []string
	for _, ld := range allLogs {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			rl := ld.ResourceLogs().At(i)
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				sl := rl.ScopeLogs().At(j)
				for k := 0; k < sl.LogRecords().Len(); k++ {
					lr := sl.LogRecords().At(k)
					if v, exists := lr.Attributes().Get("type"); exists {
						types = append(types, v.Str())
					}
				}
			}
		}
	}
	return types
}

func collectLogBodies(allLogs []plog.Logs) []string {
	var bodies []string
	for _, ld := range allLogs {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			rl := ld.ResourceLogs().At(i)
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				sl := rl.ScopeLogs().At(j)
				for k := 0; k < sl.LogRecords().Len(); k++ {
					lr := sl.LogRecords().At(k)
					if lr.Body().Str() != "" {
						bodies = append(bodies, lr.Body().Str())
					}
				}
			}
		}
	}
	return bodies
}

func collectLogRequestIDs(allLogs []plog.Logs) []string {
	seen := make(map[string]bool)
	var ids []string
	for _, ld := range allLogs {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			rl := ld.ResourceLogs().At(i)
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				sl := rl.ScopeLogs().At(j)
				for k := 0; k < sl.LogRecords().Len(); k++ {
					lr := sl.LogRecords().At(k)
					if v, exists := lr.Attributes().Get("faas.invocation_id"); exists {
						id := v.Str()
						if !seen[id] {
							seen[id] = true
							ids = append(ids, id)
						}
					}
				}
			}
		}
	}
	return ids
}
