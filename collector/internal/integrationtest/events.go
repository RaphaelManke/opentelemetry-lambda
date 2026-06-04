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
	"time"
)

// TelemetryEventBuilder helps construct Lambda Telemetry API events for tests.
type TelemetryEventBuilder struct {
	requestID string
	baseTime  time.Time
}

// NewTelemetryEventBuilder creates an event builder for the given request.
func NewTelemetryEventBuilder(requestID string) *TelemetryEventBuilder {
	return &TelemetryEventBuilder{
		requestID: requestID,
		baseTime:  time.Now().UTC(),
	}
}

// WithBaseTime sets the base time for generated events.
func (b *TelemetryEventBuilder) WithBaseTime(t time.Time) *TelemetryEventBuilder {
	b.baseTime = t
	return b
}

// PlatformInitStart generates a platform.initStart event.
func (b *TelemetryEventBuilder) PlatformInitStart() TelemetryEvent {
	return TelemetryEvent{
		Time: b.baseTime.Format(time.RFC3339Nano),
		Type: "platform.initStart",
		Record: map[string]any{
			"initializationType": "on-demand",
			"phase":              "init",
			"runtimeVersion":     "nodejs:20.v38",
			"runtimeVersionArn":  "arn:aws:lambda:us-east-1::runtime:nodejs20",
			"functionName":       "my-function",
			"functionVersion":    "$LATEST",
		},
	}
}

// PlatformInitRuntimeDone generates a platform.initRuntimeDone event.
func (b *TelemetryEventBuilder) PlatformInitRuntimeDone() TelemetryEvent {
	return TelemetryEvent{
		Time: b.baseTime.Add(100 * time.Millisecond).Format(time.RFC3339Nano),
		Type: "platform.initRuntimeDone",
		Record: map[string]any{
			"initializationType": "on-demand",
			"phase":              "init",
			"status":             "success",
		},
	}
}

// PlatformInitRuntimeDoneWithStatus generates a platform.initRuntimeDone with a custom status.
func (b *TelemetryEventBuilder) PlatformInitRuntimeDoneWithStatus(status string) TelemetryEvent {
	return TelemetryEvent{
		Time: b.baseTime.Add(100 * time.Millisecond).Format(time.RFC3339Nano),
		Type: "platform.initRuntimeDone",
		Record: map[string]any{
			"initializationType": "on-demand",
			"phase":              "init",
			"status":             status,
		},
	}
}

// PlatformInitReport generates a platform.initReport event.
func (b *TelemetryEventBuilder) PlatformInitReport(durationMs float64) TelemetryEvent {
	return TelemetryEvent{
		Time: b.baseTime.Add(105 * time.Millisecond).Format(time.RFC3339Nano),
		Type: "platform.initReport",
		Record: map[string]any{
			"initializationType": "on-demand",
			"phase":              "init",
			"status":             "success",
			"metrics": map[string]any{
				"durationMs": durationMs,
			},
		},
	}
}

// PlatformStart generates a platform.start event.
func (b *TelemetryEventBuilder) PlatformStart() TelemetryEvent {
	return TelemetryEvent{
		Time: b.baseTime.Add(110 * time.Millisecond).Format(time.RFC3339Nano),
		Type: "platform.start",
		Record: map[string]any{
			"requestId": b.requestID,
			"version":   "$LATEST",
		},
	}
}

// PlatformRuntimeDone generates a platform.runtimeDone event with success status.
func (b *TelemetryEventBuilder) PlatformRuntimeDone() TelemetryEvent {
	return b.PlatformRuntimeDoneWithStatus("success", 50.0)
}

// PlatformRuntimeDoneWithStatus generates a platform.runtimeDone event.
func (b *TelemetryEventBuilder) PlatformRuntimeDoneWithStatus(status string, durationMs float64) TelemetryEvent {
	return TelemetryEvent{
		Time: b.baseTime.Add(200 * time.Millisecond).Format(time.RFC3339Nano),
		Type: "platform.runtimeDone",
		Record: map[string]any{
			"requestId": b.requestID,
			"status":    status,
			"metrics": map[string]any{
				"durationMs":       durationMs,
				"producedBytes":    1024,
				"responseDuration": 5.0,
			},
		},
	}
}

// PlatformReport generates a platform.report event with metrics.
func (b *TelemetryEventBuilder) PlatformReport(durationMs, billedDurationMs, memorySizeMB, maxMemoryUsedMB float64) TelemetryEvent {
	return TelemetryEvent{
		Time: b.baseTime.Add(210 * time.Millisecond).Format(time.RFC3339Nano),
		Type: "platform.report",
		Record: map[string]any{
			"requestId": b.requestID,
			"status":    "success",
			"metrics": map[string]any{
				"durationMs":       durationMs,
				"billedDurationMs": billedDurationMs,
				"memorySizeMB":     memorySizeMB,
				"maxMemoryUsedMB":  maxMemoryUsedMB,
			},
		},
	}
}

// PlatformReportWithInitDuration generates a platform.report with init duration (cold start).
func (b *TelemetryEventBuilder) PlatformReportWithInitDuration(durationMs, billedDurationMs, memorySizeMB, maxMemoryUsedMB, initDurationMs float64) TelemetryEvent {
	return TelemetryEvent{
		Time: b.baseTime.Add(210 * time.Millisecond).Format(time.RFC3339Nano),
		Type: "platform.report",
		Record: map[string]any{
			"requestId": b.requestID,
			"status":    "success",
			"metrics": map[string]any{
				"durationMs":       durationMs,
				"billedDurationMs": billedDurationMs,
				"memorySizeMB":     memorySizeMB,
				"maxMemoryUsedMB":  maxMemoryUsedMB,
				"initDurationMs":   initDurationMs,
			},
		},
	}
}

// FunctionLog generates a function log event.
func (b *TelemetryEventBuilder) FunctionLog(level, message string) TelemetryEvent {
	return TelemetryEvent{
		Time: b.baseTime.Add(150 * time.Millisecond).Format(time.RFC3339Nano),
		Type: "function",
		Record: map[string]any{
			"requestId": b.requestID,
			"timestamp": b.baseTime.Add(150 * time.Millisecond).Format(time.RFC3339Nano),
			"level":     level,
			"message":   message,
		},
	}
}

// ColdStartEvents returns the full sequence of telemetry events for a cold start invocation.
func ColdStartEvents(requestID string) []TelemetryEvent {
	b := NewTelemetryEventBuilder(requestID)
	return []TelemetryEvent{
		b.PlatformInitStart(),
		b.PlatformInitRuntimeDone(),
		b.PlatformInitReport(100.0),
		b.PlatformStart(),
		b.PlatformRuntimeDone(),
		b.PlatformReportWithInitDuration(50.0, 100.0, 128.0, 64.0, 100.0),
	}
}

// WarmInvocationEvents returns telemetry events for a warm (non-cold-start) invocation.
func WarmInvocationEvents(requestID string) []TelemetryEvent {
	b := NewTelemetryEventBuilder(requestID)
	return []TelemetryEvent{
		b.PlatformStart(),
		b.PlatformRuntimeDone(),
		b.PlatformReport(30.0, 50.0, 128.0, 70.0),
	}
}

// ErrorInvocationEvents returns telemetry events for a failed invocation.
func ErrorInvocationEvents(requestID string) []TelemetryEvent {
	b := NewTelemetryEventBuilder(requestID)
	return []TelemetryEvent{
		b.PlatformStart(),
		b.PlatformRuntimeDoneWithStatus("failure", 45.0),
		b.PlatformReport(45.0, 100.0, 128.0, 80.0),
	}
}

// TimeoutInvocationEvents returns telemetry events for a timed-out invocation.
func TimeoutInvocationEvents(requestID string) []TelemetryEvent {
	b := NewTelemetryEventBuilder(requestID)
	return []TelemetryEvent{
		b.PlatformStart(),
		b.PlatformRuntimeDoneWithStatus("timeout", 3000.0),
		b.PlatformReport(3000.0, 3000.0, 128.0, 90.0),
	}
}

// FunctionLogEvents returns telemetry events that include function logs within an invocation.
func FunctionLogEvents(requestID string, messages ...string) []TelemetryEvent {
	b := NewTelemetryEventBuilder(requestID)
	events := []TelemetryEvent{
		b.PlatformStart(),
	}
	for _, msg := range messages {
		events = append(events, b.FunctionLog("INFO", msg))
	}
	events = append(events,
		b.PlatformRuntimeDone(),
		b.PlatformReport(50.0, 100.0, 128.0, 64.0),
	)
	return events
}
