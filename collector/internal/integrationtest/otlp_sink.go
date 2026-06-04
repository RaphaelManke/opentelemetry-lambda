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
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

// OTLPSink is an HTTP server that receives OTLP signals and stores them for assertions.
type OTLPSink struct {
	t        *testing.T
	server   *http.Server
	listener net.Listener
	endpoint string

	mu      sync.Mutex
	traces  []ptrace.Traces
	metrics []pmetric.Metrics
	logs    []plog.Logs

	traceCh  chan struct{}
	metricCh chan struct{}
	logCh    chan struct{}
}

// NewOTLPSink creates a new OTLP HTTP sink that captures traces, metrics, and logs.
func NewOTLPSink(t *testing.T) *OTLPSink {
	s := &OTLPSink{
		t:        t,
		traceCh:  make(chan struct{}, 100),
		metricCh: make(chan struct{}, 100),
		logCh:    make(chan struct{}, 100),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/traces", s.handleTraces)
	mux.HandleFunc("/v1/metrics", s.handleMetrics)
	mux.HandleFunc("/v1/logs", s.handleLogs)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	s.listener = l
	s.endpoint = l.Addr().String()
	s.server = &http.Server{Handler: mux}

	go func() {
		if err := s.server.Serve(l); err != http.ErrServerClosed {
			t.Logf("OTLP sink server error: %v", err)
		}
	}()

	t.Cleanup(func() {
		_ = s.server.Close()
	})

	return s
}

// Endpoint returns the host:port of the sink (without scheme).
func (s *OTLPSink) Endpoint() string {
	return s.endpoint
}

// HTTPEndpoint returns the full HTTP URL of the sink.
func (s *OTLPSink) HTTPEndpoint() string {
	return "http://" + s.endpoint
}

// GetTraces returns all received traces.
func (s *OTLPSink) GetTraces() []ptrace.Traces {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ptrace.Traces{}, s.traces...)
}

// GetMetrics returns all received metrics.
func (s *OTLPSink) GetMetrics() []pmetric.Metrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]pmetric.Metrics{}, s.metrics...)
}

// GetLogs returns all received logs.
func (s *OTLPSink) GetLogs() []plog.Logs {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]plog.Logs{}, s.logs...)
}

// TraceCount returns the total number of spans received.
func (s *OTLPSink) TraceCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, td := range s.traces {
		count += td.SpanCount()
	}
	return count
}

// MetricCount returns the total number of metric data points received.
func (s *OTLPSink) MetricCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, md := range s.metrics {
		count += md.MetricCount()
	}
	return count
}

// LogCount returns the total number of log records received.
func (s *OTLPSink) LogCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, ld := range s.logs {
		count += ld.LogRecordCount()
	}
	return count
}

// WaitForTraces waits until at least minSpans spans are received or timeout.
func (s *OTLPSink) WaitForTraces(t *testing.T, minSpans int, timeout time.Duration) []ptrace.Traces {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.TraceCount() >= minSpans {
			return s.GetTraces()
		}
		select {
		case <-s.traceCh:
		case <-time.After(50 * time.Millisecond):
		}
	}
	if s.TraceCount() < minSpans {
		t.Fatalf("timed out waiting for traces: got %d spans, want at least %d", s.TraceCount(), minSpans)
	}
	return s.GetTraces()
}

// WaitForMetrics waits until at least minMetrics metric data points are received or timeout.
func (s *OTLPSink) WaitForMetrics(t *testing.T, minMetrics int, timeout time.Duration) []pmetric.Metrics {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.MetricCount() >= minMetrics {
			return s.GetMetrics()
		}
		select {
		case <-s.metricCh:
		case <-time.After(50 * time.Millisecond):
		}
	}
	if s.MetricCount() < minMetrics {
		t.Fatalf("timed out waiting for metrics: got %d metrics, want at least %d", s.MetricCount(), minMetrics)
	}
	return s.GetMetrics()
}

// WaitForLogs waits until at least minLogs log records are received or timeout.
func (s *OTLPSink) WaitForLogs(t *testing.T, minLogs int, timeout time.Duration) []plog.Logs {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.LogCount() >= minLogs {
			return s.GetLogs()
		}
		select {
		case <-s.logCh:
		case <-time.After(50 * time.Millisecond):
		}
	}
	if s.LogCount() < minLogs {
		t.Fatalf("timed out waiting for logs: got %d records, want at least %d", s.LogCount(), minLogs)
	}
	return s.GetLogs()
}

func (s *OTLPSink) handleTraces(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	req := ptraceotlp.NewExportRequest()
	contentType := r.Header.Get("Content-Type")
	if contentType == "application/json" {
		err = req.UnmarshalJSON(body)
	} else {
		err = req.UnmarshalProto(body)
	}
	if err != nil {
		s.t.Logf("OTLP sink: failed to unmarshal traces (content-type=%s): %v", contentType, err)
		http.Error(w, "failed to unmarshal traces", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.traces = append(s.traces, req.Traces())
	s.mu.Unlock()

	select {
	case s.traceCh <- struct{}{}:
	default:
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	resp := ptraceotlp.NewExportResponse()
	respBytes, _ := resp.MarshalProto()
	_, _ = w.Write(respBytes)
}

func (s *OTLPSink) handleMetrics(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	req := pmetricotlp.NewExportRequest()
	contentType := r.Header.Get("Content-Type")
	if contentType == "application/json" {
		err = req.UnmarshalJSON(body)
	} else {
		err = req.UnmarshalProto(body)
	}
	if err != nil {
		s.t.Logf("OTLP sink: failed to unmarshal metrics (content-type=%s): %v", contentType, err)
		http.Error(w, "failed to unmarshal metrics", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.metrics = append(s.metrics, req.Metrics())
	s.mu.Unlock()

	select {
	case s.metricCh <- struct{}{}:
	default:
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	resp := pmetricotlp.NewExportResponse()
	respBytes, _ := resp.MarshalProto()
	_, _ = w.Write(respBytes)
}

func (s *OTLPSink) handleLogs(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	req := plogotlp.NewExportRequest()
	contentType := r.Header.Get("Content-Type")
	if contentType == "application/json" {
		err = req.UnmarshalJSON(body)
	} else {
		err = req.UnmarshalProto(body)
	}
	if err != nil {
		s.t.Logf("OTLP sink: failed to unmarshal logs (content-type=%s): %v", contentType, err)
		http.Error(w, "failed to unmarshal logs", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.logs = append(s.logs, req.Logs())
	s.mu.Unlock()

	select {
	case s.logCh <- struct{}{}:
	default:
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	resp := plogotlp.NewExportResponse()
	respBytes, _ := resp.MarshalProto()
	_, _ = w.Write(respBytes)
}
