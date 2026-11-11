/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kubernetes

import (
	"context"
	"maps"
	"sync"

	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

const (
	// maxLimitedErrorLogsPerLoop is the number of limited error logs emitted per
	// autoscaler loop. The rest are suppressed and only reported as a count by
	// SuppressErrorQuota.Flush.
	maxLimitedErrorLogsPerLoop = 30
	// suppressedErrorsSummaryMessage is the message of the error log emitted by
	// SuppressErrorQuota.Flush when some error logs were suppressed. The counts are
	// attached to it as key/value pairs: totalErrors, loggedErrors and suppressedErrors.
	suppressedErrorsSummaryMessage = "Some repeated error logs were suppressed since the end of the previous autoscaler loop"
)

// SuppressErrorQuota caps how many error logs with given messages and event reasons
// are emitted per CA loop, so that a burst of identical errors doesn't flood the logs.
// The maxLimitedErrorLogsPerLoop is shared by all loggers returned by Logger and is
// reset on every Flush, which is called once at the end of every CA loop.
type SuppressErrorQuota struct {
	messageReasons  map[string]string
	mu              sync.Mutex
	loggedCount     int
	suppressedCount int
}

// NewSuppressErrorQuota returns a SuppressErrorQuota which limits error logs whose
// message is a key of messageReasons and whose event reason is the corresponding value.
func NewSuppressErrorQuota(messageReasons map[string]string) *SuppressErrorQuota {
	return &SuppressErrorQuota{messageReasons: maps.Clone(messageReasons)}
}

// Logger returns a copy of base whose LogSink limits matching error logs against q's per-loop quota.
// If q is nil or base has no LogSink, base is returned unchanged.
func (q *SuppressErrorQuota) Logger(base klog.Logger) klog.Logger {
	if q == nil {
		return base
	}
	sink := base.GetSink()
	if sink == nil {
		return base
	}
	// New LogSink adds one stack frame between the logr.Logger and base LogSink.
	// Base LogSink must be told to skip this frame, so that log lines still show the
	// file:line of the original caller and not of suppressErrorQuotaLogSink.
	if withCallDepth, ok := sink.(logr.CallDepthLogSink); ok {
		sink = withCallDepth.WithCallDepth(1)
	}
	return base.WithSink(&suppressErrorQuotaLogSink{delegate: sink, quota: q})
}

// Flush logs a single summary line using logger from ctx if any error logs were
// suppressed since the previous Flush. Resets the quota for next CA loop run.
func (q *SuppressErrorQuota) Flush(ctx context.Context) {
	if q == nil {
		return
	}
	logger := klog.FromContext(ctx)
	q.mu.Lock()
	logged, suppressed := q.loggedCount, q.suppressedCount
	q.loggedCount, q.suppressedCount = 0, 0
	q.mu.Unlock()

	if suppressed > 0 {
		logger.Error(nil, suppressedErrorsSummaryMessage,
			"totalErrors", logged+suppressed,
			"loggedErrors", logged,
			"suppressedErrors", suppressed)
	}
}

// allow checks whether an error log fits in the current loop quota.
// The log is counted as logged or suppressed.
func (q *SuppressErrorQuota) allow() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.loggedCount < maxLimitedErrorLogsPerLoop {
		q.loggedCount++
		return true
	}
	q.suppressedCount++
	return false
}

// eventReason returns the Reason of the *v1.Event passed under the "event" key.
func eventReason(keysAndValues []any) string {
	for i := 0; i+1 < len(keysAndValues); i += 2 {
		if key, ok := keysAndValues[i].(string); ok && key == "event" {
			if value, ok := keysAndValues[i+1].(*v1.Event); ok {
				return value.Reason
			}
		}
	}
	return ""
}

// suppressErrorQuotaLogSink is the logr.LogSink of SuppressErrorQuota.Logger.
type suppressErrorQuotaLogSink struct {
	delegate logr.LogSink
	quota    *SuppressErrorQuota
}

var _ logr.CallDepthLogSink = &suppressErrorQuotaLogSink{}

// Init is a no-op because the logger is built with logr.Logger.WithSink (does not
// call Init) and delegate has already been initialized by its own logger.
func (s *suppressErrorQuotaLogSink) Init(logr.RuntimeInfo) {}

// Enabled forwards to delegate.
func (s *suppressErrorQuotaLogSink) Enabled(level int) bool {
	return s.delegate.Enabled(level)
}

// Info forwards to delegate.
func (s *suppressErrorQuotaLogSink) Info(level int, msg string, keysAndValues ...any) {
	s.delegate.Info(level, msg, keysAndValues...)
}

// Error forwards to delegate, unless:
// - msg is a key of the quota's messageReasons and
// - Error contains key "event" whose Event reason is the value for msg and
// - the quota in the current CA loop is used up.
func (s *suppressErrorQuotaLogSink) Error(err error, msg string, keysAndValues ...any) {
	if reason, ok := s.quota.messageReasons[msg]; ok && eventReason(keysAndValues) == reason && !s.quota.allow() {
		return
	}
	s.delegate.Error(err, msg, keysAndValues...)
}

// WithValues returns a suppressErrorQuotaLogSink sharing the same quota.
func (s *suppressErrorQuotaLogSink) WithValues(keysAndValues ...any) logr.LogSink {
	return &suppressErrorQuotaLogSink{delegate: s.delegate.WithValues(keysAndValues...), quota: s.quota}
}

// WithName returns a suppressErrorQuotaLogSink sharing the same quota.
func (s *suppressErrorQuotaLogSink) WithName(name string) logr.LogSink {
	return &suppressErrorQuotaLogSink{delegate: s.delegate.WithName(name), quota: s.quota}
}

// WithCallDepth returns a suppressErrorQuotaLogSink sharing the same quota, with the
// call depth of delegate adjusted if delegate supports it.
func (s *suppressErrorQuotaLogSink) WithCallDepth(depth int) logr.LogSink {
	delegate := s.delegate
	if withCallDepth, ok := delegate.(logr.CallDepthLogSink); ok {
		delegate = withCallDepth.WithCallDepth(depth)
	}
	return &suppressErrorQuotaLogSink{delegate: delegate, quota: s.quota}
}
