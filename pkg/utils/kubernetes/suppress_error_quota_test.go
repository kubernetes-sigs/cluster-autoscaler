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
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
)

const (
	testQuotaLimit          = 3
	testSuppressEventReason = "NotTriggerScaleUp"
)

func TestSuppressErrorQuota(t *testing.T) {
	for name, tc := range map[string]struct {
		nilQuota       bool
		noEventKey     bool
		message        string
		eventReason    string
		errorLogs      int
		wantLogged     int
		wantSuppressed int
	}{
		"nil quota no limit": {
			nilQuota:    true,
			message:     droppedEventLogMessage,
			eventReason: testSuppressEventReason,
			errorLogs:   5,
			wantLogged:  5,
		},
		"under quota": {
			message:     droppedEventLogMessage,
			eventReason: testSuppressEventReason,
			errorLogs:   2,
			wantLogged:  2,
		},
		"over quota": {
			message:        droppedEventLogMessage,
			eventReason:    testSuppressEventReason,
			errorLogs:      5,
			wantLogged:     3,
			wantSuppressed: 2,
		},
		"other error messages not limited": {
			message:     "Some other error",
			eventReason: testSuppressEventReason,
			errorLogs:   5,
			wantLogged:  5,
		},
		"other event reasons not limited": {
			message:     droppedEventLogMessage,
			eventReason: "NotSuppressedReason",
			errorLogs:   5,
			wantLogged:  5,
		},
		"errors without event key not limited": {
			noEventKey: true,
			message:    droppedEventLogMessage,
			errorLogs:  5,
			wantLogged: 5,
		},
	} {
		t.Run(name, func(t *testing.T) {
			baseLogger := ktesting.NewLogger(t, ktesting.NewConfig(ktesting.BufferLogs(true)))
			ctx := klog.NewContext(context.Background(), baseLogger.WithValues("iterationId", "test"))
			quota := NewSuppressErrorQuota(droppedEventLogMessage, testSuppressEventReason, testQuotaLimit)
			if tc.nilQuota {
				quota = nil
			}
			logger := quota.Logger(baseLogger)
			ev := &v1.Event{Reason: tc.eventReason}
			for range tc.errorLogs {
				if tc.noEventKey {
					logger.Error(nil, tc.message)
				} else {
					logger.Error(nil, tc.message, "event", ev)
				}
			}
			quota.Flush(ctx)

			logged, summaries := loggedErrorsAndSummaries(baseLogger, tc.message)
			assert.Equal(t, tc.wantLogged, logged)
			if tc.wantSuppressed == 0 {
				assert.Empty(t, summaries)
				return
			}
			if assert.Len(t, summaries, 1) {
				assertSummary(t, summaries[0], tc.wantLogged, tc.wantSuppressed)
			}
		})
	}
}

func TestSuppressErrorQuotaFlushResetsQuota(t *testing.T) {
	baseLogger := ktesting.NewLogger(t, ktesting.NewConfig(ktesting.BufferLogs(true)))
	ctx := klog.NewContext(context.Background(), baseLogger.WithValues("iterationId", "test"))
	quota := NewSuppressErrorQuota(droppedEventLogMessage, testSuppressEventReason, testQuotaLimit)
	logger := quota.Logger(baseLogger)
	ev := &v1.Event{Reason: testSuppressEventReason}

	loops := 2
	errorLogs := 5 // per loop
	wantSuppressedPerLoop := 2

	for range loops {
		for range errorLogs {
			logger.Error(nil, droppedEventLogMessage, "event", ev)
		}
		quota.Flush(ctx)
	}
	logged, summaries := loggedErrorsAndSummaries(baseLogger, droppedEventLogMessage)
	assert.Equal(t, loops*testQuotaLimit, logged)
	if assert.Len(t, summaries, loops) {
		for _, summary := range summaries {
			assertSummary(t, summary, testQuotaLimit, wantSuppressedPerLoop)
		}
	}
}

func loggedErrorsAndSummaries(baseLogger klog.Logger, message string) (int, []ktesting.LogEntry) {
	var logged int
	var summaries []ktesting.LogEntry
	for _, entry := range baseLogger.GetSink().(ktesting.Underlier).GetBuffer().Data() {
		switch entry.Message {
		case message:
			logged++
		case suppressedErrorsSummaryMessage:
			summaries = append(summaries, entry)
		}
	}
	return logged, summaries
}

func assertSummary(t *testing.T, summary ktesting.LogEntry, wantLogged, wantSuppressed int) {
	t.Helper()
	assert.Equal(t, []any{"iterationId", "test"}, summary.WithKVList)
	assert.Equal(t, []any{
		"message", droppedEventLogMessage,
		"eventReason", testSuppressEventReason,
		"totalErrors", wantLogged + wantSuppressed,
		"loggedErrors", wantLogged,
		"suppressedErrors", wantSuppressed,
	}, summary.ParameterKVList)
}
