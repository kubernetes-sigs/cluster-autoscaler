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

func TestSuppressErrorQuota(t *testing.T) {
	suppressEventReason := "NotTriggerScaleUp"
	otherErrorMessage := "Some other error"
	otherEventReason := "NotSuppressedReason"
	for name, tc := range map[string]struct {
		nilQuota              bool
		noEventKey            bool
		message               string
		eventReason           string
		errorsPerLoop         int
		loops                 int
		wantLoggedPerLoop     int
		wantSummaries         int
		wantSuppressedPerLoop int
	}{
		"nil quota no limit": {
			nilQuota:          true,
			message:           droppedEventLogMessage,
			eventReason:       suppressEventReason,
			errorsPerLoop:     maxLimitedErrorLogsPerLoop + 5,
			loops:             1,
			wantLoggedPerLoop: maxLimitedErrorLogsPerLoop + 5,
		},
		"under quota": {
			message:           droppedEventLogMessage,
			eventReason:       suppressEventReason,
			errorsPerLoop:     maxLimitedErrorLogsPerLoop,
			loops:             1,
			wantLoggedPerLoop: maxLimitedErrorLogsPerLoop,
		},
		"over quota": {
			message:               droppedEventLogMessage,
			eventReason:           suppressEventReason,
			errorsPerLoop:         maxLimitedErrorLogsPerLoop + 5,
			loops:                 1,
			wantLoggedPerLoop:     maxLimitedErrorLogsPerLoop,
			wantSummaries:         1,
			wantSuppressedPerLoop: 5,
		},
		"other error messages not limited": {
			message:               otherErrorMessage,
			eventReason:           suppressEventReason,
			errorsPerLoop:         maxLimitedErrorLogsPerLoop + 5,
			loops:                 1,
			wantLoggedPerLoop:     maxLimitedErrorLogsPerLoop + 5,
			wantSuppressedPerLoop: 0,
			wantSummaries:         0,
		},
		"other event reasons not limited": {
			message:               droppedEventLogMessage,
			eventReason:           otherEventReason,
			errorsPerLoop:         maxLimitedErrorLogsPerLoop + 5,
			loops:                 2,
			wantLoggedPerLoop:     maxLimitedErrorLogsPerLoop + 5,
			wantSuppressedPerLoop: 0,
			wantSummaries:         0,
		},
		"errors without event key not limited": {
			noEventKey:        true,
			message:           droppedEventLogMessage,
			errorsPerLoop:     maxLimitedErrorLogsPerLoop + 5,
			loops:             1,
			wantLoggedPerLoop: maxLimitedErrorLogsPerLoop + 5,
		},
		"quota resets after flush": {
			message:               droppedEventLogMessage,
			eventReason:           suppressEventReason,
			errorsPerLoop:         maxLimitedErrorLogsPerLoop + 5,
			loops:                 2,
			wantLoggedPerLoop:     maxLimitedErrorLogsPerLoop,
			wantSuppressedPerLoop: 5,
			wantSummaries:         2,
		},
	} {
		t.Run(name, func(t *testing.T) {
			baseLogger := ktesting.NewLogger(t, ktesting.NewConfig(ktesting.BufferLogs(true)))
			ctx := klog.NewContext(context.Background(), baseLogger.WithValues("iterationId", "test"))
			suppressErrorQuota := NewSuppressErrorQuota(map[string]string{droppedEventLogMessage: suppressEventReason})
			if tc.nilQuota {
				suppressErrorQuota = nil
			}
			ev := &v1.Event{Reason: tc.eventReason}
			logger := suppressErrorQuota.Logger(baseLogger)
			for range tc.loops {
				for range tc.errorsPerLoop {
					if tc.noEventKey {
						logger.Error(nil, tc.message)
					} else {
						logger.Error(nil, tc.message, "event", ev)
					}
				}
				suppressErrorQuota.Flush(ctx)
			}
			var logged int
			var summaries []ktesting.LogEntry
			for _, entry := range baseLogger.GetSink().(ktesting.Underlier).GetBuffer().Data() {
				switch entry.Message {
				case tc.message:
					logged++
				case suppressedErrorsSummaryMessage:
					summaries = append(summaries, entry)
				}
			}
			assert.Equal(t, tc.wantLoggedPerLoop*tc.loops, logged)
			assert.Len(t, summaries, tc.wantSummaries)
			for _, summary := range summaries {
				assert.Equal(t, []any{"iterationId", "test"}, summary.WithKVList)
				assert.Equal(t, []any{
					"totalErrors", tc.wantLoggedPerLoop + tc.wantSuppressedPerLoop,
					"loggedErrors", tc.wantLoggedPerLoop,
					"suppressedErrors", tc.wantSuppressedPerLoop,
				}, summary.ParameterKVList)
			}
		})
	}
}
