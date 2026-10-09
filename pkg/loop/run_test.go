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

package loop

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	"sigs.k8s.io/cluster-autoscaler/pkg/metrics"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
)

type fakeAutoscaler struct {
	err errors.AutoscalerError
}

func (f *fakeAutoscaler) RunOnce(_ context.Context, _ time.Time) errors.AutoscalerError {
	return f.err
}

func TestRunAutoscalerOnceLogsError(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       errors.AutoscalerError
		wantInLog []string
	}{
		{
			name: "no error",
		},
		{
			name:      "internal error",
			err:       errors.NewAutoscalerError(errors.InternalError, "failed to initialize ClusterSnapshot: boom"),
			wantInLog: []string{"Autoscaling iteration aborted with an error", "failed to initialize ClusterSnapshot: boom", "internalError", "iterationId"},
		},
		{
			name:      "transient error",
			err:       errors.NewAutoscalerError(errors.TransientError, "transient boom"),
			wantInLog: []string{"Autoscaling iteration aborted with an error", "transient boom", "transientError", "iterationId"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger := ktesting.NewLogger(t, ktesting.NewConfig(ktesting.BufferLogs(true)))
			ctx := klog.NewContext(context.Background(), logger)
			healthCheck := metrics.NewHealthCheck(time.Minute, time.Minute, time.Minute)

			RunAutoscalerOnce(ctx, &fakeAutoscaler{err: tc.err}, healthCheck, time.Now(), 1)

			logs := logger.GetSink().(ktesting.Underlier).GetBuffer().String()
			if tc.err == nil {
				if strings.Contains(logs, "Autoscaling iteration aborted") {
					t.Errorf("unexpected error log for a successful iteration:\n%s", logs)
				}
				return
			}
			for _, want := range tc.wantInLog {
				if !strings.Contains(logs, want) {
					t.Errorf("expected logs to contain %q, got:\n%s", want, logs)
				}
			}
		})
	}
}
