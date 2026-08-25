/*
Copyright 2025 The Kubernetes Authors.

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

package synctest

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	"sigs.k8s.io/cluster-autoscaler/pkg/core"
)

// RunOnceAfter advances the virtual clock by the specified duration and then
// executes a single Cluster Autoscaler cycle.
func RunOnceAfter(ctx context.Context, t *testing.T, autoscaler core.Autoscaler, d time.Duration) error {
	t.Helper()

	// Ensure any pending work is done before changing the time.
	synctest.Wait()

	time.Sleep(d)
	err := autoscaler.RunOnce(ctx, time.Now())

	// Let side-effects of the RunOnce finish.
	synctest.Wait()
	return err
}

// MustRunOnceAfter is a helper that calls RunOnceAfter and
// immediately fails the test if an error occurs.
// Use this for "happy path" simulation steps.
func MustRunOnceAfter(ctx context.Context, t *testing.T, autoscaler core.Autoscaler, d time.Duration) {
	t.Helper()
	err := RunOnceAfter(ctx, t, autoscaler, d)
	assert.NoError(t, err)
}

// TearDown is a helper to tear down the context and drain the synctest bubble.
func TearDown(cancel context.CancelFunc) {
	cancel()
	// Synctest drain: Background goroutines (like MetricAsyncRecorder) often use uninterruptible time.Sleep loops.
	// In a synctest bubble, these are "durable" sleeps. We must advance the virtual clock to allow these goroutines to wake up, observe the
	// closed context channel, and terminate gracefully.
	time.Sleep(1 * time.Minute)
	synctest.Wait()
}

func GetTestContext(t *testing.T) (context.Context, context.CancelFunc) {
	logger := ktesting.NewLogger(t, ktesting.DefaultConfig)
	logger = klog.LoggerWithValues(logger, "test", t.Name())
	ctx, cancel := context.WithCancel(t.Context())
	return klog.NewContext(ctx, logger), cancel
}
