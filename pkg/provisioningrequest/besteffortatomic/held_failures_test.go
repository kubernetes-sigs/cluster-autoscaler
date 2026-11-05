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

package besteffortatomic

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	"sigs.k8s.io/cluster-autoscaler/pkg/observers/nodegroupchange"
	ca_processors "sigs.k8s.io/cluster-autoscaler/pkg/processors"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

// TestHeldFailuresReachMetrics checks that the provisioning class records the failures it doesn't
// report to the shared observers in metrics of its own.
func TestHeldFailuresReachMetrics(t *testing.T) {
	for _, tc := range []struct {
		name          string
		cloudProvider cloudprovider.CloudProvider
		wantMetrics   bool
	}{
		{name: "with a cloud provider", cloudProvider: testprovider.NewTestCloudProviderBuilder().Build(), wantMetrics: true},
		{name: "without a cloud provider"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			class := NewWithPodsInjector(nil, nil)
			class.scaleUpOrchestrator = initializeOnlyOrchestrator{}
			processors := &ca_processors.AutoscalingProcessors{ScaleStateNotifier: nodegroupchange.NewNodeGroupChangeObserversList()}
			class.Initialize(&ca_context.AutoscalingContext{CloudProvider: tc.cloudProvider}, processors, nil, nil, taints.TaintConfig{}, nil, nil)
			require.NotNil(t, class.heldFailures)
			if tc.wantMetrics {
				assert.IsType(t, &nodegroupchange.NodeGroupChangeMetricsProducer{}, class.heldFailures.metrics)
			} else {
				assert.Nil(t, class.heldFailures.metrics)
			}
		})
	}
}

// initializeOnlyOrchestrator is a scale-up orchestrator that can only be initialized.
type initializeOnlyOrchestrator struct {
	scaleup.Orchestrator
}

func (initializeOnlyOrchestrator) Initialize(*ca_context.AutoscalingContext, *ca_processors.AutoscalingProcessors, *clusterstate.ClusterStateRegistry, estimator.EstimatorBuilder, taints.TaintConfig, *resourcequotas.TrackerFactory) {
}

func TestHeldFailureObserver(t *testing.T) {
	ctx := t.Context()
	now := time.Now()
	provider := testprovider.NewTestCloudProviderBuilder().Build()
	provider.AddNodeGroup("a", 0, 10, 1)
	provider.AddNodeGroup("b", 0, 10, 1)
	a, b := provider.GetNodeGroup("a"), provider.GetNodeGroup("b")
	recorder := &recordingObserver{}
	metricsRecorder := &recordingObserver{}
	observer := newHeldFailureObserver(recorder, metricsRecorder)

	observer.RegisterFailedScaleUp(ctx, a, 8, cloudprovider.InstanceErrorInfo{}, now)
	assert.Equal(t, []recordedCall{{"FailedScaleUp", "a", 8}}, recorder.calls, "failures pass through while not holding")
	assert.Empty(t, metricsRecorder.calls, "forwarded failures reach the metrics through the shared observers")

	recorder.calls = nil
	observer.hold()
	observer.RegisterFailedScaleUp(ctx, b, 6, cloudprovider.InstanceErrorInfo{}, now)
	observer.RegisterFailedScaleUp(ctx, a, 8, cloudprovider.InstanceErrorInfo{}, now)
	observer.RegisterFailedScaleUp(ctx, b, 3, cloudprovider.InstanceErrorInfo{}, now)
	observer.RegisterFailedScaleUp(ctx, a, 4, cloudprovider.InstanceErrorInfo{}, now)
	observer.RegisterScaleUp(ctx, a, 2, now)
	observer.RegisterScaleDown(b, "node", now, now)
	observer.RegisterFailedScaleDown(b, "reason", now)
	forwarded := []recordedCall{{"ScaleUp", "a", 2}, {"ScaleDown", "b", 0}, {"FailedScaleDown", "b", 0}}
	assert.Equal(t, forwarded, recorder.calls, "only failed scale-ups are held")
	assert.Empty(t, metricsRecorder.calls)

	observer.release(ctx)
	assert.Equal(t, append(forwarded, recordedCall{"FailedScaleUp", "b", 3}), recorder.calls,
		"a accepted a smaller resize, so only b's last failure is reported")
	assert.Equal(t, []recordedCall{{"FailedScaleUp", "b", 6}, {"FailedScaleUp", "a", 8}, {"FailedScaleUp", "a", 4}}, metricsRecorder.calls,
		"every failed resize that isn't reported still counts in the metrics")

	recorder.calls = nil
	metricsRecorder.calls = nil
	observer.RegisterFailedScaleUp(ctx, a, 1, cloudprovider.InstanceErrorInfo{}, now)
	assert.Equal(t, []recordedCall{{"FailedScaleUp", "a", 1}}, recorder.calls, "release stops holding")
	assert.Empty(t, metricsRecorder.calls)

	observer = newHeldFailureObserver(recorder, nil)
	observer.hold()
	observer.RegisterFailedScaleUp(ctx, a, 8, cloudprovider.InstanceErrorInfo{}, now)
	observer.RegisterScaleUp(ctx, a, 4, now)
	assert.NotPanics(t, func() { observer.release(ctx) }, "metrics are optional")
}

type recordedCall struct {
	method    string
	nodeGroup string
	delta     int
}

type recordingObserver struct {
	calls []recordedCall
}

func (r *recordingObserver) RegisterScaleUp(_ context.Context, nodeGroup cloudprovider.NodeGroup, delta int, _ time.Time) {
	r.calls = append(r.calls, recordedCall{"ScaleUp", nodeGroup.Id(), delta})
}

func (r *recordingObserver) RegisterFailedScaleUp(_ context.Context, nodeGroup cloudprovider.NodeGroup, delta int, _ cloudprovider.InstanceErrorInfo, _ time.Time) {
	r.calls = append(r.calls, recordedCall{"FailedScaleUp", nodeGroup.Id(), delta})
}

func (r *recordingObserver) RegisterScaleDown(nodeGroup cloudprovider.NodeGroup, _ string, _ time.Time, _ time.Time) {
	r.calls = append(r.calls, recordedCall{"ScaleDown", nodeGroup.Id(), 0})
}

func (r *recordingObserver) RegisterFailedScaleDown(nodeGroup cloudprovider.NodeGroup, _ string, _ time.Time) {
	r.calls = append(r.calls, recordedCall{"FailedScaleDown", nodeGroup.Id(), 0})
}
