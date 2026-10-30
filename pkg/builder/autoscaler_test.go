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

package builder

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/debuggingsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	"sigs.k8s.io/cluster-autoscaler/pkg/expander"
	"sigs.k8s.io/cluster-autoscaler/pkg/loop"
	testutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func TestAutoscalerBuilderNoError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := testutils.GetTestContextWithCancel(t)

		options := config.AutoscalingOptions{
			CloudProviderName: "gce",
			EstimatorName:     estimator.BinpackingEstimatorName,
			ExpanderNames:     expander.LeastWasteExpanderName,
		}

		debuggingSnapshotter := debuggingsnapshot.NewDebuggingSnapshotter(false)
		kubeClient := fake.NewClientset()

		mgr, err := manager.New(&rest.Config{}, manager.Options{
			Metrics: metricsserver.Options{
				BindAddress: "0",
			},
			HealthProbeBindAddress: "0",
		})

		autoscaler, trigger, err := New(options).
			WithDebuggingSnapshotter(debuggingSnapshotter).
			WithManager(mgr).
			WithKubeClient(kubeClient).
			WithInformerFactory(informers.NewSharedInformerFactory(kubeClient, 0)).
			WithCloudProvider(test.NewCloudProvider(nil)).
			WithPodObserver(&loop.UnschedulablePodObserver{}).
			Build(ctx)

		assert.NoError(t, err)
		assert.NotNil(t, autoscaler)
		assert.NotNil(t, trigger)

		cancel()

		// Synctest drain: Background goroutines (like MetricAsyncRecorder) often use uninterruptible time.Sleep loops.
		// In a synctest bubble, these are "durable" sleeps. We must advance the virtual clock to allow these goroutines to wake up, observe the
		// closed context channel, and terminate gracefully.
		time.Sleep(1 * time.Second)
	})
}

func TestAutoscalerBuilderScaleDownStrategy(t *testing.T) {
	testCases := []struct {
		name              string
		scaleDownStrategy string
		wantErr           bool
	}{
		{
			name:              "strategy not configured",
			scaleDownStrategy: "",
		},
		{
			name:              "whitespaces only, strategy not configured",
			scaleDownStrategy: "      ",
		},
		{
			name:              "valid strategy",
			scaleDownStrategy: "Utilization=1",
		},
		{
			name:              "valid strategy trim whitespaces",
			scaleDownStrategy: "Utilization  =  1  ",
		},
		{
			name:              "float weight",
			scaleDownStrategy: "Utilization=4.2",
		},
		{
			name:              "unknown dimension",
			scaleDownStrategy: "Unknown=1",
			wantErr:           true,
		},
		{
			name:              "duplicate dimension",
			scaleDownStrategy: "Utilization=1,Utilization=2",
			wantErr:           true,
		},
		{
			name:              "missing weights",
			scaleDownStrategy: "Utilization",
			wantErr:           true,
		},
		{
			name:              "invalid negative weight",
			scaleDownStrategy: "Utilization=-42",
			wantErr:           true,
		},
		{
			name:              "invalid zero weight",
			scaleDownStrategy: "Utilization=0",
			wantErr:           true,
		},
		{
			name:              "invalid weight format",
			scaleDownStrategy: "Utilization=4.5.6",
			wantErr:           true,
		},
		{
			name:              "trailing comma produces empty part",
			scaleDownStrategy: "Utilization=42,",
			wantErr:           true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := testutils.GetTestContextWithCancel(t)

				options := config.AutoscalingOptions{
					CloudProviderName: "gce",
					EstimatorName:     estimator.BinpackingEstimatorName,
					ExpanderNames:     expander.LeastWasteExpanderName,
					ScaleDownStrategy: tc.scaleDownStrategy,
				}

				kubeClient := fake.NewClientset()
				mgr, err := manager.New(&rest.Config{}, manager.Options{
					Metrics: metricsserver.Options{
						BindAddress: "0",
					},
					HealthProbeBindAddress: "0",
				})
				assert.NoError(t, err)

				autoscaler, trigger, err := New(options).
					WithDebuggingSnapshotter(debuggingsnapshot.NewDebuggingSnapshotter(false)).
					WithManager(mgr).
					WithKubeClient(kubeClient).
					WithInformerFactory(informers.NewSharedInformerFactory(kubeClient, 0)).
					WithCloudProvider(test.NewCloudProvider(nil)).
					WithPodObserver(&loop.UnschedulablePodObserver{}).
					Build(ctx)

				if tc.wantErr {
					assert.Error(t, err)
					assert.Nil(t, autoscaler)
					assert.Nil(t, trigger)
				} else {
					assert.NoError(t, err)
					assert.NotNil(t, autoscaler)
					assert.NotNil(t, trigger)
				}
				cancel()
				time.Sleep(1 * time.Second)
			})
		})
	}
}
