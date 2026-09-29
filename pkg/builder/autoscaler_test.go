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
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	schedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	schedulerconfiglatest "k8s.io/kubernetes/pkg/scheduler/apis/config/latest"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/debuggingsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	"sigs.k8s.io/cluster-autoscaler/pkg/expander"
	"sigs.k8s.io/cluster-autoscaler/pkg/loop"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/gpu"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func TestAutoscalerBuilderNoError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		customResource := apiv1.ResourceName("example.com/extender-managed-resource")
		originalGPUVendorResourceNames := append([]apiv1.ResourceName(nil), gpu.GPUVendorResourceNames...)
		t.Cleanup(func() {
			gpu.GPUVendorResourceNames = originalGPUVendorResourceNames
		})

		schedulerConfig, err := schedulerconfiglatest.Default()
		assert.NoError(t, err)
		schedulerConfig.Extenders = []schedulerconfig.Extender{{
			URLPrefix:  "http://127.0.0.1",
			FilterVerb: "filter",
			ManagedResources: []schedulerconfig.ExtenderManagedResource{{
				Name: string(customResource),
			}},
		}}

		options := config.AutoscalingOptions{
			CloudProviderName: "gce",
			EstimatorName:     estimator.BinpackingEstimatorName,
			ExpanderNames:     expander.LeastWasteExpanderName,
			SchedulerConfig:   schedulerConfig,
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
		assert.Contains(t, gpu.GPUVendorResourceNames, customResource)

		cancel()

		// Synctest drain: Background goroutines (like MetricAsyncRecorder) often use uninterruptible time.Sleep loops.
		// In a synctest bubble, these are "durable" sleeps. We must advance the virtual clock to allow these goroutines to wake up, observe the
		// closed context channel, and terminate gracefully.
		time.Sleep(1 * time.Second)
	})
}
