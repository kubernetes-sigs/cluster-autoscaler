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

package builder

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	provreqfake "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/client/clientset/versioned/fake"
	"k8s.io/client-go/informers"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	coreoptions "sigs.k8s.io/cluster-autoscaler/pkg/core/options"
	ca_processors "sigs.k8s.io/cluster-autoscaler/pkg/processors"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/pods"
)

func TestBuildProvisioningRequestRegistersResourceClaimTemplateInformerOnlyWithDRA(t *testing.T) {
	for _, tc := range []struct {
		name       string
		draEnabled bool
	}{
		{name: "DRA disabled", draEnabled: false},
		{name: "DRA enabled", draEnabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			kubeClient := kubefake.NewSimpleClientset()
			informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
			builder := &AutoscalerBuilder{
				informerFactory: informerFactory,
				prClient:        provreqfake.NewSimpleClientset(),
			}
			opts := &coreoptions.AutoscalerOptions{Processors: &ca_processors.AutoscalingProcessors{}}

			_, _, err := builder.buildProvisioningRequest(
				ctx,
				config.AutoscalingOptions{DynamicResourceAllocationEnabled: tc.draEnabled},
				opts,
				pods.NewCombinedPodListProcessor(nil),
			)
			require.NoError(t, err)

			informerFactory.Start(ctx.Done())
			for _, synced := range informerFactory.WaitForCacheSync(ctx.Done()) {
				require.True(t, synced)
			}

			rctInformerStarted := false
			for _, action := range kubeClient.Actions() {
				if action.GetResource().Resource == "resourceclaimtemplates" {
					rctInformerStarted = true
					break
				}
			}
			assert.Equal(t, tc.draEnabled, rctInformerStarted)
		})
	}
}
