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

package inmemory

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	fakecloudprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/test/integration"
	synctestutils "sigs.k8s.io/cluster-autoscaler/pkg/test/integration/synctest"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// TestStaticAutoscaler_NodeWithoutCSINode verifies that a registered Node without a backing CSINode object
// doesn't fail the autoscaler loop when CSINode-aware scheduling is enabled.
func TestStaticAutoscaler_NodeWithoutCSINode(t *testing.T) {
	options := integration.NewTestConfig().
		WithOverrides(integration.WithCSINodeAwareScheduling()).
		ResolveOptions()
	infra := integration.SetupInfrastructure(t)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer synctestutils.TearDown(cancel)

		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
		require.NoError(t, err)

		// A single ready Node with no CSINode object created for it.
		node := test.BuildTestNode("ng-node-0", 1000, 1000, test.IsReady(true))
		infra.Fakes.CloudProvider.AddNodeGroup("ng", fakecloudprovider.WithNode(node))

		synctestutils.MustRunOnceAfter(t, autoscaler, time.Second)
	})
}
