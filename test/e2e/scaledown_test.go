//go:build e2e

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

package e2e

import (
	"context"
	"testing"

	"sigs.k8s.io/cluster-autoscaler/pkg/e2e/common"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

func TestScaleDownUnneededNode(t *testing.T) {
	testCfg := common.GetTestConfig()

	feature := features.New("Scale Down Unneeded Node").
		Assess("scale down empty node after pod deletion", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			if err := common.RunScaleDownUnneededNode(ctx, client, cfg.Namespace(), testCfg); err != nil {
				t.Fatalf("scale down unneeded node test failed: %v", err)
			}
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err == nil {
				_ = common.CleanUpNodeGroup(ctx, client, testCfg.NodeGroup)
			}
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}
