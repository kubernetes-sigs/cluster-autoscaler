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

package integration

import (
	"time"

	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

// DefaultAutoscalingOptions provides the baseline configuration for all tests.
var DefaultAutoscalingOptions = config.DefaultAutoscalingOptions(func(o *config.AutoscalingOptions) {
	o.NodeGroupDefaults = config.DefaultNodeGroupAutoscalingOptions(func(ng *config.NodeGroupAutoscalingOptions) {
		ng.ScaleDownUnneededTime = time.Second
		ng.ScaleDownUnreadyTime = time.Minute
		ng.MaxNodeProvisionTime = 10 * time.Second
	})
	o.MaxTotalUnreadyPercentage = 1
	o.OkTotalUnreadyCount = 100000
	o.EnforceNodeGroupMinSize = true
	o.ScaleDownSimulationTimeout = 24 * time.Hour
	o.ScaleDownDelayAfterAdd = 0
	o.ScaleDownDelayAfterDelete = 0
	o.ScaleDownDelayAfterFailure = 0
	o.ScaleDownDelayTypeLocal = true
	o.NodeDeleteDelayAfterTaint = 0
	o.MaxNodesTotal = 10000
	o.MaxCoresTotal = 100000              // WARN: This setting isn't actually used by the fake CloudProvider.GetResourceLimiter(), there's a separate config there
	o.MaxMemoryTotal = 100000 * units.GiB // WARN: This setting isn't actually used by the fake CloudProvider.GetResourceLimiter(), there's a separate config there
	o.ClusterName = "cluster-test"
	o.MaxBinpackingTime = 10 * time.Second
	o.PredicateParallelism = 1
})

// TestConfig is the "blueprint" for a test. It defines the entire
// initial state of the world before the test runs.
type TestConfig struct {
	// BaseOptions can be set to DefaultAutoscalingOptions or a custom base.
	BaseOptions *config.AutoscalingOptions
	// OptionsOverrides allows adding options overrides.
	OptionsOverrides []config.AutoscalingOptionModifier
}

// NewTestConfig creates a test config pre-populated with DefaultAutoscalingOptions.
func NewTestConfig() *TestConfig {
	return &TestConfig{
		BaseOptions:      &DefaultAutoscalingOptions,
		OptionsOverrides: []config.AutoscalingOptionModifier{},
	}
}

// WithOverrides allows adding options overrides to the config.
func (c *TestConfig) WithOverrides(overrides ...config.AutoscalingOptionModifier) *TestConfig {
	c.OptionsOverrides = append(c.OptionsOverrides, overrides...)
	return c
}

// ResolveOptions merges the base options with all registered overrides.
func (c *TestConfig) ResolveOptions() config.AutoscalingOptions {
	var opts config.AutoscalingOptions
	if c.BaseOptions != nil {
		opts = *c.BaseOptions
	} else {
		opts = DefaultAutoscalingOptions
	}

	for _, override := range c.OptionsOverrides {
		override(&opts)
	}
	return opts
}

// WithCloudProviderName sets the cloud provider name.
func WithCloudProviderName(name string) config.AutoscalingOptionModifier {
	return func(o *config.AutoscalingOptions) {
		o.CloudProviderName = name
	}
}

// WithScaleDownUnneededTime sets the scale down unneeded time option.
func WithScaleDownUnneededTime(d time.Duration) config.AutoscalingOptionModifier {
	return func(o *config.AutoscalingOptions) {
		o.NodeGroupDefaults.ScaleDownUnneededTime = d
	}
}

// WithProvisioningRequestEnabled enables ProvisioningRequest support.
func WithProvisioningRequestEnabled() config.AutoscalingOptionModifier {
	return func(o *config.AutoscalingOptions) {
		o.ProvisioningRequestEnabled = true
	}
}

// WithCSINodeAwareScheduling enables CSI node aware scheduling (CSINode snapshotting, the CSI custom
// resources processor and the NodeVolumeLimits scheduler plugin backed by the snapshot).
func WithCSINodeAwareScheduling() config.AutoscalingOptionModifier {
	return func(o *config.AutoscalingOptions) {
		o.CSINodeAwareSchedulingEnabled = true
	}
}

// WithPodGroupSupport enables or disables PodGroup support (nodes hosting pods of gang-scheduled
// PodGroups are not scaled down).
func WithPodGroupSupport(enabled bool) config.AutoscalingOptionModifier {
	return func(o *config.AutoscalingOptions) {
		o.PodGroupSupportEnabled = enabled
	}
}

// WithMaxNodeStartupTime sets how long a registered but unready Node is still treated as starting up before it's considered unready.
func WithMaxNodeStartupTime(d time.Duration) config.AutoscalingOptionModifier {
	return func(o *config.AutoscalingOptions) {
		o.NodeGroupDefaults.MaxNodeStartupTime = d
	}
}

// WithMaxNodeProvisionTime sets how long a scale-up request stays active before it times out. A timed-out request
// backs the NodeGroup off and reverts the scale-up.
func WithMaxNodeProvisionTime(d time.Duration) config.AutoscalingOptionModifier {
	return func(o *config.AutoscalingOptions) {
		o.NodeGroupDefaults.MaxNodeProvisionTime = d
	}
}
