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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// ProviderType identifies the underlying cluster environment for E2E tests.
type ProviderType string

const (
	// ProviderKWOK represents a simulated KWOK cluster on Kind.
	ProviderKWOK ProviderType = "kwok"
)

// TestConfig holds provider-specific parameters for E2E tests.
type TestConfig struct {
	Provider             ProviderType
	NodeGroup            string
	NodeGroupLabelKey    string
	NodeCPU              resource.Quantity
	NodeMemory           resource.Quantity
	Tolerations          []corev1.Toleration
	NodeReadyTimeout     time.Duration
	ScaleDownTimeout     time.Duration
	PodSchedulingTimeout time.Duration
	PodDeletionTimeout   time.Duration
	// NoScaleDownWindow is how long tests assert that the node count stays unchanged when
	// scale-down is expected to be blocked. It must be longer than the time CA needs to
	// remove an unneeded node in this environment.
	NoScaleDownWindow time.Duration
}

// KwokTestConfig returns the TestConfig for running E2E tests against the local Kind + KWOK cluster.
func KwokTestConfig() *TestConfig {
	return &TestConfig{
		Provider:          ProviderKWOK,
		NodeGroup:         "kind-worker",
		NodeGroupLabelKey: "kwok-nodegroup",
		NodeCPU:           resource.MustParse("12"),
		NodeMemory:        resource.MustParse("32Gi"),
		Tolerations: []corev1.Toleration{
			{
				Key:      "kwok-provider",
				Operator: corev1.TolerationOpExists,
				Effect:   corev1.TaintEffectNoSchedule,
			},
		},
		NodeReadyTimeout:     1 * time.Minute,
		ScaleDownTimeout:     1 * time.Minute,
		PodSchedulingTimeout: 1 * time.Minute,
		PodDeletionTimeout:   1 * time.Minute,
		// CA runs with --scale-down-unneeded-time=10s and the default 10s --scan-interval,
		// so an unneeded node is removed within ~30s. 45s leaves margin on top of that.
		NoScaleDownWindow: 45 * time.Second,
	}
}

// CalculateCPURequest returns a millicore CPU resource string corresponding to the given fraction
// of a single node's available CPU (NodeCPU).
func (c *TestConfig) CalculateCPURequest(fraction float64) string {
	milli := int64(float64(c.NodeCPU.MilliValue()) * fraction)
	if milli < 10 {
		milli = 10
	}
	return fmt.Sprintf("%dm", milli)
}

// CalculateMemoryRequest returns a MiB memory resource string corresponding to the given fraction
// of a single node's available memory (NodeMemory).
func (c *TestConfig) CalculateMemoryRequest(fraction float64) string {
	mib := int64((float64(c.NodeMemory.Value()) * fraction) / (1024 * 1024))
	if mib < 16 {
		mib = 16
	}
	return fmt.Sprintf("%dMi", mib)
}
