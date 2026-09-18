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

package common

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// ProviderType identifies the underlying cluster environment for E2E tests.
type ProviderType string

const (
	// ProviderKWOK represents a simulated KWOK cluster on Kind.
	ProviderKWOK ProviderType = "kwok"
	// ProviderGCE represents a real GCE/GKE cluster.
	ProviderGCE ProviderType = "gce"

	KwokTaintKey      = "kwok-provider"
	NodeGroupLabelKey = "kwok-nodegroup"
	DefaultNodeGroup  = "kind-worker"
)

// TestConfig holds provider-specific configuration for E2E tests.
type TestConfig struct {
	Provider             ProviderType
	NodeGroup            string
	NodeGroupLabelKey    string
	NodeCPU              int64 // Single node allocatable CPU in millicores
	Tolerations          []corev1.Toleration
	NodeReadyTimeout     time.Duration
	ScaleDownTimeout     time.Duration
	PodSchedulingTimeout time.Duration
	PodDeletionTimeout   time.Duration
}

var (
	defaultConfig     *TestConfig
	defaultConfigOnce sync.Once

	flagProvider          string
	flagNodeGroup         string
	flagNodeGroupLabelKey string
	flagNodeCPU           int64
)

func init() {
	flag.StringVar(&flagProvider, "e2e-provider", getEnvOrDefault("E2E_PROVIDER", string(ProviderKWOK)), "E2E provider environment (kwok or gce)")
	flag.StringVar(&flagNodeGroup, "e2e-nodegroup", getEnvOrDefault("E2E_NODEGROUP", ""), "Target node group name")
	flag.StringVar(&flagNodeGroupLabelKey, "e2e-nodegroup-label-key", getEnvOrDefault("E2E_NODEGROUP_LABEL_KEY", ""), "Node label key identifying the node group")
	// Defaults to 0 (auto-detect / provider default). For KWOK (which starts with 0 nodes),
	// 0 resolves to 12000m (12 cores) matching the simulated KWOK node template capacity.
	flag.Int64Var(&flagNodeCPU, "e2e-node-cpu", getEnvInt64OrDefault("E2E_NODE_CPU", 0), "Single node allocatable CPU in millicores (0 to auto-detect or use provider default)")
}

// InitTestConfig initializes the global TestConfig from flags and environment variables.
func InitTestConfig() *TestConfig {
	defaultConfigOnce.Do(func() {
		provider := ProviderType(flagProvider)
		switch provider {
		case ProviderGCE:
			// Use 15m timeouts when running against real GCE VMs: real node provisioning takes ~3-5m
			// and default GCE scale-down-unneeded-time is 10m (vs. 10s in simulated KWOK clusters).
			defaultConfig = NewGCEConfig(flagNodeGroup, flagNodeCPU, 15*time.Minute, 15*time.Minute)
			if flagNodeGroupLabelKey != "" {
				defaultConfig.NodeGroupLabelKey = flagNodeGroupLabelKey
			}
		default:
			ng := flagNodeGroup
			if ng == "" {
				ng = DefaultNodeGroup
			}
			labelKey := flagNodeGroupLabelKey
			if labelKey == "" {
				labelKey = NodeGroupLabelKey
			}
			cpu := flagNodeCPU
			if cpu <= 0 {
				// KWOK starts with 0 worker nodes, so live node inspection cannot run before scale-up.
				// Fall back to 12000m (12 cores) as defined in test/kwok/templates/node.yaml.
				cpu = 12000
			}
			defaultConfig = &TestConfig{
				Provider:          ProviderKWOK,
				NodeGroup:         ng,
				NodeGroupLabelKey: labelKey,
				NodeCPU:           cpu,
				Tolerations: []corev1.Toleration{
					{
						Key:      KwokTaintKey,
						Operator: corev1.TolerationOpExists,
						Effect:   corev1.TaintEffectNoSchedule,
					},
				},
				// Simulated KWOK nodes come up in seconds and use a 10s scale-down-unneeded-time.
				NodeReadyTimeout:     2 * time.Minute,
				ScaleDownTimeout:     4 * time.Minute,
				PodSchedulingTimeout: 2 * time.Minute,
				PodDeletionTimeout:   2 * time.Minute,
			}
		}
	})
	return defaultConfig
}

// GetTestConfig returns the active TestConfig, initializing defaults if needed.
func GetTestConfig() *TestConfig {
	if defaultConfig == nil {
		return InitTestConfig()
	}
	return defaultConfig
}

// NewGCEConfig creates a TestConfig tailored for GCE E2E test environments.
func NewGCEConfig(nodeGroup string, nodeCPU int64, scaleUpTimeout, scaleDownTimeout time.Duration) *TestConfig {
	labelKey := ""
	if nodeGroup != "" {
		labelKey = "cloud.google.com/gke-nodepool"
	}
	if nodeCPU <= 0 {
		nodeCPU = 3900 // Default allocatable millicores on e2-standard-4 / n1-standard-4
	}
	return &TestConfig{
		Provider:             ProviderGCE,
		NodeGroup:            nodeGroup,
		NodeGroupLabelKey:    labelKey,
		NodeCPU:              nodeCPU,
		Tolerations:          nil,
		NodeReadyTimeout:     scaleUpTimeout,
		ScaleDownTimeout:     scaleDownTimeout,
		PodSchedulingTimeout: scaleUpTimeout,
		PodDeletionTimeout:   5 * time.Minute,
	}
}

// CalculateCPURequest returns a millicore resource string corresponding to the given fraction of NodeCPU.
func (c *TestConfig) CalculateCPURequest(fraction float64) string {
	milli := int64(float64(c.NodeCPU) * fraction)
	if milli < 10 {
		milli = 10
	}
	return fmt.Sprintf("%dm", milli)
}

func getEnvOrDefault(key, def string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}

func getEnvInt64OrDefault(key string, def int64) int64 {
	if val := os.Getenv(key); val != "" {
		if parsed, err := strconv.ParseInt(val, 10, 64); err == nil {
			return parsed
		}
	}
	return def
}
