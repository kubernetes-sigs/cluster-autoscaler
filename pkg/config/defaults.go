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

package config

import (
	"time"

	kube_rest "k8s.io/client-go/rest"
	gce_localssdsize "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/gce/localssdsize"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

// AutoscalingOptionModifier mutates an AutoscalingOptions instance.
type AutoscalingOptionModifier func(*AutoscalingOptions)

// NodeGroupAutoscalingOptionModifier mutates a NodeGroupAutoscalingOptions instance.
type NodeGroupAutoscalingOptionModifier func(*NodeGroupAutoscalingOptions)

// DefaultNodeGroupAutoscalingOptions returns canonical default per-node-group autoscaling options.
func DefaultNodeGroupAutoscalingOptions(modifiers ...NodeGroupAutoscalingOptionModifier) NodeGroupAutoscalingOptions {
	opts := NodeGroupAutoscalingOptions{
		ScaleDownUtilizationThreshold:    DefaultScaleDownUtilizationThreshold,
		ScaleDownGpuUtilizationThreshold: DefaultScaleDownGpuUtilizationThreshold,
		ScaleDownUnneededTime:            DefaultScaleDownUnneededTime,
		ScaleDownUnreadyTime:             DefaultScaleDownUnreadyTime,
		MaxNodeProvisionTime:             15 * time.Minute,
		MaxNodeStartupTime:               15 * time.Minute,
	}

	for _, modify := range modifiers {
		if modify != nil {
			modify(&opts)
		}
	}
	return opts
}

// DefaultAutoscalingOptions returns canonical default AutoscalingOptions with optional modifiers applied in order.
// CloudProviderName is intentionally left empty because its default depends on runtime provider registration.
func DefaultAutoscalingOptions(modifiers ...AutoscalingOptionModifier) AutoscalingOptions {
	opts := AutoscalingOptions{
		NodeGroupDefaults:  DefaultNodeGroupAutoscalingOptions(),
		NodeGroupSetRatios: NewDefaultNodeGroupDifferenceRatios(),
		KubeClientOpts: KubeClientOptions{
			APIContentType:  "application/vnd.kubernetes.protobuf",
			KubeClientBurst: kube_rest.DefaultBurst,
			KubeClientQPS:   kube_rest.DefaultQPS,
		},
		GCEOptions: GCEOptions{
			ConcurrentRefreshes:            1,
			MigInstancesMinRefreshWaitTime: 5 * time.Second,
			LocalSSDDiskSizeProvider:       gce_localssdsize.NewSimpleLocalSSDProvider(),
		},

		// Cluster & endpoint settings
		Address:              ":8085",
		ConfigNamespace:      "kube-system",
		UserAgent:            "cluster-autoscaler",
		WriteStatusConfigMap: true,
		StatusConfigMapName:  "cluster-autoscaler-status",
		ScanInterval:         DefaultScanInterval,
		FrequentLoopsEnabled: true,

		// Resource limits (MaxMemoryTotal is stored in bytes)
		MaxCoresTotal:          DefaultMaxClusterCores,
		MaxMemoryTotal:         DefaultMaxClusterMemory * units.GiB,
		GpuTotal:               []GpuLimits{},
		NodeGroups:             []string{},
		NodeGroupAutoDiscovery: []string{},

		// Health & cache timeouts
		MaxInactivityTime:         10 * time.Minute,
		MaxFailingTime:            15 * time.Minute,
		MaxStartupTime:            20 * time.Minute,
		MaxTotalUnreadyPercentage: 45,
		OkTotalUnreadyCount:       3,
		NodeInfoCacheExpireTime:   87600 * time.Hour,

		// Scale-up defaults
		ScaleUpFromZero:                 true,
		EstimatorName:                   DefaultEstimatorName,
		ExpanderNames:                   DefaultExpanderName,
		MaxBinpackingTime:               5 * time.Minute,
		MaxNodeGroupBinpackingDuration:  10 * time.Second,
		MaxNodesPerScaleUp:              1000,
		SalvoScaleUpBudget:              time.Minute,
		InitialNodeGroupBackoffDuration: 5 * time.Minute,
		MaxNodeGroupBackoffDuration:     30 * time.Minute,
		NodeGroupBackoffResetTimeout:    3 * time.Hour,
		ExpendablePodsPriorityCutoff:    -10,
		PendingPodsBatchingTimeout:      4 * time.Minute,
		PodInjectionLimit:               5000,

		// Scale-down defaults
		ScaleDownEnabled:                  true,
		ScaleDownUnreadyEnabled:           true,
		ScaleDownDelayAfterAdd:            10 * time.Minute,
		ScaleDownDelayAfterFailure:        DefaultScaleDownDelayAfterFailure,
		ScaleDownNonEmptyCandidatesCount:  30,
		ScaleDownCandidatesPoolRatio:      0.1,
		ScaleDownCandidatesPoolMinCount:   50,
		ScaleDownSimulationTimeout:        30 * time.Second,
		MaxScaleDownParallelism:           10,
		MaxDrainParallelism:               1,
		MaxConcurrentNodesTainting:        5,
		MaxBulkSoftTaintCount:             10,
		MaxBulkSoftTaintTime:              3 * time.Second,
		MaxPodEvictionTime:                2 * time.Minute,
		MaxGracefulTerminationSec:         DefaultMaxGracefulTerminationSec,
		NodeDeletionDelayTimeout:          2 * time.Minute,
		NodeDeleteDelayAfterTaint:         5 * time.Second,
		UnremovableNodeRecheckTimeout:     5 * time.Minute,
		CordonNodeBeforeTerminate:         true,
		DaemonSetEvictionForOccupiedNodes: true,
		SkipNodesWithSystemPods:           true,
		SkipNodesWithLocalStorage:         true,
		SkipNodesWithCustomControllerPods: true,
		BspDisruptionTimeout:              time.Hour,

		// ProvisioningRequest & CapacityBuffer defaults
		ProvisioningRequestInitialBackoffTime:        1 * time.Minute,
		ProvisioningRequestMaxBackoffTime:            10 * time.Minute,
		ProvisioningRequestMaxBackoffCacheSize:       1000,
		CheckCapacityProvisioningRequestMaxBatchSize: 10,
		CheckCapacityProvisioningRequestBatchTimebox: 10 * time.Second,
		CapacityBufferPodDryRunEnabled:               true,
		CapacityBufferReadyReplicasEnabled:           true,

		// Scheduling & feature toggles
		PredicateParallelism:             4,
		DynamicResourceAllocationEnabled: true,
		CSINodeAwareSchedulingEnabled:    true,
		StartupTaints:                    []string{},
		StartupTaintPrefixes:             []string{},
		StatusTaints:                     []string{},
		BalancingExtraIgnoredLabels:      []string{},
		BalancingLabels:                  []string{},
		BypassedSchedulers:               map[string]bool{},
		AllowedSchedulers:                map[string]bool{},
	}

	for _, modify := range modifiers {
		if modify != nil {
			modify(&opts)
		}
	}
	return opts
}
