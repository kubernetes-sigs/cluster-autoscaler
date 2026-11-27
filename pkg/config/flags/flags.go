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

package flags

import (
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/pflag"

	cloudBuilder "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/builder"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	"sigs.k8s.io/cluster-autoscaler/pkg/expander"
	scheduler_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/scheduler"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"

	"k8s.io/klog/v2"
	kubelet_config "k8s.io/kubernetes/pkg/kubelet/apis/config"
	scheduler_config "k8s.io/kubernetes/pkg/scheduler/apis/config"
)

const (
	// MaxGracefulTerminationSecDefault is the default max graceful termination value in seconds.
	//
	// Deprecated: use config.DefaultMaxGracefulTerminationSec instead.
	// Scheduled for removal in 1.42.
	MaxGracefulTerminationSecDefault = config.DefaultMaxGracefulTerminationSec
)

// AutoscalingFlags holds the command line flags for the autoscaler and responsible
// for parsing and building the AutoscalingOptions struct from commandline flags.
// TODO(viktoriaas): Remove AutoscalingFlags in a follow-up to introduction of DefaultAutoscalingOptions which will
// introduce generic pflag.Value wrapper and helper to parse composite flags directly into AutoscalingOptions.
type AutoscalingFlags struct {
	// Flags that require post-processing to be converted to the AutoscalingOptions struct
	schedulerConfigFile string
	coresTotal          string
	memoryTotal         string
	drainPriorityConfig string
	gpuTotal            []string
	bypassedSchedulers  []string
	allowedSchedulers   []string
	kubeClientQPS       float64
	schedulerVerbosity  int
	// The AutoscalingOptions struct that will be populated by the flags
	// as a result of parsing the flags
	o           config.AutoscalingOptions
	initialized bool
}

// NewAutoscalingFlags creates an AutoscalingFlags instance seeded with canonical defaults.
func NewAutoscalingFlags() *AutoscalingFlags {
	return &AutoscalingFlags{}
}

func (p *AutoscalingFlags) initDefaults() {
	if p.initialized {
		return
	}
	p.o = config.DefaultAutoscalingOptions()
	p.o.CloudProviderName = cloudBuilder.DefaultCloudProvider()

	p.coresTotal = minMaxFlagString(p.o.MinCoresTotal, p.o.MaxCoresTotal)
	p.memoryTotal = minMaxFlagString(p.o.MinMemoryTotal/units.GiB, p.o.MaxMemoryTotal/units.GiB)
	p.kubeClientQPS = float64(p.o.KubeClientOpts.KubeClientQPS)
	p.schedulerVerbosity = -1
	p.gpuTotal = []string{}
	p.bypassedSchedulers = []string{}
	p.allowedSchedulers = []string{}
	p.initialized = true
}

// AddFlags injects autoscaling CLI flags for a given flagset
func (p *AutoscalingFlags) AddFlags(fs *pflag.FlagSet) {
	p.initDefaults()

	// Set of flags directly bound to internal AutoscalingOptions struct
	fs.StringVar(&p.o.ClusterName, "cluster-name", p.o.ClusterName, "Autoscaled cluster name, if available")
	fs.StringVar(&p.o.Address, "address", p.o.Address, "The address to expose prometheus metrics.")
	fs.StringVar(&p.o.KubeClientOpts.Master, "kubernetes", p.o.KubeClientOpts.Master, "Kubernetes master location. Leave blank for default")
	fs.StringVar(&p.o.KubeClientOpts.KubeConfigPath, "kubeconfig", p.o.KubeClientOpts.KubeConfigPath, "Path to kubeconfig file with authorization and master location information.")
	fs.StringVar(&p.o.KubeClientOpts.APIContentType, "kube-api-content-type", p.o.KubeClientOpts.APIContentType, "Content type of requests sent to apiserver.")
	fs.IntVar(&p.o.KubeClientOpts.KubeClientBurst, "kube-client-burst", p.o.KubeClientOpts.KubeClientBurst, "Burst value for kubernetes client.")
	fs.Float64Var(&p.kubeClientQPS, "kube-client-qps", p.kubeClientQPS, "QPS value for kubernetes client.")
	fs.StringVar(&p.o.CloudConfig, "cloud-config", p.o.CloudConfig, "The path to the cloud provider configuration file.  Empty string for no configuration file.")
	fs.StringVar(&p.o.CloudProviderName, "cloud-provider", p.o.CloudProviderName, "Cloud provider type. Available values: ["+strings.Join(cloudBuilder.AvailableCloudProviders(), ",")+"]")
	fs.StringVar(&p.o.ConfigNamespace, "namespace", p.o.ConfigNamespace, "Namespace in which cluster-autoscaler run.")
	fs.BoolVar(&p.o.EnforceNodeGroupMinSize, "enforce-node-group-min-size", p.o.EnforceNodeGroupMinSize, "Should CA scale up the node group to the configured min size if needed.")
	fs.BoolVar(&p.o.ScaleDownUnreadyEnabled, "scale-down-unready-enabled", p.o.ScaleDownUnreadyEnabled, "Should CA scale down unready nodes of the cluster")
	fs.DurationVar(&p.o.ScaleDownDelayAfterAdd, "scale-down-delay-after-add", p.o.ScaleDownDelayAfterAdd, "How long after scale up that scale down evaluation resumes")
	fs.BoolVar(&p.o.ScaleDownDelayTypeLocal, "scale-down-delay-type-local", p.o.ScaleDownDelayTypeLocal, "Should --scale-down-delay-after-* flags be applied locally per nodegroup or globally across all nodegroups")
	fs.DurationVar(&p.o.ScaleDownDelayAfterDelete, "scale-down-delay-after-delete", p.o.ScaleDownDelayAfterDelete, "How long after node deletion that scale down evaluation resumes (default 0s)")
	fs.DurationVar(&p.o.ScaleDownDelayAfterFailure, "scale-down-delay-after-failure", p.o.ScaleDownDelayAfterFailure, "How long after scale down failure that scale down evaluation resumes")
	fs.DurationVar(&p.o.NodeGroupDefaults.ScaleDownUnneededTime, "scale-down-unneeded-time", p.o.NodeGroupDefaults.ScaleDownUnneededTime, "How long a node should be unneeded before it is eligible for scale down")
	fs.DurationVar(&p.o.NodeGroupDefaults.ScaleDownUnreadyTime, "scale-down-unready-time", p.o.NodeGroupDefaults.ScaleDownUnreadyTime, "How long an unready node should be unneeded before it is eligible for scale down")
	fs.Float64Var(&p.o.NodeGroupDefaults.ScaleDownUtilizationThreshold, "scale-down-utilization-threshold", p.o.NodeGroupDefaults.ScaleDownUtilizationThreshold, "The maximum value between the sum of cpu requests and sum of memory requests of all pods running on the node divided by node's corresponding allocatable resource, below which a node can be considered for scale down")
	fs.Float64Var(&p.o.NodeGroupDefaults.ScaleDownGpuUtilizationThreshold, "scale-down-gpu-utilization-threshold", p.o.NodeGroupDefaults.ScaleDownGpuUtilizationThreshold,
		"Maximum across resources representing the selected GPU family of the sum of pod requests divided by the node's allocatable resource, at or below which a node can be considered for scale down. "+
			"NVIDIA whole-GPU, MIG, and time-sliced resources are evaluated together. "+
			"CPU and memory utilization are ignored for GPU nodes.")
	fs.IntVar(&p.o.ScaleDownNonEmptyCandidatesCount, "scale-down-non-empty-candidates-count", p.o.ScaleDownNonEmptyCandidatesCount,
		"Maximum number of non empty nodes considered in one iteration as candidates for scale down with drain."+
			"Lower value means better CA responsiveness but possible slower scale down latency."+
			"Higher value can affect CA performance with big clusters (hundreds of nodes)."+
			"Set to non positive value to turn this heuristic off - CA will not limit the number of nodes it considers.")
	fs.Float64Var(&p.o.ScaleDownCandidatesPoolRatio, "scale-down-candidates-pool-ratio", p.o.ScaleDownCandidatesPoolRatio,
		"A ratio of nodes that are considered as additional non empty candidates for"+
			"scale down when some candidates from previous iteration are no longer valid."+
			"Lower value means better CA responsiveness but possible slower scale down latency."+
			"Higher value can affect CA performance with big clusters (hundreds of nodes)."+
			"Set to 1.0 to turn this heuristics off - CA will take all nodes as additional candidates.")
	fs.IntVar(&p.o.ScaleDownCandidatesPoolMinCount, "scale-down-candidates-pool-min-count", p.o.ScaleDownCandidatesPoolMinCount,
		"Minimum number of nodes that are considered as additional non empty candidates"+
			"for scale down when some candidates from previous iteration are no longer valid."+
			"When calculating the pool size for additional candidates we take"+
			"max(#nodes * scale-down-candidates-pool-ratio, scale-down-candidates-pool-min-count).")
	fs.DurationVar(&p.o.NodeDeletionDelayTimeout, "node-deletion-delay-timeout", p.o.NodeDeletionDelayTimeout, "Maximum time CA waits for removing delay-deletion.cluster-autoscaler.kubernetes.io/ annotations before deleting the node.")
	fs.DurationVar(&p.o.NodeDeletionBatcherInterval, "node-deletion-batcher-interval", p.o.NodeDeletionBatcherInterval, "How long CA ScaleDown gather nodes to delete them in batch. (default 0s)")
	fs.DurationVar(&p.o.ScanInterval, "scan-interval", p.o.ScanInterval, "How often cluster is reevaluated for scale up or down")
	fs.IntVar(&p.o.MaxNodesTotal, "max-nodes-total", p.o.MaxNodesTotal, "Maximum number of nodes in all node groups. Cluster autoscaler will not grow the cluster beyond this number.")
	fs.IntVar(&p.o.MaxBulkSoftTaintCount, "max-bulk-soft-taint-count", p.o.MaxBulkSoftTaintCount, "Maximum number of nodes that can be tainted/untainted PreferNoSchedule at the same time. Set to 0 to turn off such tainting.")
	fs.DurationVar(&p.o.MaxBulkSoftTaintTime, "max-bulk-soft-taint-time", p.o.MaxBulkSoftTaintTime, "Maximum duration of tainting/untainting nodes as PreferNoSchedule at the same time.")
	fs.Float64Var(&p.o.MaxTotalUnreadyPercentage, "max-total-unready-percentage", p.o.MaxTotalUnreadyPercentage, "Maximum percentage of unready nodes in the cluster.  After this is exceeded, CA halts operations")
	fs.IntVar(&p.o.OkTotalUnreadyCount, "ok-total-unready-count", p.o.OkTotalUnreadyCount, "Number of allowed unready nodes, irrespective of max-total-unready-percentage")
	fs.BoolVar(&p.o.ScaleUpFromZero, "scale-up-from-zero", p.o.ScaleUpFromZero, "Should CA scale up when there are 0 ready nodes.")
	fs.BoolVar(&p.o.ParallelScaleUp, "parallel-scale-up", p.o.ParallelScaleUp, "Whether to allow parallel node groups scale up. Experimental: may not work on some cloud providers, enable at your own risk.")
	fs.DurationVar(&p.o.NodeGroupDefaults.MaxNodeProvisionTime, "max-node-provision-time", p.o.NodeGroupDefaults.MaxNodeProvisionTime, "The default maximum time CA waits for node to be provisioned - the value can be overridden per node group")
	fs.DurationVar(&p.o.NodeGroupDefaults.MaxNodeStartupTime, "max-node-startup-time", p.o.NodeGroupDefaults.MaxNodeStartupTime, "The maximum time from the moment the node is registered to the time the node is ready - the value can be overridden per node group")
	fs.DurationVar(&p.o.MaxPodEvictionTime, "max-pod-eviction-time", p.o.MaxPodEvictionTime, "Maximum time CA tries to evict a pod before giving up")
	fs.StringVar(&p.o.EstimatorName, "estimator", p.o.EstimatorName, "Type of resource estimator to be used in scale up. Available values: ["+strings.Join(estimator.AvailableEstimators, ",")+"]")
	fs.StringVar(&p.o.ExpanderNames, "expander", p.o.ExpanderNames, "Type of node group expander to be used in scale up. Available values: ["+strings.Join(expander.AvailableExpanders, ",")+"]. Specifying multiple values separated by commas will call the expanders in succession until there is only one option remaining. Ties still existing after this process are broken randomly.")
	fs.StringVar(&p.o.GRPCExpanderCert, "grpc-expander-cert", p.o.GRPCExpanderCert, "Path to cert used by gRPC server over TLS")
	fs.StringVar(&p.o.GRPCExpanderURL, "grpc-expander-url", p.o.GRPCExpanderURL, "URL to reach gRPC expander server.")
	fs.BoolVar(&p.o.NodeGroupDefaults.IgnoreDaemonSetsUtilization, "ignore-daemonsets-utilization", p.o.NodeGroupDefaults.IgnoreDaemonSetsUtilization, "Should CA ignore DaemonSet pods when calculating resource utilization for scaling down")
	fs.BoolVar(&p.o.IgnoreMirrorPodsUtilization, "ignore-mirror-pods-utilization", p.o.IgnoreMirrorPodsUtilization, "Should CA ignore Mirror pods when calculating resource utilization for scaling down")
	fs.BoolVar(&p.o.WriteStatusConfigMap, "write-status-configmap", p.o.WriteStatusConfigMap, "Should CA write status information to a configmap")
	fs.StringVar(&p.o.StatusConfigMapName, "status-config-map-name", p.o.StatusConfigMapName, "Status configmap name")
	fs.DurationVar(&p.o.MaxInactivityTime, "max-inactivity", p.o.MaxInactivityTime, "Maximum time from last recorded autoscaler activity before automatic restart")
	fs.DurationVar(&p.o.MaxBinpackingTime, "max-binpacking-time", p.o.MaxBinpackingTime, "Maximum time spend on binpacking for a single scale-up. If binpacking is limited by this, scale-up will continue with the already calculated scale-up options.")
	fs.DurationVar(&p.o.MaxFailingTime, "max-failing-time", p.o.MaxFailingTime, "Maximum time from last recorded successful autoscaler run before automatic restart")
	fs.DurationVar(&p.o.MaxStartupTime, "max-startup-time", p.o.MaxStartupTime, "Maximum time until first recorded successful autoscaler run before automatic restart")
	fs.BoolVar(&p.o.BalanceSimilarNodeGroups, "balance-similar-node-groups", p.o.BalanceSimilarNodeGroups, "Detect similar node groups and balance the number of nodes between them")
	fs.DurationVar(&p.o.UnremovableNodeRecheckTimeout, "unremovable-node-recheck-timeout", p.o.UnremovableNodeRecheckTimeout, "The timeout before we check again a node that couldn't be removed before")
	fs.IntVar(&p.o.ExpendablePodsPriorityCutoff, "expendable-pods-priority-cutoff", p.o.ExpendablePodsPriorityCutoff, "Pods with priority below cutoff will be expendable. They can be killed without any consideration during scale down and they don't cause scale up. Pods with null priority (PodPriority disabled) are non expendable.")
	fs.BoolVar(&p.o.Regional, "regional", p.o.Regional, "Cluster is regional.")
	fs.DurationVar(&p.o.NewPodScaleUpDelay, "new-pod-scale-up-delay", p.o.NewPodScaleUpDelay, "Pods less than this old will not be considered for scale-up. Can be increased for individual pods through annotation 'cluster-autoscaler.kubernetes.io/pod-scale-up-delay'. (default 0s)")
	fs.BoolVar(&p.o.ScaleFromUnschedulable, "scale-from-unschedulable", p.o.ScaleFromUnschedulable, "Specifies that the CA should ignore a node's .spec.unschedulable field in node templates when considering to scale a node group.")
	fs.BoolVar(&p.o.EnableProfiling, "profiling", p.o.EnableProfiling, "Is debug/pprof endpoint enabled")
	fs.BoolVar(&p.o.ClusterAPICloudConfigAuthoritative, "clusterapi-cloud-config-authoritative", p.o.ClusterAPICloudConfigAuthoritative, "Treat the cloud-config flag authoritatively (do not fallback to using kubeconfig flag). ClusterAPI only")
	fs.BoolVar(&p.o.CordonNodeBeforeTerminate, "cordon-node-before-terminating", p.o.CordonNodeBeforeTerminate, "Should CA cordon nodes before terminating during downscale process")
	fs.BoolVar(&p.o.DaemonSetEvictionForEmptyNodes, "daemonset-eviction-for-empty-nodes", p.o.DaemonSetEvictionForEmptyNodes, "DaemonSet pods will be gracefully terminated from empty nodes")
	fs.BoolVar(&p.o.DaemonSetEvictionForOccupiedNodes, "daemonset-eviction-for-occupied-nodes", p.o.DaemonSetEvictionForOccupiedNodes, "DaemonSet pods will be gracefully terminated from non-empty nodes")
	fs.StringVar(&p.o.UserAgent, "user-agent", p.o.UserAgent, "User agent used for HTTP calls.")
	fs.BoolVar(&p.o.EmitPerNodeGroupMetrics, "emit-per-nodegroup-metrics", p.o.EmitPerNodeGroupMetrics, "If true, emit per node group metrics.")
	fs.BoolVar(&p.o.DebuggingSnapshotEnabled, "debugging-snapshot-enabled", p.o.DebuggingSnapshotEnabled, "Whether the debugging snapshot of cluster autoscaler feature is enabled")
	fs.DurationVar(&p.o.NodeInfoCacheExpireTime, "node-info-cache-expire-time", p.o.NodeInfoCacheExpireTime, "Node Info cache expire time for each item. Default value is 10 years.")
	fs.DurationVar(&p.o.InitialNodeGroupBackoffDuration, "initial-node-group-backoff-duration", p.o.InitialNodeGroupBackoffDuration, "initialNodeGroupBackoffDuration is the duration of first backoff after a new node failed to start.")
	fs.DurationVar(&p.o.MaxNodeGroupBackoffDuration, "max-node-group-backoff-duration", p.o.MaxNodeGroupBackoffDuration, "maxNodeGroupBackoffDuration is the maximum backoff duration for a NodeGroup after new nodes failed to start.")
	fs.DurationVar(&p.o.NodeGroupBackoffResetTimeout, "node-group-backoff-reset-timeout", p.o.NodeGroupBackoffResetTimeout, "nodeGroupBackoffResetTimeout is the time after last failed scale-up when the backoff duration is reset.")
	fs.IntVar(&p.o.MaxScaleDownParallelism, "max-scale-down-parallelism", p.o.MaxScaleDownParallelism, "Maximum number of nodes (both empty and needing drain) that can be deleted in parallel.")
	fs.IntVar(&p.o.MaxDrainParallelism, "max-drain-parallelism", p.o.MaxDrainParallelism, "Maximum number of nodes needing drain, that can be drained and deleted in parallel.")
	fs.IntVar(&p.o.MaxConcurrentNodesTainting, "max-concurrent-nodes-tainting", p.o.MaxConcurrentNodesTainting, "Maximum number of nodes that can be tainted/untainted as ToBeDeleted at the same time.")
	fs.BoolVar(&p.o.RecordDuplicatedEvents, "record-duplicated-events", p.o.RecordDuplicatedEvents, "enable duplication of similar events within a 5 minute window.")
	fs.IntVar(&p.o.MaxNodesPerScaleUp, "max-nodes-per-scaleup", p.o.MaxNodesPerScaleUp, "Max nodes added in a single scale-up. This is intended strictly for optimizing CA algorithm latency and not a tool to rate-limit scale-up throughput.")
	fs.DurationVar(&p.o.MaxNodeGroupBinpackingDuration, "max-nodegroup-binpacking-duration", p.o.MaxNodeGroupBinpackingDuration, "Maximum time that will be spent in binpacking simulation for each NodeGroup.")
	fs.BoolVar(&p.o.FastpathBinpackingEnabled, "fastpath-binpacking-enabled", p.o.FastpathBinpackingEnabled, "Whether to use fastpath binpacking algorithm to optimize scale-ups.")
	fs.BoolVar(&p.o.SkipNodesWithSystemPods, "skip-nodes-with-system-pods", p.o.SkipNodesWithSystemPods, "If true cluster autoscaler will wait for --blocking-system-pod-distruption-timeout before deleting nodes with pods from kube-system (except for DaemonSet or mirror pods)")
	fs.BoolVar(&p.o.SkipNodesWithLocalStorage, "skip-nodes-with-local-storage", p.o.SkipNodesWithLocalStorage, "If true cluster autoscaler will never delete nodes with pods with local storage, e.g. EmptyDir or HostPath")
	fs.BoolVar(&p.o.SkipNodesWithCustomControllerPods, "skip-nodes-with-custom-controller-pods", p.o.SkipNodesWithCustomControllerPods, "If true cluster autoscaler will never delete nodes with pods owned by custom controllers")
	fs.IntVar(&p.o.MinReplicaCount, "min-replica-count", p.o.MinReplicaCount, "Minimum number or replicas that a replica set or replication controller should have to allow their pods deletion in scale down")
	fs.DurationVar(&p.o.BspDisruptionTimeout, "blocking-system-pod-distruption-timeout", p.o.BspDisruptionTimeout, "The timeout after which CA will evict non-pdb-assigned blocking system pods, applicable only when --skip-nodes-with-system-pods is set to true")
	fs.DurationVar(&p.o.NodeDeleteDelayAfterTaint, "node-delete-delay-after-taint", p.o.NodeDeleteDelayAfterTaint, "How long to wait before deleting a node after tainting it")
	fs.DurationVar(&p.o.ScaleDownSimulationTimeout, "scale-down-simulation-timeout", p.o.ScaleDownSimulationTimeout, "How long should we run scale down simulation.")
	fs.Float64Var(&p.o.NodeGroupSetRatios.MaxCapacityMemoryDifferenceRatio, "memory-difference-ratio", p.o.NodeGroupSetRatios.MaxCapacityMemoryDifferenceRatio, "Maximum difference in memory capacity between two similar node groups to be considered for balancing. Value is a ratio of the smaller node group's memory capacity.")
	fs.Float64Var(&p.o.NodeGroupSetRatios.MaxFreeDifferenceRatio, "max-free-difference-ratio", p.o.NodeGroupSetRatios.MaxFreeDifferenceRatio, "Maximum difference in free resources between two similar node groups to be considered for balancing. Value is a ratio of the smaller node group's free resource.")
	fs.Float64Var(&p.o.NodeGroupSetRatios.MaxAllocatableDifferenceRatio, "max-allocatable-difference-ratio", p.o.NodeGroupSetRatios.MaxAllocatableDifferenceRatio, "Maximum difference in allocatable resources between two similar node groups to be considered for balancing. Value is a ratio of the smaller node group's allocatable resource.")
	fs.BoolVar(&p.o.ForceDaemonSets, "force-ds", p.o.ForceDaemonSets, "Blocks scale-up of node groups too small for all suitable Daemon Sets pods.")
	fs.BoolVar(&p.o.DynamicNodeDeleteDelayAfterTaintEnabled, "dynamic-node-delete-delay-after-taint-enabled", p.o.DynamicNodeDeleteDelayAfterTaintEnabled, "Enables dynamic adjustment of NodeDeleteDelayAfterTaint based of the latency between CA and api-server")
	fs.BoolVar(&p.o.ProvisioningRequestEnabled, "enable-provisioning-requests", p.o.ProvisioningRequestEnabled, "Whether the clusterautoscaler will be handling the ProvisioningRequest CRs.")
	fs.DurationVar(&p.o.ProvisioningRequestInitialBackoffTime, "provisioning-request-initial-backoff-time", p.o.ProvisioningRequestInitialBackoffTime, "Initial backoff time for ProvisioningRequest retry after failed ScaleUp.")
	fs.DurationVar(&p.o.ProvisioningRequestMaxBackoffTime, "provisioning-request-max-backoff-time", p.o.ProvisioningRequestMaxBackoffTime, "Max backoff time for ProvisioningRequest retry after failed ScaleUp.")
	fs.IntVar(&p.o.ProvisioningRequestMaxBackoffCacheSize, "provisioning-request-max-backoff-cache-size", p.o.ProvisioningRequestMaxBackoffCacheSize, "Max size for ProvisioningRequest cache size used for retry backoff mechanism.")
	fs.BoolVar(&p.o.FrequentLoopsEnabled, "frequent-loops-enabled", p.o.FrequentLoopsEnabled, "Whether clusterautoscaler triggers new iterations more frequently when it's needed")
	fs.BoolVar(&p.o.AsyncNodeGroupsEnabled, "async-node-groups", p.o.AsyncNodeGroupsEnabled, "Whether clusterautoscaler creates and deletes node groups asynchronously. Experimental: requires cloud provider supporting async node group operations, enable at your own risk.")
	fs.BoolVar(&p.o.ProactiveScaleupEnabled, "enable-proactive-scaleup", p.o.ProactiveScaleupEnabled, "Whether to enable/disable proactive scale-ups, defaults to false")
	fs.BoolVar(&p.o.SalvoScaleUp, "salvo-scale-up", p.o.SalvoScaleUp, "Whether to allow multiple scale-ups in a single CA loop.")
	fs.DurationVar(&p.o.SalvoScaleUpBudget, "salvo-scale-up-budget", p.o.SalvoScaleUpBudget, "Maximum time CA spends on subsequent scale ups in a single CA loop. Requires salvo-scale-up flag to be enabled.")
	fs.BoolVar(&p.o.ScaleUpSimulationForSkippedNodeGroupsEnabled, "scaleup-simulation-for-skipped-node-groups-enabled", p.o.ScaleUpSimulationForSkippedNodeGroupsEnabled, "Whether to enable the scale up simulation for skipped node groups.")
	fs.BoolVar(&p.o.GCEOptions.BulkMigInstancesListingEnabled, "bulk-mig-instances-listing-enabled", p.o.GCEOptions.BulkMigInstancesListingEnabled, "Fetch GCE mig instances in bulk instead of per mig")
	fs.IntVar(&p.o.GCEOptions.ConcurrentRefreshes, "gce-concurrent-refreshes", p.o.GCEOptions.ConcurrentRefreshes, "Maximum number of concurrent refreshes per cloud object type.")
	fs.DurationVar(&p.o.GCEOptions.MigInstancesMinRefreshWaitTime, "gce-mig-instances-min-refresh-wait-time", p.o.GCEOptions.MigInstancesMinRefreshWaitTime, "The minimum time which needs to pass before GCE MIG instances from a given MIG can be refreshed.")
	fs.BoolVar(&p.o.AWSUseStaticInstanceList, "aws-use-static-instance-list", p.o.AWSUseStaticInstanceList, "Should CA fetch instance types in runtime or use a static list. AWS only")
	fs.StringArrayVar(&p.o.BalancingExtraIgnoredLabels, "balancing-ignore-label", p.o.BalancingExtraIgnoredLabels, "Specifies a label to ignore in addition to the basic and cloud-provider set of labels when comparing if two node groups are similar")
	fs.StringArrayVar(&p.o.BalancingLabels, "balancing-label", p.o.BalancingLabels, "Specifies a label to use for comparing if two node groups are similar, rather than the built in heuristics. Setting this flag disables all other comparison logic, and cannot be combined with --balancing-ignore-label.")
	fs.StringArrayVar(&p.o.StartupTaintPrefixes, "startup-taint-prefix", p.o.StartupTaintPrefixes, "Specifies a taint key prefix. Any taint whose key starts with this prefix will be treated as a startup taint (in addition to the built-in prefixes). Can be used multiple times.")
	fs.IntVar(&p.o.PodInjectionLimit, "pod-injection-limit", p.o.PodInjectionLimit, "Limits total number of pods while injecting fake pods. If unschedulable pods already exceeds the limit, pod injection is disabled but pods are not truncated.")
	fs.BoolVar(&p.o.CheckCapacityBatchProcessing, "check-capacity-batch-processing", p.o.CheckCapacityBatchProcessing, "Whether to enable batch processing for check capacity requests.")
	fs.IntVar(&p.o.CheckCapacityProvisioningRequestMaxBatchSize, "check-capacity-provisioning-request-max-batch-size", p.o.CheckCapacityProvisioningRequestMaxBatchSize, "Maximum number of provisioning requests to process in a single batch.")
	fs.DurationVar(&p.o.CheckCapacityProvisioningRequestBatchTimebox, "check-capacity-provisioning-request-batch-timebox", p.o.CheckCapacityProvisioningRequestBatchTimebox, "Maximum time to process a batch of provisioning requests.")
	fs.BoolVar(&p.o.ForceDeleteLongUnregisteredNodes, "force-delete-unregistered-nodes", p.o.ForceDeleteLongUnregisteredNodes, "Whether to enable force deletion of long unregistered nodes, regardless of the min size of the node group the belong to.")
	fs.BoolVar(&p.o.ForceDeleteFailedNodes, "force-delete-failed-nodes", p.o.ForceDeleteFailedNodes, "Whether to enable force deletion of failed nodes, regardless of the min size of the node group the belong to.")
	fs.BoolVar(&p.o.CSINodeAwareSchedulingEnabled, "enable-csi-node-aware-scheduling", p.o.CSINodeAwareSchedulingEnabled, "Whether logic for handling CSINode objects is enabled.")
	fs.BoolVar(&p.o.PodGroupSupportEnabled, "enable-pod-group-support", p.o.PodGroupSupportEnabled, "Whether logic for handling scheduling.k8s.io PodGroups is enabled: nodes hosting pods of gang-scheduled PodGroups (gang policy with minCount > 1) are not scaled down. Requires the GenericWorkload feature gate and the scheduling.k8s.io/v1beta1 API to be enabled on the API server.")
	fs.IntVar(&p.o.PredicateParallelism, "predicate-parallelism", p.o.PredicateParallelism, "Maximum parallelism of scheduler predicate checking.")
	fs.StringVar(&p.o.CheckCapacityProcessorInstance, "check-capacity-processor-instance", p.o.CheckCapacityProcessorInstance, "Name of the processor instance. Only ProvisioningRequests that define this name in their parameters with the key \"processorInstance\" will be processed by this CA instance. It only refers to check capacity ProvisioningRequests, but if not empty, best-effort atomic ProvisioningRequests processing is disabled in this instance. Not recommended: Until CA 1.35, ProvisioningRequests with this name as prefix in their class will be also processed.")
	fs.DurationVar(&p.o.NodeDeletionCandidateTTL, "node-deletion-candidate-ttl", p.o.NodeDeletionCandidateTTL, "Maximum time a node can be marked as removable before the marking becomes stale. This sets the TTL of Cluster-Autoscaler's state if the Cluste-Autoscaler deployment becomes inactive (default 0s)")
	fs.BoolVar(&p.o.CapacitybufferControllerEnabled, "capacity-buffer-controller-enabled", p.o.CapacitybufferControllerEnabled, "Whether to enable the default controller for capacity buffers or not")
	fs.BoolVar(&p.o.CapacitybufferPodInjectionEnabled, "capacity-buffer-pod-injection-enabled", p.o.CapacitybufferPodInjectionEnabled, "Whether to enable pod list processor that processes ready capacity buffers and injects fake pods accordingly")
	fs.BoolVar(&p.o.CapacityBufferPodDryRunEnabled, "capacity-buffer-pod-dry-run-enabled", p.o.CapacityBufferPodDryRunEnabled, "Whether to use server dry run to build managed pod templates for capacity buffers. That ensures that the buffers' fake pods will more reliably resemble real pods by going through the pod defaulting, mutating and validating webhooks. No-op if --capacity-buffer-controller-enabled is false. Note: requires \"create\" permission on pods to call server dry run. No real pods will be created.")
	fs.BoolVar(&p.o.CapacityBufferReadyReplicasEnabled, "capacity-buffer-ready-replicas-enabled", p.o.CapacityBufferReadyReplicasEnabled, "Whether to reconcile capacity buffers' readyReplicas status field. No-op if --capacity-buffer-pod-injection-enabled is false.")
	fs.BoolVar(&p.o.NodeRemovalLatencyTrackingEnabled, "node-removal-latency-tracking-enabled", p.o.NodeRemovalLatencyTrackingEnabled, "Whether to track latency from when an unneeded node is eligible for scale down until it is removed or needed again.")
	fs.BoolVar(&p.o.MaxNodeSkipEvalTimeTrackerEnabled, "max-node-skip-eval-time-tracker-enabled", p.o.MaxNodeSkipEvalTimeTrackerEnabled, "Whether to enable the tracking of the maximum time of node being skipped during ScaleDown")
	fs.BoolVar(&p.o.CapacityQuotasEnabled, "capacity-quotas-enabled", p.o.CapacityQuotasEnabled, "Whether to enable CapacityQuota CRD support.")
	fs.DurationVar(&p.o.PendingPodsBatchingTimeout, "pending-pods-batching-timeout", p.o.PendingPodsBatchingTimeout, "The maximum time spends when running scheduling simulations when preparing the batch of pods passed to scale up logic. This timeout is used in core filter-out pod list processor.")
	fs.BoolVar(&p.o.GracefulDegradationEnabled, "enable-graceful-degradation", p.o.GracefulDegradationEnabled, "Enables graceful degradation for unschedulable pods. When enabled, scheduling simulations during filter out pod list processor will be stopped after a timeout(specified by scheduling-simulation-timeout) and CA will proceed with a sub set of pods.")
	fs.BoolVar(&p.o.InterPodAffinityHostnameFastPath, "enable-inter-pod-affinity-hostname-fast-path", p.o.InterPodAffinityHostnameFastPath, "Enable the InterPodAffinityHostnameFastPath scheduler feature gate.")
	fs.BoolVar(&p.o.ScaleDownEnabled, "scale-down-enabled", p.o.ScaleDownEnabled, "Should CA scale down the cluster")

	// Deprecated flags
	fs.BoolVar(&p.o.DynamicResourceAllocationEnabled, "enable-dynamic-resource-allocation", p.o.DynamicResourceAllocationEnabled, "Handle DRA (Dynamic Resource Allocation) objects, locked to true.")

	// Properties specific to the cloud provider, but kept for the sake of POC
	fs.StringArrayVar(&p.o.NodeGroups, "nodes", p.o.NodeGroups, "sets min,max size and other configuration data for a node group in a format accepted by cloud provider. Can be used multiple times. Format: <min>:<max>:<other...>")
	fs.StringArrayVar(&p.o.NodeGroupAutoDiscovery, "node-group-auto-discovery", p.o.NodeGroupAutoDiscovery, "One or more definition(s) of node group auto-discovery. "+
		"A definition is expressed `<name of discoverer>:[<key>[=<value>]]`. "+
		"The `aws`, `gce`, and `azure` cloud providers are currently supported. AWS matches by ASG tags, e.g. `asg:tag=tagKey,anotherTagKey`. "+
		"GCE matches by IG name prefix, and requires you to specify min and max nodes per IG, e.g. `mig:namePrefix=pfx,min=0,max=10` "+
		"Azure matches by VMSS tags, similar to AWS. And you can optionally specify a default min and max size, e.g. `label:tag=tagKey,anotherTagKey=bar,min=0,max=600`. "+
		"Can be used multiple times.")

	// Flags which require post-processing
	fs.StringVar(&p.schedulerConfigFile, config.SchedulerConfigFileFlag, p.schedulerConfigFile, "scheduler-config allows changing configuration of in-tree scheduler plugins acting on PreFilter and Filter extension points")
	fs.StringVar(&p.coresTotal, "cores-total", p.coresTotal, "Minimum and maximum number of cores in cluster, in the format <min>:<max>. Cluster autoscaler will not scale the cluster beyond these numbers.")
	fs.StringVar(&p.memoryTotal, "memory-total", p.memoryTotal, "Minimum and maximum number of gigabytes of memory in cluster, in the format <min>:<max>. Cluster autoscaler will not scale the cluster beyond these numbers.")
	fs.IntVar(&p.o.MaxGracefulTerminationSec, "max-graceful-termination-sec", p.o.MaxGracefulTerminationSec, "Maximum number of seconds CA waits for pod termination when trying to scale down a node. This flag is mutually exclusion with drain-priority-config flag which allows more configuration options.")
	fs.StringArrayVar(&p.gpuTotal, "gpu-total", p.gpuTotal, "Minimum and maximum number of different GPUs in cluster, in the format <gpu_type>:<min>:<max>. Cluster autoscaler will not scale the cluster beyond these numbers. Can be passed multiple times. CURRENTLY THIS FLAG ONLY WORKS ON GKE.")
	fs.StringArrayVar(&p.o.StartupTaints, "startup-taint", p.o.StartupTaints, "Specifies a taint to ignore in node templates when considering to scale a node group")
	fs.StringArrayVar(&p.o.StatusTaints, "status-taint", p.o.StatusTaints, "Specifies a taint to ignore in node templates when considering to scale a node group but nodes will not be treated as unready")
	fs.StringSliceVar(&p.bypassedSchedulers, "bypassed-scheduler-names", p.bypassedSchedulers, "Names of schedulers to bypass. If set to non-empty value, CA will not wait for pods to reach a certain age before triggering a scale-up.")
	fs.StringSliceVar(&p.allowedSchedulers, "allowed-scheduler-names", p.allowedSchedulers, "If set to non-empty value, CA will proceed only with pods targeting schedulers in the list, from the list of unschedulable and scheduler unprocessed pods")
	fs.StringVar(&p.drainPriorityConfig, "drain-priority-config", p.drainPriorityConfig,
		"List of ',' separated pairs (priority:terminationGracePeriodSeconds) of integers separated by ':' enables priority evictor. Priority evictor groups pods into priority groups based on pod priority and evict pods in the ascending order of group priorities"+
			"--max-graceful-termination-sec flag should not be set when this flag is set. Not setting this flag will use unordered evictor by default."+
			"Priority evictor reuses the concepts of drain logic in kubelet(https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/2712-pod-priority-based-graceful-node-shutdown#migration-from-the-node-graceful-shutdown-feature)."+
			"Eg. flag usage:  '10000:20,1000:100,0:60'")
	fs.IntVar(&p.schedulerVerbosity, "scheduler-verbosity", p.schedulerVerbosity, "Specifies the verbosity threshold for logs produced by the scheduler. Should not be greater than -v flag value. If it's set to -1 the scheduler log verbosity will be equal to -v flag value.")
}

// Options builds AutoscalingOptions from the flags and returns them by value
func (p *AutoscalingFlags) Options() (config.AutoscalingOptions, error) {
	p.initDefaults()

	minCoresTotal, maxCoresTotal, err := parseMinMaxFlag(p.coresTotal)
	if err != nil {
		return config.AutoscalingOptions{}, fmt.Errorf("Failed to parse flags: %w", err)
	}

	minMemoryTotal, maxMemoryTotal, err := parseMinMaxFlag(p.memoryTotal)
	if err != nil {
		return config.AutoscalingOptions{}, fmt.Errorf("Failed to parse flags: %w", err)
	}
	// Convert memory limits to bytes.
	minMemoryTotal = minMemoryTotal * units.GiB
	maxMemoryTotal = maxMemoryTotal * units.GiB

	parsedGpuTotal, err := parseMultipleGpuLimits(p.gpuTotal)
	if err != nil {
		return config.AutoscalingOptions{}, fmt.Errorf("Failed to parse flags: %w", err)
	}

	var parsedSchedConfig *scheduler_config.KubeSchedulerConfiguration
	// if scheduler config flag was set by the user
	if p.schedulerConfigFile != "" {
		parsedSchedConfig, err = scheduler_util.ConfigFromPath(p.schedulerConfigFile)
		if err != nil {
			return config.AutoscalingOptions{}, fmt.Errorf("Failed to get scheduler config: %w", err)
		}
	}

	if p.drainPriorityConfig != "" && p.o.MaxGracefulTerminationSec != config.DefaultMaxGracefulTerminationSec {
		return config.AutoscalingOptions{}, fmt.Errorf("Invalid configuration, could not use --drain-priority-config together with --max-graceful-termination-sec")
	}

	var drainPriorityConfigMap []kubelet_config.ShutdownGracePeriodByPodPriority
	if p.drainPriorityConfig != "" {
		drainPriorityConfigMap = parseShutdownGracePeriodsAndPriorities(p.drainPriorityConfig)
		if len(drainPriorityConfigMap) == 0 {
			return config.AutoscalingOptions{}, fmt.Errorf("Invalid configuration, parsing --drain-priority-config")
		}
	}

	if p.o.PredicateParallelism < 1 {
		return config.AutoscalingOptions{}, fmt.Errorf("Invalid value for --predicate-parallelism flag: %d", p.o.PredicateParallelism)
	}

	if p.o.MaxConcurrentNodesTainting < 1 {
		return config.AutoscalingOptions{}, fmt.Errorf("Invalid value for --max-concurrent-nodes-tainting flag: %d", p.o.MaxConcurrentNodesTainting)
	}

	if p.o.DynamicResourceAllocationEnabled == false {
		return config.AutoscalingOptions{}, fmt.Errorf("--enable-dynamic-resource-allocation flag must be true: %t", p.o.DynamicResourceAllocationEnabled)
	}

	allowedSchedulers, err := parseAllowedSchedulers(p.allowedSchedulers, p.bypassedSchedulers)
	if err != nil {
		return config.AutoscalingOptions{}, err
	}

	maxHealthCheckTimeout := max(p.o.MaxInactivityTime, p.o.MaxFailingTime, p.o.MaxStartupTime)
	if p.o.MaxStartupTime < maxHealthCheckTimeout {
		klog.Warningf("--max-startup-time needs to be at least as high as --max-failing-time and --max-inactivity, overriding it to the highest value out of them: %v", maxHealthCheckTimeout)
		p.o.MaxStartupTime = maxHealthCheckTimeout
	}

	var verbosity int
	if vFlag := flag.CommandLine.Lookup("v"); vFlag != nil {
		if v, err := strconv.Atoi(vFlag.Value.String()); err == nil {
			verbosity = v
		}
	}
	if verbosity < p.schedulerVerbosity {
		klog.Warningf("--scheduler-verbosity should be at most as high as --v flag, overriding it to: %d", verbosity)
		p.schedulerVerbosity = verbosity
	}
	if p.schedulerVerbosity < 0 {
		p.schedulerVerbosity = verbosity
	}

	p.o.AllowedSchedulers = allowedSchedulers
	p.o.BypassedSchedulers = scheduler_util.SchedulersMap(p.bypassedSchedulers)
	p.o.DrainPriorityConfig = drainPriorityConfigMap
	p.o.MinCoresTotal = minCoresTotal
	p.o.MaxCoresTotal = maxCoresTotal
	p.o.MinMemoryTotal = minMemoryTotal
	p.o.MaxMemoryTotal = maxMemoryTotal
	p.o.GpuTotal = parsedGpuTotal
	p.o.SchedulerConfig = parsedSchedConfig
	p.o.KubeClientOpts.KubeClientQPS = float32(p.kubeClientQPS)
	p.o.SchedulerVerbosityOffset = verbosity - p.schedulerVerbosity

	return p.o, nil
}

func minMaxFlagString(min, max int64) string {
	return fmt.Sprintf("%v:%v", min, max)
}

func validateMinMaxFlag(min, max int64) error {
	if min < 0 {
		return fmt.Errorf("min size must be greater or equal to  0")
	}
	if max < min {
		return fmt.Errorf("max size must be greater or equal to min size")
	}
	return nil
}

func parseMinMaxFlag(flag string) (int64, int64, error) {
	tokens := strings.SplitN(flag, ":", 2)
	if len(tokens) != 2 {
		return 0, 0, fmt.Errorf("wrong nodes configuration: %s", flag)
	}

	min, err := strconv.ParseInt(tokens[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to set min size: %s, expected integer, err: %v", tokens[0], err)
	}

	max, err := strconv.ParseInt(tokens[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to set max size: %s, expected integer, err: %v", tokens[1], err)
	}

	err = validateMinMaxFlag(min, max)
	if err != nil {
		return 0, 0, err
	}

	return min, max, nil
}

func parseMultipleGpuLimits(flags []string) ([]config.GpuLimits, error) {
	parsedFlags := make([]config.GpuLimits, 0, len(flags))
	for _, flag := range flags {
		parsedFlag, err := parseSingleGpuLimit(flag)
		if err != nil {
			return nil, err
		}
		parsedFlags = append(parsedFlags, parsedFlag)
	}
	return parsedFlags, nil
}

func parseSingleGpuLimit(limits string) (config.GpuLimits, error) {
	parts := strings.Split(limits, ":")
	if len(parts) != 3 {
		return config.GpuLimits{}, fmt.Errorf("incorrect gpu limit specification: %v", limits)
	}
	gpuType := parts[0]
	minVal, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return config.GpuLimits{}, fmt.Errorf("incorrect gpu limit - min is not integer: %v", limits)
	}
	maxVal, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return config.GpuLimits{}, fmt.Errorf("incorrect gpu limit - max is not integer: %v", limits)
	}
	if minVal < 0 {
		return config.GpuLimits{}, fmt.Errorf("incorrect gpu limit - min is less than 0; %v", limits)
	}
	if maxVal < 0 {
		return config.GpuLimits{}, fmt.Errorf("incorrect gpu limit - max is less than 0; %v", limits)
	}
	if minVal > maxVal {
		return config.GpuLimits{}, fmt.Errorf("incorrect gpu limit - min is greater than max; %v", limits)
	}
	parsedGpuLimits := config.GpuLimits{
		GpuType: gpuType,
		Min:     minVal,
		Max:     maxVal,
	}
	return parsedGpuLimits, nil
}

// parseShutdownGracePeriodsAndPriorities parse priorityGracePeriodStr and returns an array of ShutdownGracePeriodByPodPriority if succeeded.
// Otherwise, returns an empty list
func parseShutdownGracePeriodsAndPriorities(priorityGracePeriodStr string) []kubelet_config.ShutdownGracePeriodByPodPriority {
	var priorityGracePeriodMap, emptyMap []kubelet_config.ShutdownGracePeriodByPodPriority

	if priorityGracePeriodStr == "" {
		return emptyMap
	}
	priorityGracePeriodStrArr := strings.Split(priorityGracePeriodStr, ",")
	for _, item := range priorityGracePeriodStrArr {
		priorityAndPeriod := strings.Split(item, ":")
		if len(priorityAndPeriod) != 2 {
			klog.Errorf("Parsing shutdown grace periods failed because '%s' is not a priority and grace period couple separated by ':'", item)
			return emptyMap
		}
		priority, err := strconv.Atoi(priorityAndPeriod[0])
		if err != nil {
			klog.Errorf("Parsing shutdown grace periods and priorities failed: %v", err)
			return emptyMap
		}
		shutDownGracePeriod, err := strconv.Atoi(priorityAndPeriod[1])
		if err != nil {
			klog.Errorf("Parsing shutdown grace periods and priorities failed: %v", err)
			return emptyMap
		}
		priorityGracePeriodMap = append(priorityGracePeriodMap, kubelet_config.ShutdownGracePeriodByPodPriority{
			Priority:                   int32(priority),
			ShutdownGracePeriodSeconds: int64(shutDownGracePeriod),
		})
	}
	return priorityGracePeriodMap
}

func parseAllowedSchedulers(allowedSchedulers, bypassedSchedulers []string) (map[string]bool, error) {
	allowedSchedulersMap := scheduler_util.SchedulersMap(allowedSchedulers)
	if len(allowedSchedulers) == 0 {
		return allowedSchedulersMap, nil
	}
	bypassedSchedulersMap := scheduler_util.SchedulersMap(bypassedSchedulers)

	for scheduler := range bypassedSchedulersMap {
		if found := allowedSchedulersMap[scheduler]; !found {
			return nil, fmt.Errorf("Invalid configuration. --bypassed-scheduler-names should be a subset of --allowed-scheduler-names. %s not included in --allowed-scheduler-names", scheduler)
		}
	}
	return allowedSchedulersMap, nil
}
