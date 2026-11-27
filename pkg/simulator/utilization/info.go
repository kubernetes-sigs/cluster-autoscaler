/*
Copyright 2016 The Kubernetes Authors.

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

package utilization

import (
	"context"
	"fmt"
	"slices"
	"time"

	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	drautils "sigs.k8s.io/cluster-autoscaler/pkg/simulator/dynamicresources/utils"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/drain"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/gpu"
	podutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/pod"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// Info contains utilization information for a node.
type Info struct {
	CpuUtil             float64
	MemUtil             float64
	GpuUtil             float64
	DynamicResourceUtil float64
	// Resource name of highest utilization resource
	ResourceName apiv1.ResourceName
	// Max(CpuUtil, MemUtil), highest GPU resource utilization, or DynamicResourceUtil.
	Utilization float64
}

// Calculate calculates utilization of a node, defined as the maximum of CPU and
// memory utilization, or the highest per-resource GPU utilization for GPU nodes.
// Per-resource utilization is the sum of requests for it divided by allocatable.
// It also returns the individual CPU, memory and GPU utilization.
func Calculate(ctx context.Context, nodeInfo *framework.NodeInfo, skipDaemonSetPods, skipMirrorPods, draEnabled bool, gpuConfig *cloudprovider.GpuConfig, currentTime time.Time) (utilInfo Info, err error) {
	logger := klog.FromContext(ctx)
	if gpuConfig != nil && !gpuConfig.ExposedViaDra() {
		gpuResources := []apiv1.ResourceName{gpuConfig.ExtendedResourceName}
		if gpu.IsNvidiaGPUResource(gpuConfig.ExtendedResourceName) {
			for resourceName, allocatable := range nodeInfo.Node().Status.Allocatable {
				if resourceName != gpuConfig.ExtendedResourceName && gpu.IsNvidiaGPUResource(resourceName) && !allocatable.IsZero() {
					gpuResources = append(gpuResources, resourceName)
				}
			}
		}
		// Keep the provider-selected resource first so it wins utilization ties.
		slices.Sort(gpuResources[1:])

		gpuUtilInfo := Info{ResourceName: gpuConfig.ExtendedResourceName}
		for _, resourceName := range gpuResources {
			gpuUtil, err := CalculateUtilizationOfResource(nodeInfo, resourceName, skipDaemonSetPods, skipMirrorPods, currentTime)
			if err != nil {
				logger.V(3).Info("Node has unready GPU resource", "node", klog.KObj(nodeInfo.Node()), "resourceName", resourceName, "err", err)
				// Unready GPU resources contribute zero utilization so they do not block scale-down.
				continue
			}
			if gpuUtil > gpuUtilInfo.GpuUtil {
				gpuUtilInfo = Info{GpuUtil: gpuUtil, ResourceName: resourceName, Utilization: gpuUtil}
			}
		}

		// Skips cpu and memory utilization calculation for node with GPU.
		return gpuUtilInfo, nil
	}

	if draEnabled && len(nodeInfo.LocalResourceSlices) > 0 {
		// TODO(DRA): Figure out how to calculate Node utilization for DRA resources properly. The current logic is provisional, it should work well for Nodes with a single Pool of
		// expensive Devices but is probably not flexible enough for other scenarios.
		resourceName, highestUtil, err := drautils.HighestDynamicResourceUtilization(nodeInfo)
		if err != nil {
			return Info{}, err
		}
		return Info{DynamicResourceUtil: highestUtil, Utilization: highestUtil, ResourceName: resourceName}, nil
	}

	cpu, err := CalculateUtilizationOfResource(nodeInfo, apiv1.ResourceCPU, skipDaemonSetPods, skipMirrorPods, currentTime)
	if err != nil {
		return Info{}, err
	}
	mem, err := CalculateUtilizationOfResource(nodeInfo, apiv1.ResourceMemory, skipDaemonSetPods, skipMirrorPods, currentTime)
	if err != nil {
		return Info{}, err
	}

	utilization := Info{CpuUtil: cpu, MemUtil: mem}

	if cpu > mem {
		utilization.ResourceName = apiv1.ResourceCPU
		utilization.Utilization = cpu
	} else {
		utilization.ResourceName = apiv1.ResourceMemory
		utilization.Utilization = mem
	}

	return utilization, nil
}

// CalculateUtilizationOfResource calculates utilization of a given resource for a node.
func CalculateUtilizationOfResource(nodeInfo *framework.NodeInfo, resourceName apiv1.ResourceName, skipDaemonSetPods, skipMirrorPods bool, currentTime time.Time) (float64, error) {
	nodeAllocatable, found := nodeInfo.Node().Status.Allocatable[resourceName]
	if !found {
		return 0, fmt.Errorf("failed to get %v from %s", resourceName, nodeInfo.Node().Name)
	}
	if nodeAllocatable.MilliValue() == 0 {
		return 0, fmt.Errorf("%v is 0 at %s", resourceName, nodeInfo.Node().Name)
	}

	// if skipDaemonSetPods = True, DaemonSet pods resourses will be subtracted
	// from the node allocatable and won't be added to pods requests
	// the same with the Mirror pod.
	var podsRequest, daemonSetAndMirrorPodsUtilization int64
	for _, podInfo := range nodeInfo.Pods() {
		resourceValue := podRequestMilliValue(podInfo, resourceName)

		// factor daemonset pods out of the utilization calculations
		if skipDaemonSetPods && podutils.IsDaemonSetPod(podInfo.Pod) {
			daemonSetAndMirrorPodsUtilization += resourceValue
			continue
		}

		// factor mirror pods out of the utilization calculations
		if skipMirrorPods && podutils.IsMirrorPod(podInfo.Pod) {
			daemonSetAndMirrorPodsUtilization += resourceValue
			continue
		}

		// ignore Pods that should be terminated
		if drain.IsPodLongTerminating(podInfo.Pod, currentTime) {
			continue
		}

		podsRequest += resourceValue
	}

	return float64(podsRequest) / float64(nodeAllocatable.MilliValue()-daemonSetAndMirrorPodsUtilization), nil
}

// podRequestMilliValue returns the pod's request for resourceName in milli-units.
//
// For CPU and memory it uses the requests the scheduler already computed and cached in the
// PodInfo when the pod was added to the snapshot, instead of recomputing all requests for every
// resource. The scheduler uses the same helper as podutils.PodRequests, plus options for in-place
// pod-level resizes and DRA node-allocatable claims, which utilization never accounted for: pods
// with pod-level resources, resource claims or node-allocatable claim statuses keep using
// podutils.PodRequests, so results only change by rounding (see below).
//
// Values are rounded per pod (to milli-CPU and to bytes, like the scheduler's accounting)
// before being summed, instead of being summed exactly and rounded once. This only differs
// for requests below a milli-CPU or a byte.
func podRequestMilliValue(podInfo *framework.PodInfo, resourceName apiv1.ResourceName) int64 {
	if podInfo.PodInfo != nil && podInfo.Pod.Spec.Resources == nil && len(podInfo.Pod.Spec.ResourceClaims) == 0 &&
		len(podInfo.Pod.Status.NodeAllocatableResourceClaimStatuses) == 0 {
		switch resourceName {
		case apiv1.ResourceCPU:
			return podInfo.CalculateResource().Resource.GetMilliCPU()
		case apiv1.ResourceMemory:
			return podInfo.CalculateResource().Resource.GetMemory() * 1000
		}
	}
	podRequests := podutils.PodRequests(podInfo.Pod)
	value := podRequests[resourceName]
	return value.MilliValue()
}
