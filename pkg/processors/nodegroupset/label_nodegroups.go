/*
Copyright 2021 The Kubernetes Authors.

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

package nodegroupset

import (
	klog "k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

// CreateLabelNodeInfoComparator returns a comparator that checks for node group similarity using the given labels.
func CreateLabelNodeInfoComparator(labels []string) NodeInfoComparator {
	return func(n1, n2 *framework.NodeInfo) bool {
		return areLabelsSame(n1, n2, labels)
	}
}

func areLabelsSame(n1, n2 *framework.NodeInfo, labels []string) bool {
	for _, label := range labels {
		val1, exists := n1.Node().ObjectMeta.Labels[label]
		if !exists {
			klog.V(8).InfoS("label not present on node", "label", label, "node", n1.Node().Name)
			return false
		}
		val2, exists := n2.Node().ObjectMeta.Labels[label]
		if !exists {
			klog.V(8).InfoS("label not present on node", "label", label, "node", n2.Node().Name)
			return false
		}
		if val1 != val2 {
			klog.V(8).InfoS("label did not match", "label", label, "node1", n1.Node().Name, "value1", val1, "node2", n2.Node().Name, "value2", val2)
			return false
		}
	}
	return true
}
