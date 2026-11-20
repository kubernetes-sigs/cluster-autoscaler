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

package utilization

import (
	"context"
	"fmt"
	"testing"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// BenchmarkCalculate covers the median (9) and max (34) pods per node of a representative large cluster.
func BenchmarkCalculate(b *testing.B) {
	for _, podsPerNode := range []int{9, 34} {
		node := BuildTestNode("node", 64000, 256*1024*1024*1024)
		var pods []*apiv1.Pod
		for i := 0; i < podsPerNode; i++ {
			pod := BuildTestPod(fmt.Sprintf("p-%d", i), 500, 2*1024*1024*1024, WithNodeName(node.Name))
			if i%5 == 0 {
				pod = BuildTestPod(fmt.Sprintf("ds-%d", i), 100, 256*1024*1024, WithNodeName(node.Name), WithDSController())
			}
			pods = append(pods, pod)
		}
		nodeInfo := framework.NewTestNodeInfo(node, pods...)
		now := time.Now()
		b.Run(fmt.Sprintf("pods=%d", podsPerNode), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := Calculate(context.Background(), nodeInfo, true, true, false, nil, now); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
