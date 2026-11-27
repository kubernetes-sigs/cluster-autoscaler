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

package metrics

import (
	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/klog/v2"
	cbclient "sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/client"
)

const (
	readyPodSlotsMetricName     = "capacity_buffer_ready_pod_slots"
	requestedPodSlotsMetricName = "capacity_buffer_requested_pod_slots"

	readyPodSlotsHelp     = "Last reported number of CapacityBuffer pod slots placed by the autoscaler simulation on existing Kubernetes nodes (status.readyReplicas), excluding upcoming nodes. Counts simulated fake-pod placements, not running pods, nodes, or reserved capacity. Asynchronous status may be stale and may describe a different evaluation than the requested target."
	requestedPodSlotsHelp = "Last reported effective CapacityBuffer target in pod slots (status.replicas), after controller-applied limits and quotas. Counts desired fake-pod slots, not Kubernetes nodes or outstanding node requests."
)

var (
	readyPodSlotsDesc = prometheus.NewDesc(
		prometheus.BuildFQName("cluster_autoscaler", "", readyPodSlotsMetricName),
		readyPodSlotsHelp,
		[]string{"namespace", "buffer_name"},
		nil,
	)
	requestedPodSlotsDesc = prometheus.NewDesc(
		prometheus.BuildFQName("cluster_autoscaler", "", requestedPodSlotsMetricName),
		requestedPodSlotsHelp,
		[]string{"namespace", "buffer_name"},
		nil,
	)
)

// podSlotsCollector exports last-reported CapacityBuffer status, omitting nil fields
// independently while preserving stored zeros. Values may come from different
// evaluations and must not be interpreted as fresh availability.
type podSlotsCollector struct {
	client capacityBufferClient
}

// NewBufferPodSlotsCollector creates a new pod slots collector instance.
func NewBufferPodSlotsCollector(client capacityBufferClient) *podSlotsCollector {
	return &podSlotsCollector{client: client}
}

// RegisterBufferPodSlotsCollector registers the capacity buffer pod slots collector.
func RegisterBufferPodSlotsCollector(client *cbclient.CapacityBufferClient) {
	legacyregistry.MustRegister(NewBufferPodSlotsCollector(client))
}

// Describe implements the prometheus.Collector interface.
func (c *podSlotsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- readyPodSlotsDesc
	ch <- requestedPodSlotsDesc
}

// Collect implements the prometheus.Collector interface.
func (c *podSlotsCollector) Collect(ch chan<- prometheus.Metric) {
	buffers, err := c.client.ListCapacityBuffers("")
	if err != nil {
		klog.Errorf("Failed to list capacity buffers with error: %v", err.Error())
		return
	}

	for _, buffer := range buffers {
		if buffer.Status.Replicas != nil {
			reportPodSlotsMetric(ch, requestedPodSlotsDesc, float64(*buffer.Status.Replicas), buffer.Namespace, buffer.Name)
		}
		if buffer.Status.ReadyReplicas != nil {
			reportPodSlotsMetric(ch, readyPodSlotsDesc, float64(*buffer.Status.ReadyReplicas), buffer.Namespace, buffer.Name)
		}
	}
}

func reportPodSlotsMetric(ch chan<- prometheus.Metric, desc *prometheus.Desc, value float64, namespace, name string) {
	ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, namespace, name)
}

// Create implements the k8smetrics.Registerable interface.
func (c *podSlotsCollector) Create(version *semver.Version) bool {
	return true
}

// ClearState implements the k8smetrics.Registerable interface.
func (c *podSlotsCollector) ClearState() {
	// No-op for stateless collector
}

// FQName implements the k8smetrics.Registerable interface.
func (c *podSlotsCollector) FQName() string {
	return readyPodSlotsMetricName
}
