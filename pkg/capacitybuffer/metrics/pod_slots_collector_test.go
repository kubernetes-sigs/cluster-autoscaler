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
	"errors"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/utils/ptr"
)

const (
	readyPodSlotsFQName     = "cluster_autoscaler_capacity_buffer_ready_pod_slots"
	requestedPodSlotsFQName = "cluster_autoscaler_capacity_buffer_requested_pod_slots"

	// Expected help wording is asserted explicitly so production help strings
	// cannot drift from the documented metric semantics unnoticed.
	readyPodSlotsExpectedHelp     = "Last reported number of CapacityBuffer pod slots placed by the autoscaler simulation on existing Kubernetes nodes (status.readyReplicas), excluding upcoming nodes. Counts simulated fake-pod placements, not running pods, nodes, or reserved capacity. Asynchronous status may be stale and may describe a different evaluation than the requested target."
	requestedPodSlotsExpectedHelp = "Last reported effective CapacityBuffer target in pod slots (status.replicas), after controller-applied limits and quotas. Counts desired fake-pod slots, not Kubernetes nodes or outstanding node requests."
)

// fakePodSlotsClient is a fake capacity buffer client for the pod slots collector tests.
type fakePodSlotsClient struct {
	buffers []*v1beta1.CapacityBuffer
	listErr error
}

func (c *fakePodSlotsClient) ListCapacityBuffers(namespace string) ([]*v1beta1.CapacityBuffer, error) {
	if c.listErr != nil {
		return nil, c.listErr
	}
	return c.buffers, nil
}

func newPodSlotsTestBuffer(namespace, name, uid string, replicas, readyReplicas *int32) *v1beta1.CapacityBuffer {
	return &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			UID:       types.UID(uid),
		},
		Status: v1beta1.CapacityBufferStatus{
			Replicas:      replicas,
			ReadyReplicas: readyReplicas,
		},
	}
}

// newPodSlotsRegistry registers the collector in an isolated registry so the
// tests never collide with the global legacyregistry.
func newPodSlotsRegistry(collector *podSlotsCollector) k8smetrics.KubeRegistry {
	registry := k8smetrics.NewKubeRegistry()
	registry.MustRegister(collector)
	return registry
}

// collectedSamples gathers the registry and returns family name -> "namespace/buffer_name" -> gauge value.
func collectedSamples(t *testing.T, registry k8smetrics.KubeRegistry) map[string]map[string]float64 {
	families, err := registry.Gather()
	assert.NoError(t, err)
	samples := make(map[string]map[string]float64)
	for _, family := range families {
		values := make(map[string]float64)
		for _, metric := range family.Metric {
			labels := make(map[string]string, len(metric.Label))
			for _, labelPair := range metric.Label {
				labels[*labelPair.Name] = *labelPair.Value
			}
			values[labels["namespace"]+"/"+labels["buffer_name"]] = metric.GetGauge().GetValue()
		}
		samples[family.GetName()] = values
	}
	return samples
}

func withSpecTarget(buffer *v1beta1.CapacityBuffer, target *int32) *v1beta1.CapacityBuffer {
	buffer.Spec.Replicas = target
	return buffer
}

func TestBufferPodSlotsCollector_Collect(t *testing.T) {
	testCases := []struct {
		name    string
		buffers []*v1beta1.CapacityBuffer
		want    map[string]map[string]float64
	}{
		{
			name:    "requested 3 with stored ready 2",
			buffers: []*v1beta1.CapacityBuffer{newPodSlotsTestBuffer("default", "buf", "uid", ptr.To(int32(3)), ptr.To(int32(2)))},
			want: map[string]map[string]float64{
				requestedPodSlotsFQName: {"default/buf": 3},
				readyPodSlotsFQName:     {"default/buf": 2},
			},
		},
		{
			name:    "both fields explicitly zero",
			buffers: []*v1beta1.CapacityBuffer{newPodSlotsTestBuffer("default", "buf", "uid", ptr.To(int32(0)), ptr.To(int32(0)))},
			want: map[string]map[string]float64{
				requestedPodSlotsFQName: {"default/buf": 0},
				readyPodSlotsFQName:     {"default/buf": 0},
			},
		},
		{
			name:    "ready nil with requested set",
			buffers: []*v1beta1.CapacityBuffer{newPodSlotsTestBuffer("default", "buf", "uid", ptr.To(int32(3)), nil)},
			want: map[string]map[string]float64{
				requestedPodSlotsFQName: {"default/buf": 3},
			},
		},
		{
			name:    "requested nil with ready set",
			buffers: []*v1beta1.CapacityBuffer{newPodSlotsTestBuffer("default", "buf", "uid", nil, ptr.To(int32(2)))},
			want: map[string]map[string]float64{
				readyPodSlotsFQName: {"default/buf": 2},
			},
		},
		{
			name:    "both nil emits nothing",
			buffers: []*v1beta1.CapacityBuffer{newPodSlotsTestBuffer("default", "buf", "uid", nil, nil)},
			want:    map[string]map[string]float64{},
		},
		{
			name: "multiple names and namespaces",
			buffers: []*v1beta1.CapacityBuffer{
				newPodSlotsTestBuffer("ns-1", "buf-a", "uid-1", ptr.To(int32(3)), ptr.To(int32(2))),
				newPodSlotsTestBuffer("ns-2", "buf-b", "uid-2", ptr.To(int32(1)), ptr.To(int32(0))),
				newPodSlotsTestBuffer("ns-1", "buf-c", "uid-3", nil, ptr.To(int32(4))),
			},
			want: map[string]map[string]float64{
				requestedPodSlotsFQName: {"ns-1/buf-a": 3, "ns-2/buf-b": 1},
				readyPodSlotsFQName:     {"ns-1/buf-a": 2, "ns-2/buf-b": 0, "ns-1/buf-c": 4},
			},
		},
		{
			name: "stored ready can exceed requested after target reduction",
			buffers: []*v1beta1.CapacityBuffer{
				withSpecTarget(newPodSlotsTestBuffer("default", "buf", "uid", ptr.To(int32(2)), ptr.To(int32(3))), ptr.To(int32(5))),
			},
			want: map[string]map[string]float64{
				requestedPodSlotsFQName: {"default/buf": 2},
				readyPodSlotsFQName:     {"default/buf": 3},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			collector := NewBufferPodSlotsCollector(&fakePodSlotsClient{buffers: tc.buffers})
			assert.Equal(t, tc.want, collectedSamples(t, newPodSlotsRegistry(collector)))
		})
	}
}

func TestBufferPodSlotsCollector_SuccessiveCollections(t *testing.T) {
	client := &fakePodSlotsClient{
		buffers: []*v1beta1.CapacityBuffer{
			newPodSlotsTestBuffer("default", "buf", "uid-1", ptr.To(int32(3)), ptr.To(int32(2))),
		},
	}
	collector := NewBufferPodSlotsCollector(client)
	registry := newPodSlotsRegistry(collector)

	assert.Equal(t, map[string]map[string]float64{
		requestedPodSlotsFQName: {"default/buf": 3},
		readyPodSlotsFQName:     {"default/buf": 2},
	}, collectedSamples(t, registry))

	// A buffer missing from the list must not retain samples from earlier scrapes.
	client.buffers = nil
	assert.Empty(t, collectedSamples(t, registry))

	// Changed status values are visible on the next scrape and inputs are never mutated.
	buffer := newPodSlotsTestBuffer("default", "buf", "uid-3", ptr.To(int32(2)), ptr.To(int32(1)))
	client.buffers = []*v1beta1.CapacityBuffer{buffer}
	assert.Equal(t, map[string]map[string]float64{
		requestedPodSlotsFQName: {"default/buf": 2},
		readyPodSlotsFQName:     {"default/buf": 1},
	}, collectedSamples(t, registry))

	buffer.Status.Replicas = ptr.To(int32(4))
	buffer.Status.ReadyReplicas = ptr.To(int32(3))
	snapshotAfterUpdate := buffer.DeepCopy()
	assert.Equal(t, map[string]map[string]float64{
		requestedPodSlotsFQName: {"default/buf": 4},
		readyPodSlotsFQName:     {"default/buf": 3},
	}, collectedSamples(t, registry))
	assert.Equal(t, snapshotAfterUpdate, buffer)
}

func TestBufferPodSlotsCollector_ListError(t *testing.T) {
	client := &fakePodSlotsClient{listErr: errors.New("informer not synced")}
	collector := NewBufferPodSlotsCollector(client)
	registry := newPodSlotsRegistry(collector)

	// A failed listing publishes no samples without panicking.
	assert.Empty(t, collectedSamples(t, registry))

	// A later successful scrape reads the current fields.
	client.listErr = nil
	client.buffers = []*v1beta1.CapacityBuffer{
		newPodSlotsTestBuffer("default", "buf", "uid", ptr.To(int32(5)), ptr.To(int32(4))),
	}
	assert.Equal(t, map[string]map[string]float64{
		requestedPodSlotsFQName: {"default/buf": 5},
		readyPodSlotsFQName:     {"default/buf": 4},
	}, collectedSamples(t, registry))
}

func TestBufferPodSlotsCollector_MetricFamilies(t *testing.T) {
	client := &fakePodSlotsClient{
		buffers: []*v1beta1.CapacityBuffer{
			newPodSlotsTestBuffer("default", "buf", "uid", ptr.To(int32(1)), ptr.To(int32(1))),
		},
	}
	collector := NewBufferPodSlotsCollector(client)

	families, err := newPodSlotsRegistry(collector).Gather()
	assert.NoError(t, err)

	wantHelp := map[string]string{
		readyPodSlotsFQName:     readyPodSlotsExpectedHelp,
		requestedPodSlotsFQName: requestedPodSlotsExpectedHelp,
	}
	gotNames := make([]string, 0, len(families))
	for _, family := range families {
		gotNames = append(gotNames, family.GetName())
		assert.Equal(t, dto.MetricType_GAUGE, family.GetType())
		assert.Equal(t, wantHelp[family.GetName()], family.GetHelp())
		for _, metric := range family.Metric {
			labels := make([]string, 0, len(metric.Label))
			for _, labelPair := range metric.Label {
				labels = append(labels, *labelPair.Name)
			}
			assert.ElementsMatch(t, []string{"namespace", "buffer_name"}, labels)
		}
	}
	assert.Equal(t, []string{readyPodSlotsFQName, requestedPodSlotsFQName}, gotNames)
}
