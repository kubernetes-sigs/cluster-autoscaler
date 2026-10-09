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
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	scheduler_config "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/gce/localssdsize"
)

func testLogOptions(schedulerConfig *scheduler_config.KubeSchedulerConfiguration) LoggableAutoscalingOptions {
	return LoggableAutoscalingOptions{
		NodeGroupDefaults: NodeGroupAutoscalingOptions{ScaleDownUnneededTime: 10 * time.Minute},
		MaxNodesTotal:     100,
		ExpanderNames:     "least-waste",
		SchedulerConfig:   schedulerConfig,
		GCEOptions: GCEOptions{
			ConcurrentRefreshes:      2,
			LocalSSDDiskSizeProvider: localssdsize.NewSimpleLocalSSDProvider(),
		},
	}
}

func TestLoggableAutoscalingOptionsString(t *testing.T) {
	for name, tc := range map[string]struct {
		schedulerConfig *scheduler_config.KubeSchedulerConfiguration
		want            string
	}{
		"default scheduler config": {want: "SchedulerConfig:default "},
		"custom scheduler config": {
			schedulerConfig: &scheduler_config.KubeSchedulerConfiguration{Parallelism: 16},
			want:            "SchedulerConfig:custom (--scheduler-config-file) ",
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := testLogOptions(tc.schedulerConfig).String()
			assert.Contains(t, s, tc.want)
			assert.Contains(t, s, "{NodeGroupDefaults:{ScaleDownUtilizationThreshold:0 ")
			assert.Contains(t, s, "ScaleDownUnneededTime:10m0s ")
			assert.Contains(t, s, " MaxNodesTotal:100 ")
			assert.Contains(t, s, " ExpanderNames:least-waste ")
			assert.Contains(t, s, " GCEOptions:{ConcurrentRefreshes:2 ")
			assert.NotContains(t, s, "LocalSSDDiskSizeProvider")
			assert.NotContains(t, s, "Parallelism:16")
		})
	}
}

func TestLoggableAutoscalingOptionsMarshalLog(t *testing.T) {
	for name, tc := range map[string]struct {
		schedulerConfig *scheduler_config.KubeSchedulerConfiguration
		want            string
	}{
		"default scheduler config": {want: SchedulerConfigDefault},
		"custom scheduler config": {
			schedulerConfig: &scheduler_config.KubeSchedulerConfiguration{Parallelism: 16},
			want:            SchedulerConfigCustom,
		},
	} {
		t.Run(name, func(t *testing.T) {
			// Structured loggers serialize the result of MarshalLog, so check its JSON form.
			marshalled, err := json.Marshal(testLogOptions(tc.schedulerConfig).MarshalLog())
			assert.NoError(t, err)

			var got map[string]interface{}
			assert.NoError(t, json.Unmarshal(marshalled, &got))
			assert.Equal(t, tc.want, got["SchedulerConfig"])
			assert.Equal(t, 100.0, got["MaxNodesTotal"])
			assert.Equal(t, "least-waste", got["ExpanderNames"])
			assert.Equal(t, "10m0s", got["NodeGroupDefaults"].(map[string]interface{})["ScaleDownUnneededTime"])
			gceOptions := got["GCEOptions"].(map[string]interface{})
			assert.Equal(t, 2.0, gceOptions["ConcurrentRefreshes"])
			assert.NotContains(t, gceOptions, "LocalSSDDiskSizeProvider")
		})
	}
}
