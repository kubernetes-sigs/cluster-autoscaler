/*
Copyright 2017 The Kubernetes Authors.

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

package utils

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	apiv1 "k8s.io/api/core/v1"
	kube_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kube_client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate/api"
	"sigs.k8s.io/yaml"

	klog "k8s.io/klog/v2"
)

const (
	// ConfigMapLastUpdatedKey is the name of annotation informing about status ConfigMap last update.
	ConfigMapLastUpdatedKey = "cluster-autoscaler.kubernetes.io/last-updated"
	// ConfigMapLastUpdateFormat it the timestamp format used for last update annotation in status ConfigMap
	ConfigMapLastUpdateFormat = "2006-01-02 15:04:05.999999999 -0700 MST"
	// maxStatusConfigMapSize is the maximum number of bytes we can write to a config map. The limit comes from 1MB value limit of etcd with a safety buffer.
	maxStatusConfigMapSize = 1000000
)

// LogEventRecorder records events on some top-level object, to give user (without access to logs) a view of most important CA actions.
type LogEventRecorder struct {
	recorder     record.EventRecorder
	statusObject runtime.Object
	active       bool
}

// Event records an event on underlying object. This does nothing if the underlying object is not set.
func (ler *LogEventRecorder) Event(eventtype, reason, message string) {
	if ler.active && ler.statusObject != nil {
		ler.recorder.Event(ler.statusObject, eventtype, reason, message)
	}
}

// Eventf records an event on underlying object. This does nothing if the underlying object is not set.
func (ler *LogEventRecorder) Eventf(eventtype, reason, message string, args ...interface{}) {
	if ler.active && ler.statusObject != nil {
		ler.recorder.Eventf(ler.statusObject, eventtype, reason, message, args...)
	}
}

// EmptyClusterAutoscalerStatus returns empty status for ClusterAutoscalerStatus when it is being initialized.
func EmptyClusterAutoscalerStatus() *api.ClusterAutoscalerStatus {
	return &api.ClusterAutoscalerStatus{
		AutoscalerStatus: api.ClusterAutoscalerInitializing,
	}
}

// NewStatusMapRecorder creates a LogEventRecorder creating events on status configmap.
// If the configmap doesn't exist it will be created (with 'Initializing' status).
// If active == false the map will not be created and no events will be recorded.
func NewStatusMapRecorder(kubeClient kube_client.Interface, namespace string, recorder record.EventRecorder, active bool, statusConfigMapName string) (*LogEventRecorder, error) {
	var mapObj runtime.Object
	var err error
	if active {
		mapObj, err = WriteStatusConfigMap(kubeClient, namespace, *EmptyClusterAutoscalerStatus(), nil, statusConfigMapName, time.Now())
		if err != nil {
			return nil, errors.New("Failed to init status ConfigMap")
		}
	}
	return &LogEventRecorder{
		recorder:     recorder,
		statusObject: mapObj,
		active:       active,
	}, nil
}

// WriteStatusConfigMap writes updates status ConfigMap with a given message or creates a new
// ConfigMap if it doesn't exist. If logRecorder is passed and configmap update is successful
// logRecorder's internal reference will be updated.
func WriteStatusConfigMap(kubeClient kube_client.Interface, namespace string, status api.ClusterAutoscalerStatus, logRecorder *LogEventRecorder, statusConfigMapName string, currentTime time.Time) (*apiv1.ConfigMap, error) {
	statusUpdateTime := currentTime.Format(ConfigMapLastUpdateFormat)
	status.Time = statusUpdateTime
	var configMap *apiv1.ConfigMap
	var getStatusError, writeStatusError error
	var errMsg string
	maps := kubeClient.CoreV1().ConfigMaps(namespace)
	configMap, getStatusError = maps.Get(context.TODO(), statusConfigMapName, metav1.GetOptions{})
	statusYaml, err := marshalAndTruncateStatus(status, maxStatusConfigMapSize)
	if err != nil {
		return nil, err
	}
	statusMsg := string(statusYaml)
	if getStatusError == nil {
		if configMap.Data == nil {
			configMap.Data = make(map[string]string)
		}
		configMap.Data["status"] = statusMsg
		if configMap.ObjectMeta.Annotations == nil {
			configMap.ObjectMeta.Annotations = make(map[string]string)
		}
		configMap.ObjectMeta.Annotations[ConfigMapLastUpdatedKey] = statusUpdateTime
		configMap, writeStatusError = maps.Update(context.TODO(), configMap, metav1.UpdateOptions{})
	} else if kube_errors.IsNotFound(getStatusError) {
		configMap = &apiv1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      statusConfigMapName,
				Annotations: map[string]string{
					ConfigMapLastUpdatedKey: statusUpdateTime,
				},
			},
			Data: map[string]string{
				"status": statusMsg,
			},
		}
		configMap, writeStatusError = maps.Create(context.TODO(), configMap, metav1.CreateOptions{})
	} else {
		errMsg = fmt.Sprintf("Failed to retrieve status configmap for update: %v", getStatusError)
	}
	if writeStatusError != nil {
		errMsg = fmt.Sprintf("Failed to write status configmap: %v", writeStatusError)
	}
	if errMsg != "" {
		klog.Error(errMsg)
		return nil, errors.New(errMsg)
	}
	klog.V(8).Infof("Successfully wrote status configmap with body \"%v\"", statusMsg)
	// Having this as a side-effect is somewhat ugly
	// But it makes error handling easier, as we get a free retry each loop
	if logRecorder != nil {
		logRecorder.statusObject = configMap
	}
	return configMap, nil
}

// DeleteStatusConfigMap deletes status configmap
func DeleteStatusConfigMap(kubeClient kube_client.Interface, namespace string, statusConfigMapName string) error {
	maps := kubeClient.CoreV1().ConfigMaps(namespace)
	err := maps.Delete(context.TODO(), statusConfigMapName, metav1.DeleteOptions{})
	if err != nil {
		klog.Error("Failed to delete status configmap")
	}
	return err
}

// marshalAndTruncateStatus marshals the ClusterAutoscalerStatus to YAML.
// If the resulting YAML exceeds maxStatusConfigMapSize, it uses binary search
// to find the maximum number of NodeGroups that can be included without exceeding the limit.
func marshalAndTruncateStatus(status api.ClusterAutoscalerStatus, maxStatusConfigMapSize int) ([]byte, error) {
	statusYaml, err := yaml.Marshal(status)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal status configmap: %v", err)
	}

	if len(statusYaml) <= maxStatusConfigMapSize || len(status.NodeGroups) == 0 {
		return statusYaml, nil
	}

	// Sort NodeGroups to ensure the most important ones are kept if truncation is necessary.
	// Priority: Unhealthy > ScaleUp Backoff/Error > Active ScaleUp > Active ScaleDown.
	slices.SortStableFunc(status.NodeGroups, func(a, b api.NodeGroupStatus) int {
		// Priority 1: Unhealthy NodeGroups
		aUnhealthy := a.Health.Status == api.ClusterAutoscalerUnhealthy
		bUnhealthy := b.Health.Status == api.ClusterAutoscalerUnhealthy
		if aUnhealthy != bUnhealthy {
			if aUnhealthy {
				return -1
			}
			return 1
		}

		// Priority 2: ScaleUp is Backoff or Unhealthy
		aBackoff := a.ScaleUp.Status == api.ClusterAutoscalerBackoff || a.ScaleUp.Status == api.ClusterAutoscalerUnhealthy
		bBackoff := b.ScaleUp.Status == api.ClusterAutoscalerBackoff || b.ScaleUp.Status == api.ClusterAutoscalerUnhealthy
		if aBackoff != bBackoff {
			if aBackoff {
				return -1
			}
			return 1
		}

		// Priority 3: ScaleUp is InProgress or Needed
		aActiveScaleUp := a.ScaleUp.Status == api.ClusterAutoscalerInProgress || a.ScaleUp.Status == api.ClusterAutoscalerNeeded
		bActiveScaleUp := b.ScaleUp.Status == api.ClusterAutoscalerInProgress || b.ScaleUp.Status == api.ClusterAutoscalerNeeded
		if aActiveScaleUp != bActiveScaleUp {
			if aActiveScaleUp {
				return -1
			}
			return 1
		}

		// Priority 4: ScaleDown Candidates Present
		aActiveScaleDown := a.ScaleDown.Status == api.ClusterAutoscalerCandidatesPresent
		bActiveScaleDown := b.ScaleDown.Status == api.ClusterAutoscalerCandidatesPresent
		if aActiveScaleDown != bActiveScaleDown {
			if aActiveScaleDown {
				return -1
			}
			return 1
		}

		return 0
	})

	originalNodeGroups := status.NodeGroups
	var bestYaml []byte
	var marshalErr error

	// Search for the first length where the marshaled YAML size strictly exceeds maxStatusConfigMapSize.
	failedLength := sort.Search(len(originalNodeGroups)+1, func(length int) bool {
		status.NodeGroups = originalNodeGroups[:length]
		midYaml, err := yaml.Marshal(status)
		if err != nil {
			marshalErr = err
			return true // Treat error as exceeding size to stop expanding
		}

		if len(midYaml) <= maxStatusConfigMapSize {
			bestYaml = midYaml
			return false // Length fits, try a larger length
		}
		return true // Length exceeds limit, try a smaller length
	})

	if marshalErr != nil {
		return nil, fmt.Errorf("failed to marshal status configmap during truncation: %v", marshalErr)
	}

	if failedLength <= len(originalNodeGroups) {
		finalLength := max(failedLength-1, 0)
		klog.Infof("Status configmap size limit exceeded. Truncated from %d to %d NodeGroups", len(originalNodeGroups), finalLength)
	}

	// bestYaml holds the yaml for failedLength - 1.
	// If even 0 node groups exceeds the limit, bestYaml will be nil.
	if bestYaml == nil {
		status.NodeGroups = originalNodeGroups[:0]
		return yaml.Marshal(status)
	}

	return bestYaml, nil
}
