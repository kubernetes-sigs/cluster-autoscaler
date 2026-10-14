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

package capacitybuffer

// Constants to use in Capacity Buffers objects
const (
	ActiveProvisioningStrategy      = "buffer.x-k8s.io/active-capacity"
	CapacityBufferKind              = "CapacityBuffer"
	CapacityBufferApiVersion        = "autoscaling.x-k8s.io/v1beta1"
	ReadyForProvisioningCondition   = "ReadyForProvisioning"
	ProvisioningCondition           = "Provisioning"
	LimitedByQuotasCondition        = "LimitedByQuotas"
	LimitedByQuotasReason           = "ResourceQuotasAllocated"
	AttributesSetSuccessfullyReason = "AttributesSetSuccessfully"

	// ScalableRefWatchedCondition reports whether the controller successfully established
	// a watch for the custom scalable resource referenced by spec.scalableRef.
	//
	// The condition is only set on buffers referencing a kind that is not watched statically,
	// as built-in scalable kinds always have a watch.
	//
	// It is deliberately namespaced under cluster-autoscaler.kubernetes.io: this condition
	// is a Cluster Autoscaler implementation detail, not a part of public CapacityBuffer API.
	ScalableRefWatchedCondition = "cluster-autoscaler.kubernetes.io/scalable-ref-watched"

	// WatchEstablishedReason indicates the referenced kind is being watched.
	WatchEstablishedReason = "WatchEstablished"
	// UnknownKindReason indicates the referenced kind could not be resolved against the
	// API server, most commonly because its CRD is not installed.
	UnknownKindReason = "UnknownKind"
	// WatchNotSyncedReason indicates a watch was opened but its cache never synced,
	// most commonly because the controller lacks list/watch permissions on the kind.
	WatchNotSyncedReason = "WatchNotSynced"
	// WatchLimitExceededReason indicates the maximum number of dynamic watches is exhausted.
	WatchLimitExceededReason = "WatchLimitExceeded"
)
