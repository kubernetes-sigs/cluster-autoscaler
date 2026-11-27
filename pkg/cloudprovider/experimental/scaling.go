/*
Copyright 2026 The Kubernetes Authors.

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

package experimental

import (
	"context"
	"errors"
	"fmt"

	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
)

// InstanceCreator is an optional experimental extension interface for NodeGroup
// implementations that can synchronously return the identifiers of instances
// created during scale-up.
//
// This interface intentionally does not embed cloudprovider.NodeGroup. Callers
// should use IncreaseSizeOrFallback rather than discovering and falling back
// inline.
type InstanceCreator interface {
	// IncreaseSizeWithInstances increases the size of the node group by delta and
	// synchronously returns the instances created by the request. Like NodeGroup.IncreaseSize,
	// this function should wait until the node group size is updated (though newly created
	// instances do not need to be registered as Kubernetes Nodes yet).
	//
	// Delta must be positive. If delta <= 0, implementations must return a non-nil error
	// without modifying the node group's target size or creating any instances.
	//
	// Returned Instance values must have a non-empty Id field set to the value that
	// NodeGroup.Nodes() reports for them and that the corresponding Kubernetes Node will
	// receive in Spec.ProviderID. Instance.Status, when populated, should reflect the initial
	// state of the instance (for example, InstanceCreating or InstanceRunning).
	//
	// Implementations may return fewer than delta instances (for example, on partial success
	// or when the identity of some asynchronously created instances is not yet known). On
	// partial failure (such as a chunked batch failing midway or partial quota exhaustion),
	// implementations MUST return all already-created instances alongside the non-nil error;
	// callers MUST inspect the returned slice even when err != nil.
	//
	// Returning cloudprovider.ErrNotImplemented MUST leave the node group's target size
	// unchanged and create no instances (returning a nil/empty slice), so that callers can
	// safely fall back to NodeGroup.IncreaseSize.
	IncreaseSizeWithInstances(ctx context.Context, delta int) ([]cloudprovider.Instance, error)
}

// IncreaseSizeOrFallback increases the size of ng by delta using InstanceCreator
// when available, falling back to ng.IncreaseSize if ng does not implement
// InstanceCreator or dynamically returns cloudprovider.ErrNotImplemented.
//
// Real errors from IncreaseSizeWithInstances are propagated directly without
// falling back to ng.IncreaseSize, along with any partially created instances.
func IncreaseSizeOrFallback(ctx context.Context, ng cloudprovider.NodeGroup, delta int) ([]cloudprovider.Instance, error) {
	if ng == nil {
		return nil, fmt.Errorf("node group is nil")
	}

	if creator, ok := cloudprovider.As[InstanceCreator](ng); ok {
		instances, err := creator.IncreaseSizeWithInstances(ctx, delta)
		if !errors.Is(err, cloudprovider.ErrNotImplemented) {
			return instances, err
		}
	}

	return nil, ng.IncreaseSize(ctx, delta)
}
