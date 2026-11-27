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

package mocks

import (
	"context"

	mock "github.com/stretchr/testify/mock"

	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/experimental"
	cpmocks "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/mocks"
)

var (
	_ experimental.InstanceCreator = (*InstanceCreator)(nil)
	_ experimental.InstanceCreator = (*InstanceCreatingNodeGroup)(nil)
	_ cloudprovider.NodeGroup      = (*InstanceCreatingNodeGroup)(nil)
)

// InstanceCreator is a standalone mock type for experimental.InstanceCreator.
type InstanceCreator struct {
	mock.Mock
}

// IncreaseSizeWithInstances provides a mock function with given fields: delta.
func (_m *InstanceCreator) IncreaseSizeWithInstances(ctx context.Context, delta int) ([]cloudprovider.Instance, error) {
	return unpackIncreaseSizeResult(_m.Called(delta), delta)
}

// InstanceCreatingNodeGroup is a mock implementing both cloudprovider.NodeGroup
// and experimental.InstanceCreator. It embeds cpmocks.NodeGroup so existing
// cpmocks.NodeGroup consumers do not inadvertently implement InstanceCreator.
type InstanceCreatingNodeGroup struct {
	cpmocks.NodeGroup
}

// IncreaseSizeWithInstances provides a mock function with given fields: delta.
func (_m *InstanceCreatingNodeGroup) IncreaseSizeWithInstances(ctx context.Context, delta int) ([]cloudprovider.Instance, error) {
	return unpackIncreaseSizeResult(_m.Called(delta), delta)
}

func unpackIncreaseSizeResult(ret mock.Arguments, delta int) ([]cloudprovider.Instance, error) {
	var r0 []cloudprovider.Instance
	if rf, ok := ret.Get(0).(func(int) []cloudprovider.Instance); ok {
		r0 = rf(delta)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).([]cloudprovider.Instance)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(int) error); ok {
		r1 = rf(delta)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
