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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/experimental"
	cpmocks "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/mocks"
)

func TestMockNodeGroupDoesNotImplementInstanceCreator(t *testing.T) {
	var ng cloudprovider.NodeGroup = &cpmocks.NodeGroup{}
	_, ok := cloudprovider.As[experimental.InstanceCreator](ng)
	assert.False(t, ok, "cpmocks.NodeGroup must not implement experimental.InstanceCreator to avoid breaking existing mock consumers")
}

func TestMockInstanceCreator(t *testing.T) {
	ctx := context.Background()
	want := []cloudprovider.Instance{{Id: "inst-1"}}

	m := &InstanceCreator{}
	m.On("IncreaseSizeWithInstances", 1).Return(want, nil).Once()

	got, err := m.IncreaseSizeWithInstances(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	m.AssertExpectations(t)
}

func TestMockInstanceCreatingNodeGroup(t *testing.T) {
	ctx := context.Background()

	t.Run("static return values", func(t *testing.T) {
		wantInstances := []cloudprovider.Instance{
			{Id: "inst-1", Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceCreating}},
		}
		wantErr := errors.New("partial failure")

		m := &InstanceCreatingNodeGroup{}
		m.On("Id").Return("ng-1")
		m.On("IncreaseSizeWithInstances", 2).Return(wantInstances, wantErr)

		var ng cloudprovider.NodeGroup = m
		creator, ok := cloudprovider.As[experimental.InstanceCreator](ng)
		require.True(t, ok)

		assert.Equal(t, "ng-1", ng.Id())
		got, err := creator.IncreaseSizeWithInstances(ctx, 2)
		assert.ErrorIs(t, err, wantErr)
		assert.Equal(t, wantInstances, got)
		m.AssertExpectations(t)
	})

	t.Run("nil-safe returns", func(t *testing.T) {
		m := &InstanceCreatingNodeGroup{}
		m.On("IncreaseSizeWithInstances", 1).Return(nil, cloudprovider.ErrNotImplemented).Once()
		m.On("IncreaseSizeWithInstances", 2).Return(nil, nil).Once()

		got, err := m.IncreaseSizeWithInstances(ctx, 1)
		assert.ErrorIs(t, err, cloudprovider.ErrNotImplemented)
		assert.Nil(t, got)

		got, err = m.IncreaseSizeWithInstances(ctx, 2)
		assert.NoError(t, err)
		assert.Nil(t, got)
		m.AssertExpectations(t)
	})

	t.Run("function callbacks", func(t *testing.T) {
		m := &InstanceCreatingNodeGroup{}
		wantErr := errors.New("callback error")
		m.On("IncreaseSizeWithInstances", 3).Return(
			func(delta int) []cloudprovider.Instance {
				insts := make([]cloudprovider.Instance, delta)
				for i := range delta {
					insts[i] = cloudprovider.Instance{Id: "cb-inst"}
				}
				return insts
			},
			func(delta int) error {
				return wantErr
			},
		)

		got, err := m.IncreaseSizeWithInstances(ctx, 3)
		assert.ErrorIs(t, err, wantErr)
		require.Len(t, got, 3)
		assert.Equal(t, "cb-inst", got[0].Id)
		m.AssertExpectations(t)
	})
}
