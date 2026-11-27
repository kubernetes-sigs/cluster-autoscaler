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

package experimental_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/experimental"
	expmocks "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/experimental/mocks"
	cpmocks "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/mocks"
)

type wrappingNodeGroup struct {
	cloudprovider.NodeGroup
	inner cloudprovider.NodeGroup
}

func (w *wrappingNodeGroup) Unwrap() cloudprovider.NodeGroup {
	return w.inner
}

func TestIncreaseSizeOrFallback(t *testing.T) {
	ctx := context.Background()

	t.Run("nil node group returns error", func(t *testing.T) {
		instances, err := experimental.IncreaseSizeOrFallback(ctx, nil, 1)
		assert.Error(t, err)
		assert.Nil(t, instances)
	})

	t.Run("InstanceCreator implemented and succeeds", func(t *testing.T) {
		want := []cloudprovider.Instance{
			{Id: "inst-1", Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceCreating}},
			{Id: "inst-2", Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceCreating}},
		}
		ng := &expmocks.InstanceCreatingNodeGroup{}
		ng.On("IncreaseSizeWithInstances", 2).Return(want, nil).Once()

		got, err := experimental.IncreaseSizeOrFallback(ctx, ng, 2)
		require.NoError(t, err)
		assert.Equal(t, want, got)
		ng.AssertNotCalled(t, "IncreaseSize", 2)
		ng.AssertExpectations(t)
	})

	t.Run("InstanceCreator not implemented falls back to IncreaseSize", func(t *testing.T) {
		ng := &cpmocks.NodeGroup{}
		ng.On("IncreaseSize", 3).Return(nil).Once()

		got, err := experimental.IncreaseSizeOrFallback(ctx, ng, 3)
		require.NoError(t, err)
		assert.Nil(t, got)
		ng.AssertExpectations(t)
	})

	t.Run("InstanceCreator not implemented propagates IncreaseSize error", func(t *testing.T) {
		wantErr := errors.New("v1 scale-up failed")
		ng := &cpmocks.NodeGroup{}
		ng.On("IncreaseSize", 2).Return(wantErr).Once()

		got, err := experimental.IncreaseSizeOrFallback(ctx, ng, 2)
		assert.ErrorIs(t, err, wantErr)
		assert.Nil(t, got)
		ng.AssertExpectations(t)
	})

	t.Run("dynamic ErrNotImplemented falls back to IncreaseSize", func(t *testing.T) {
		ng := &expmocks.InstanceCreatingNodeGroup{}
		ng.On("IncreaseSizeWithInstances", 2).Return(nil, cloudprovider.ErrNotImplemented).Once()
		ng.On("IncreaseSize", 2).Return(nil).Once()

		got, err := experimental.IncreaseSizeOrFallback(ctx, ng, 2)
		require.NoError(t, err)
		assert.Nil(t, got)
		ng.AssertExpectations(t)
	})

	t.Run("wrapped ErrNotImplemented falls back to IncreaseSize", func(t *testing.T) {
		wrappedErr := fmt.Errorf("pool opt-out: %w", cloudprovider.ErrNotImplemented)
		ng := &expmocks.InstanceCreatingNodeGroup{}
		ng.On("IncreaseSizeWithInstances", 1).Return(nil, wrappedErr).Once()
		ng.On("IncreaseSize", 1).Return(nil).Once()

		got, err := experimental.IncreaseSizeOrFallback(ctx, ng, 1)
		require.NoError(t, err)
		assert.Nil(t, got)
		ng.AssertExpectations(t)
	})

	t.Run("real error propagates without falling back to IncreaseSize", func(t *testing.T) {
		wantErr := errors.New("quota exceeded")
		ng := &expmocks.InstanceCreatingNodeGroup{}
		ng.On("IncreaseSizeWithInstances", 3).Return(nil, wantErr).Once()

		got, err := experimental.IncreaseSizeOrFallback(ctx, ng, 3)
		assert.ErrorIs(t, err, wantErr)
		assert.Nil(t, got)
		ng.AssertNotCalled(t, "IncreaseSize", 3)
		ng.AssertExpectations(t)
	})

	t.Run("partial failure returns created instances and error without falling back", func(t *testing.T) {
		partial := []cloudprovider.Instance{{Id: "inst-1"}}
		wantErr := errors.New("stockout on second instance")
		ng := &expmocks.InstanceCreatingNodeGroup{}
		ng.On("IncreaseSizeWithInstances", 2).Return(partial, wantErr).Once()

		got, err := experimental.IncreaseSizeOrFallback(ctx, ng, 2)
		assert.ErrorIs(t, err, wantErr)
		assert.Equal(t, partial, got)
		ng.AssertNotCalled(t, "IncreaseSize", 2)
		ng.AssertExpectations(t)
	})

	t.Run("unwraps NodeGroup wrapper to discover InstanceCreator", func(t *testing.T) {
		want := []cloudprovider.Instance{{Id: "wrapped-inst-1"}}
		inner := &expmocks.InstanceCreatingNodeGroup{}
		inner.On("IncreaseSizeWithInstances", 1).Return(want, nil).Once()
		wrapped := &wrappingNodeGroup{NodeGroup: inner, inner: inner}

		got, err := experimental.IncreaseSizeOrFallback(ctx, wrapped, 1)
		require.NoError(t, err)
		assert.Equal(t, want, got)
		inner.AssertNotCalled(t, "IncreaseSize", 1)
		inner.AssertExpectations(t)
	})
}
