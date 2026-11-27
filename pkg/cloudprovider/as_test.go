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

package cloudprovider

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testExt interface {
	ExtID() string
}

type stubNodeGroup struct {
	NodeGroup
	id string
}

type extNodeGroup struct {
	NodeGroup
	id string
}

func (e *extNodeGroup) ExtID() string { return e.id }

type wrappingNodeGroup struct {
	NodeGroup
	inner NodeGroup
}

func (w *wrappingNodeGroup) Unwrap() NodeGroup { return w.inner }

type extWrappingNodeGroup struct {
	NodeGroup
	id    string
	inner NodeGroup
}

func (w *extWrappingNodeGroup) ExtID() string     { return w.id }
func (w *extWrappingNodeGroup) Unwrap() NodeGroup { return w.inner }

type extCloudProvider struct {
	CloudProvider
	id string
}

func (e *extCloudProvider) ExtID() string { return e.id }

type wrappingCloudProvider struct {
	CloudProvider
	inner CloudProvider
}

func (w *wrappingCloudProvider) Unwrap() CloudProvider { return w.inner }

type anyWrapper struct {
	inner any
}

func (w *anyWrapper) Unwrap() any { return w.inner }

type nonComparableWrapper struct {
	NodeGroup
	tags  []string // makes the struct non-comparable
	inner NodeGroup
}

func (w nonComparableWrapper) Unwrap() NodeGroup { return w.inner }

func TestAs(t *testing.T) {
	t.Run("nil target returns false", func(t *testing.T) {
		got, ok := As[testExt](nil)
		assert.False(t, ok)
		assert.Nil(t, got)
	})

	t.Run("typed nil target returns false", func(t *testing.T) {
		var ng *extNodeGroup
		var iface NodeGroup = ng
		got, ok := As[testExt](iface)
		assert.False(t, ok)
		assert.Nil(t, got)
	})

	t.Run("direct match", func(t *testing.T) {
		var ng NodeGroup = &extNodeGroup{id: "direct"}
		got, ok := As[testExt](ng)
		require.True(t, ok)
		assert.Equal(t, "direct", got.ExtID())
	})

	t.Run("no match returns false", func(t *testing.T) {
		var ng NodeGroup = &stubNodeGroup{id: "plain"}
		got, ok := As[testExt](ng)
		assert.False(t, ok)
		assert.Nil(t, got)
	})

	t.Run("unwraps NodeGroupUnwrapper chain", func(t *testing.T) {
		inner := &extNodeGroup{id: "inner-ng"}
		wrapped := &wrappingNodeGroup{inner: &wrappingNodeGroup{inner: inner}}

		got, ok := As[testExt](wrapped)
		require.True(t, ok)
		assert.Equal(t, "inner-ng", got.ExtID())
	})

	t.Run("outermost match wins over inner", func(t *testing.T) {
		inner := &extNodeGroup{id: "inner"}
		outer := &extWrappingNodeGroup{id: "outer", inner: inner}

		got, ok := As[testExt](outer)
		require.True(t, ok)
		assert.Equal(t, "outer", got.ExtID())
	})

	t.Run("unwraps CloudProviderUnwrapper chain", func(t *testing.T) {
		inner := &extCloudProvider{id: "inner-cp"}
		wrapped := &wrappingCloudProvider{inner: inner}

		got, ok := As[testExt](wrapped)
		require.True(t, ok)
		assert.Equal(t, "inner-cp", got.ExtID())
	})

	t.Run("unwraps generic Unwrap() any", func(t *testing.T) {
		inner := &extNodeGroup{id: "via-any"}
		wrapped := &anyWrapper{inner: inner}

		got, ok := As[testExt](wrapped)
		require.True(t, ok)
		assert.Equal(t, "via-any", got.ExtID())
	})

	t.Run("wrapper returning nil terminates cleanly", func(t *testing.T) {
		wrapped := &wrappingNodeGroup{inner: nil}
		got, ok := As[testExt](wrapped)
		assert.False(t, ok)
		assert.Nil(t, got)
	})

	t.Run("wrapper returning typed nil terminates cleanly", func(t *testing.T) {
		var typedNil *extNodeGroup
		wrapped := &wrappingNodeGroup{inner: typedNil}
		got, ok := As[testExt](wrapped)
		assert.False(t, ok)
		assert.Nil(t, got)
	})

	t.Run("non-comparable wrapper does not panic", func(t *testing.T) {
		inner := &extNodeGroup{id: "under-non-comparable"}
		wrapped := nonComparableWrapper{tags: []string{"a", "b"}, inner: inner}

		got, ok := As[testExt](wrapped)
		require.True(t, ok)
		assert.Equal(t, "under-non-comparable", got.ExtID())
	})

	t.Run("self-cycle terminates cleanly", func(t *testing.T) {
		w := &wrappingNodeGroup{}
		w.inner = w

		got, ok := As[testExt](w)
		assert.False(t, ok)
		assert.Nil(t, got)
	})

	t.Run("multi-node cycle terminates cleanly", func(t *testing.T) {
		a := &wrappingNodeGroup{}
		b := &wrappingNodeGroup{inner: a}
		a.inner = b

		got, ok := As[testExt](a)
		assert.False(t, ok)
		assert.Nil(t, got)
	})

	t.Run("excessive depth terminates cleanly", func(t *testing.T) {
		var cur NodeGroup = &extNodeGroup{id: "too-deep"}
		for range maxUnwrapDepth + 5 {
			cur = &wrappingNodeGroup{inner: cur}
		}

		got, ok := As[testExt](cur)
		assert.False(t, ok)
		assert.Nil(t, got)
	})
}
