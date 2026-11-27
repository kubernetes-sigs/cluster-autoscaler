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

import "reflect"

// NodeGroupUnwrapper is implemented by NodeGroup wrappers/decorators so that
// optional extension interfaces on the underlying NodeGroup remain discoverable
// via As.
type NodeGroupUnwrapper interface {
	Unwrap() NodeGroup
}

// CloudProviderUnwrapper is implemented by CloudProvider wrappers/decorators so
// that optional extension interfaces on the underlying CloudProvider remain
// discoverable via As.
type CloudProviderUnwrapper interface {
	Unwrap() CloudProvider
}

const maxUnwrapDepth = 64

// As finds the first value in target's unwrap chain (starting with target itself)
// that implements T and returns it.
//
// Wrappers around NodeGroup or CloudProvider should implement NodeGroupUnwrapper
// or CloudProviderUnwrapper (or interface{ Unwrap() any }) so extension interfaces
// remain discoverable across decorator layers.
//
// As returns (zero, false) if target is nil (including typed nil pointers), no
// value in the unwrap chain implements T, or the unwrap chain forms a cycle or
// exceeds maxUnwrapDepth.
func As[T any](target any) (T, bool) {
	var zero T
	if isNilValue(target) {
		return zero, false
	}

	var visited map[any]struct{}
	cur := target
	for range maxUnwrapDepth {
		if isNilValue(cur) {
			return zero, false
		}
		if v, ok := cur.(T); ok {
			return v, true
		}

		next, ok := unwrapOnce(cur)
		if !ok {
			return zero, false
		}

		// Guard against unwrap cycles for comparable values (such as pointers).
		if rt := reflect.TypeOf(cur); rt != nil && rt.Comparable() {
			if visited == nil {
				visited = make(map[any]struct{})
			}
			if _, seen := visited[cur]; seen {
				return zero, false
			}
			visited[cur] = struct{}{}
		}
		cur = next
	}
	return zero, false
}

func unwrapOnce(v any) (any, bool) {
	switch u := v.(type) {
	case NodeGroupUnwrapper:
		return u.Unwrap(), true
	case CloudProviderUnwrapper:
		return u.Unwrap(), true
	case interface{ Unwrap() any }:
		return u.Unwrap(), true
	default:
		return nil, false
	}
}

func isNilValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return rv.IsNil()
	default:
		return false
	}
}
