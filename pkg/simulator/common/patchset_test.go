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

package common

import (
	"fmt"
	"maps"
	"slices"
	"testing"

	"k8s.io/utils/ptr"
)

func TestPatchSetAsMap(t *testing.T) {
	tests := map[string]struct {
		patchLayers []map[string]*int
		wantMap     map[string]int
	}{
		"EmptyPatchSet": {
			patchLayers: []map[string]*int{},
			wantMap:     map[string]int{},
		},
		"SingleLayerNoDeletions": {
			patchLayers: []map[string]*int{{"a": ptr.To(1), "b": ptr.To(2)}},
			wantMap:     map[string]int{"a": 1, "b": 2},
		},
		"SingleLayerOnlyDeletions": {
			patchLayers: []map[string]*int{{"a": nil, "b": nil}},
			wantMap:     map[string]int{},
		},
		"SingleLayerModificationsAndDeletions": {
			patchLayers: []map[string]*int{
				{"a": ptr.To(1), "b": nil, "c": ptr.To(3)}},
			wantMap: map[string]int{"a": 1, "c": 3},
		},
		"MultipleLayersNoDeletionsOverwrite": {
			patchLayers: []map[string]*int{
				{"a": ptr.To(1), "b": ptr.To(2)},
				{"b": ptr.To(22), "c": ptr.To(3)}},
			wantMap: map[string]int{"a": 1, "b": 22, "c": 3},
		},
		"MultipleLayersWithDeletions": {
			patchLayers: []map[string]*int{
				{"a": ptr.To(1), "b": nil, "x": ptr.To(100)},
				{"c": ptr.To(3), "x": ptr.To(101), "a": nil}},
			wantMap: map[string]int{"c": 3, "x": 101},
		},
		"MultipleLayersDeletionInLowerLayer": {
			patchLayers: []map[string]*int{
				{"a": nil, "b": ptr.To(2)},
				{"c": ptr.To(3)}},
			wantMap: map[string]int{"b": 2, "c": 3},
		},
		"MultipleLayersAddAfterDelete": {
			patchLayers: []map[string]*int{
				{"a": nil},
				{"a": ptr.To(11), "b": ptr.To(2)}},
			wantMap: map[string]int{"a": 11, "b": 2},
		},
		"MultipleLayersDeletePreviouslyAdded": {
			patchLayers: []map[string]*int{
				{"a": ptr.To(1), "b": ptr.To(2)},
				{"c": ptr.To(3), "b": nil}},
			wantMap: map[string]int{"a": 1, "c": 3},
		},
		"AllInOne": {
			patchLayers: []map[string]*int{
				{"k1": ptr.To(1), "k2": ptr.To(2)},
				{"k2": ptr.To(22), "k3": ptr.To(3), "k1": nil},
				{"k1": ptr.To(111), "k3": ptr.To(33), "k4": ptr.To(4), "deleted": nil},
			},
			wantMap: map[string]int{"k2": 22, "k3": 33, "k4": 4, "k1": 111},
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			patchset := buildTestPatchSet(t, test.patchLayers)
			mergedMap := patchset.AsMap()
			if !maps.Equal(mergedMap, test.wantMap) {
				t.Errorf("AsMap() result mismatch: got %v, want %v", mergedMap, test.wantMap)
			}
		})
	}
}

func TestPatchSetCommit(t *testing.T) {
	tests := map[string]struct {
		patchLayers []map[string]*int
		wantMap     map[string]int // Expected map after each commit
	}{
		"CommitEmptyPatchSet": {
			patchLayers: []map[string]*int{},
			wantMap:     map[string]int{},
		},
		"CommitSingleLayer": {
			patchLayers: []map[string]*int{{"a": ptr.To(1)}},
			wantMap:     map[string]int{"a": 1},
		},
		"CommitTwoLayersNoOverlap": {
			patchLayers: []map[string]*int{{"a": ptr.To(1)}, {"b": ptr.To(2)}},
			wantMap:     map[string]int{"a": 1, "b": 2},
		},
		"CommitTwoLayersWithOverwrite": {
			patchLayers: []map[string]*int{{"a": ptr.To(1)}, {"a": ptr.To(2)}},
			wantMap:     map[string]int{"a": 2},
		},
		"CommitTwoLayersWithDeleteInTopLayer": {
			patchLayers: []map[string]*int{{"a": ptr.To(1), "b": ptr.To(5)}, {"c": ptr.To(3), "a": nil}},
			wantMap:     map[string]int{"b": 5, "c": 3},
		},
		"CommitTwoLayersWithDeleteInBottomLayer": {
			patchLayers: []map[string]*int{{"a": ptr.To(1), "b": nil}, {"a": ptr.To(11), "c": ptr.To(3)}},
			wantMap:     map[string]int{"a": 11, "c": 3},
		},
		"CommitThreeLayers": {
			patchLayers: []map[string]*int{
				{"a": ptr.To(1), "b": ptr.To(2), "z": ptr.To(100)},
				{"b": ptr.To(22), "c": ptr.To(3), "a": nil},
				{"c": ptr.To(33), "d": ptr.To(4), "a": ptr.To(111), "b": nil, "deleted": nil},
			},
			wantMap: map[string]int{"z": 100, "c": 33, "d": 4, "a": 111},
		},
		"CommitMultipleLayersDeleteAndAddBack": {
			patchLayers: []map[string]*int{
				{"x": ptr.To(10)},
				{"x": nil},
				{"x": ptr.To(30)},
				{"y": ptr.To(40), "x": nil}},
			wantMap: map[string]int{"y": 40},
		},
		"CommitEmptyLayersBetweenDataLayers": {
			patchLayers: []map[string]*int{{"a": ptr.To(1)}, {}, {"b": ptr.To(2)}, {}, {"c": ptr.To(3)}},
			wantMap:     map[string]int{"a": 1, "b": 2, "c": 3},
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			ps := buildTestPatchSet(t, tc.patchLayers)
			initialNumPatches := ps.depth()

			if currentMap := ps.AsMap(); !maps.Equal(currentMap, tc.wantMap) {
				t.Errorf("AsMap() before any commits mismatch: got %v, want %v", currentMap, tc.wantMap)
			}

			for i := 0; i < initialNumPatches-1; i++ {
				expectedPatchesAfterCommit := ps.depth() - 1
				ps.Commit()
				if ps.depth() != expectedPatchesAfterCommit {
					t.Errorf("After commit #%d, expected %d patches, got %d", i+1, expectedPatchesAfterCommit, ps.depth())
				}
				if currentMap := ps.AsMap(); !maps.Equal(currentMap, tc.wantMap) {
					t.Errorf("AsMap() after commit #%d mismatch: got %v, want %v", i+1, currentMap, tc.wantMap)
				}
			}

			if initialNumPatches > 0 && ps.depth() != 1 {
				t.Errorf("Expected 1 patch after all commits, got %d", ps.depth())
			} else if initialNumPatches == 0 && ps.depth() != 0 {
				t.Errorf("Expected 0 patches after all commits, got %d", ps.depth())
			}
		})
	}
}

func TestPatchSetRevert(t *testing.T) {
	tests := map[string]struct {
		patchLayers         []map[string]*int
		wantInitialMap      map[string]int
		wantMapsAfterRevert []map[string]int
	}{
		"RevertEmptyPatchSet": {
			patchLayers:         []map[string]*int{},
			wantInitialMap:      map[string]int{},
			wantMapsAfterRevert: []map[string]int{{}},
		},
		"RevertSingleLayer": {
			patchLayers:         []map[string]*int{{"a": ptr.To(1)}},
			wantInitialMap:      map[string]int{"a": 1},
			wantMapsAfterRevert: []map[string]int{{"a": 1}},
		},
		"RevertTwoLayersNoOverlap": {
			patchLayers:         []map[string]*int{{"a": ptr.To(1)}, {"b": ptr.To(2)}},
			wantInitialMap:      map[string]int{"a": 1, "b": 2},
			wantMapsAfterRevert: []map[string]int{{"a": 1}, {"a": 1}},
		},
		"RevertTwoLayersWithOverwrite": {
			patchLayers:         []map[string]*int{{"a": ptr.To(1)}, {"a": ptr.To(2)}},
			wantInitialMap:      map[string]int{"a": 2},
			wantMapsAfterRevert: []map[string]int{{"a": 1}, {"a": 1}},
		},
		"RevertTwoLayersWithDeleteInTopLayer": {
			patchLayers:         []map[string]*int{{"a": ptr.To(1), "b": ptr.To(5)}, {"c": ptr.To(3), "a": nil}},
			wantInitialMap:      map[string]int{"b": 5, "c": 3},
			wantMapsAfterRevert: []map[string]int{{"a": 1, "b": 5}, {"a": 1, "b": 5}},
		},
		"RevertThreeLayers": {
			patchLayers: []map[string]*int{
				{"a": ptr.To(1), "b": ptr.To(2), "z": ptr.To(100)},
				{"b": ptr.To(22), "c": ptr.To(3), "a": nil},
				{"c": ptr.To(33), "d": ptr.To(4), "a": ptr.To(111), "b": nil, "deleted": nil},
			},
			wantInitialMap: map[string]int{"z": 100, "c": 33, "d": 4, "a": 111},
			wantMapsAfterRevert: []map[string]int{
				{"a": 1, "b": 2, "z": 100},
				{"a": 1, "b": 2, "z": 100},
				{"z": 100, "b": 22, "c": 3},
			},
		},
		"RevertMultipleLayersDeleteAndAddBack": {
			patchLayers:    []map[string]*int{{"x": ptr.To(10)}, {"x": nil}, {"x": ptr.To(30)}, {"y": ptr.To(40), "x": nil}},
			wantInitialMap: map[string]int{"y": 40},
			wantMapsAfterRevert: []map[string]int{
				{"x": 10},
				{"x": 10},
				{},
				{"x": 30},
			},
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			ps := buildTestPatchSet(t, tc.patchLayers)
			patchesNumber := ps.depth()

			if currentMap := ps.AsMap(); !maps.Equal(currentMap, tc.wantInitialMap) {
				t.Errorf("AsMap() before any reverts mismatch: got %v, want %v", currentMap, tc.wantInitialMap)
			}

			for i := 0; i <= patchesNumber; i++ {
				wantPatchesAfterRevert := ps.depth() - 1
				ps.Revert()
				if ps.depth() != wantPatchesAfterRevert && ps.depth() > 1 {
					t.Errorf("After revert #%d, expected %d patches, got %d", i+1, wantPatchesAfterRevert, ps.depth())
				}

				currentMap := ps.AsMap()
				wantMapIndex := patchesNumber - i - 1
				if wantMapIndex < 0 {
					wantMapIndex = 0
				}
				wantMapAfterRevert := tc.wantMapsAfterRevert[wantMapIndex]
				if !maps.Equal(currentMap, wantMapAfterRevert) {
					t.Errorf("AsMap() after revert #%d mismatch: got %v, want %v", i+1, currentMap, wantMapAfterRevert)
				}
			}

			if patchesNumber >= 1 && ps.depth() != 1 {
				t.Errorf("Expected 1 patch after all reverts, got %d", ps.depth())
			} else if patchesNumber == 0 && ps.depth() != 0 {
				t.Errorf("Expected 0 patches after all reverts, got %d", ps.depth())
			}
		})
	}
}

func TestPatchSetForkRevert(t *testing.T) {
	// 1. Initialize empty patchset
	ps := NewPatchSet(NewPatch[string, int]())
	if initialMap := ps.AsMap(); len(initialMap) != 0 {
		t.Fatalf("Initial AsMap() got %v, want empty", initialMap)
	}

	// 2. Call Fork
	ps.Fork()
	if ps.depth() != 2 {
		t.Fatalf("Expected 2 patches after Fork(), got %d", ps.depth())
	}

	// 3. Perform some mutation operations on the new layer
	ps.SetCurrent("a", 1)
	ps.SetCurrent("b", 2)
	ps.DeleteCurrent("a")

	// 4. Call Revert
	ps.Revert()
	if ps.depth() != 1 {
		t.Fatalf("Expected 1 patch after Revert(), got %d", ps.depth())
	}

	// 5. Compare state to the empty map
	if finalMap := ps.AsMap(); len(finalMap) != 0 {
		t.Errorf("AsMap() got %v, want empty", finalMap)
	}
}

func TestPatchSetForkCommit(t *testing.T) {
	// 1. Initialize empty patchset
	ps := NewPatchSet(NewPatch[string, int]())
	if initialMap := ps.AsMap(); len(initialMap) != 0 {
		t.Fatalf("Initial AsMap() got %v, want empty", initialMap)
	}

	// 2. Call Fork two times
	ps.Fork()
	ps.Fork()
	if ps.depth() != 3 {
		t.Fatalf("Expected 3 patches after 2xFork(), got %d", ps.depth())
	}

	// 3. Perform some mutation operations on the current layer
	ps.SetCurrent("a", 1)
	ps.SetCurrent("b", 2)
	ps.DeleteCurrent("a")

	// 4. Call Commit to persist changes
	ps.Commit()
	if ps.depth() != 2 {
		t.Fatalf("Expected 1 patch after Commit(), got %d", ps.depth())
	}

	// 5. Call Revert on the empty layer
	ps.Revert()
	if ps.depth() != 1 {
		t.Fatalf("Expected 1 patch after Revert(), got %d", ps.depth())
	}

	// 6. Compare state to the empty map
	wantMap := map[string]int{"b": 2}
	if finalMap := ps.AsMap(); maps.Equal(wantMap, finalMap) {
		t.Errorf("AsMap() got %v, want %v", finalMap, wantMap)
	}
}

func TestPatchSetFindValue(t *testing.T) {
	tests := map[string]struct {
		patchLayers []map[string]*int
		searchKey   string
		wantValue   int
		wantFound   bool
	}{
		"EmptyPatchSet_KeyNotFound": {
			patchLayers: []map[string]*int{},
			searchKey:   "a",
			wantFound:   false,
		},
		"SingleLayer_KeyFound": {
			patchLayers: []map[string]*int{{"a": ptr.To(1), "b": ptr.To(2)}},
			searchKey:   "a",
			wantValue:   1,
			wantFound:   true,
		},
		"SingleLayer_KeyNotFound": {
			patchLayers: []map[string]*int{{"a": ptr.To(1)}},
			searchKey:   "x",
			wantFound:   false,
		},
		"SingleLayer_KeyDeleted": {
			patchLayers: []map[string]*int{{"a": nil, "b": ptr.To(2)}},
			searchKey:   "a",
			wantFound:   false,
		},
		"MultiLayer_KeyInTopLayer": {
			patchLayers: []map[string]*int{{"a": ptr.To(1)}, {"a": ptr.To(10), "b": ptr.To(2)}},
			searchKey:   "a",
			wantValue:   10,
			wantFound:   true,
		},
		"MultiLayer_KeyInBottomLayer": {
			patchLayers: []map[string]*int{{"a": ptr.To(1), "b": ptr.To(2)}, {"b": ptr.To(22)}},
			searchKey:   "a",
			wantValue:   1,
			wantFound:   true,
		},
		"MultiLayer_KeyOverwrittenThenDeleted": {
			patchLayers: []map[string]*int{{"a": ptr.To(1)}, {"a": ptr.To(10)}, {"a": nil, "b": ptr.To(2)}},
			searchKey:   "a",
			wantFound:   false,
		},
		"MultiLayer_KeyDeletedThenAddedBack": {
			patchLayers: []map[string]*int{{"a": ptr.To(1)}, {"a": nil}, {"a": ptr.To(100), "b": ptr.To(2)}},
			searchKey:   "a",
			wantValue:   100,
			wantFound:   true,
		},
		"MultiLayer_KeyNotFoundInAnyLayer": {
			patchLayers: []map[string]*int{{"a": ptr.To(1)}, {"b": ptr.To(2)}},
			searchKey:   "x",
			wantFound:   false,
		},
		"MultiLayer_KeyDeletedInMiddleLayer_NotFound": {
			patchLayers: []map[string]*int{{"a": ptr.To(1)}, {"a": nil, "b": ptr.To(2)}, {"c": ptr.To(3)}},
			searchKey:   "a",
			wantFound:   false,
		},
		"MultiLayer_KeyPresentInBase_DeletedInOverlay_ReAddedInTop": {
			patchLayers: []map[string]*int{
				{"k1": ptr.To(1)},
				{"k1": nil},
				{"k1": ptr.To(111)},
			},
			searchKey: "k1",
			wantValue: 111,
			wantFound: true,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			ps := buildTestPatchSet(t, tc.patchLayers)
			val, found := ps.FindValue(tc.searchKey)

			if found != tc.wantFound || (found && val != tc.wantValue) {
				t.Errorf("FindValue(%q) got val=%v, found=%v; want val=%v, found=%v", tc.searchKey, val, found, tc.wantValue, tc.wantFound)
			}
		})
	}
}

func TestPatchSetOperations(t *testing.T) {
	tests := map[string]struct {
		patchLayers    []map[string]*int
		mutatePatchSet func(ps *PatchSet[string, int])
		searchKey      string
		wantInCurrent  bool
	}{
		"SetCurrent_NewKey_InitiallyEmptyPatchSet": {
			patchLayers: []map[string]*int{},
			mutatePatchSet: func(ps *PatchSet[string, int]) {
				ps.SetCurrent("a", 1)
			},
			searchKey:     "a",
			wantInCurrent: true,
		},
		"SetCurrent_NewKey_OnExistingEmptyLayer": {
			patchLayers: []map[string]*int{{}},
			mutatePatchSet: func(ps *PatchSet[string, int]) {
				ps.SetCurrent("a", 1)
			},
			searchKey:     "a",
			wantInCurrent: true,
		},
		"SetCurrent_OverwriteKey_InCurrentPatch": {
			patchLayers: []map[string]*int{{"a": ptr.To(10)}},
			mutatePatchSet: func(ps *PatchSet[string, int]) {
				ps.SetCurrent("a", 1)
			},
			searchKey:     "a",
			wantInCurrent: true,
		},
		"SetCurrent_OverwriteKey_InLowerPatch_AfterFork": {
			patchLayers: []map[string]*int{{"a": ptr.To(10)}},
			mutatePatchSet: func(ps *PatchSet[string, int]) {
				ps.Fork()
				ps.SetCurrent("a", 1)
			},
			searchKey:     "a",
			wantInCurrent: true,
		},
		"DeleteCurrent_ExistingKey_InCurrentPatch": {
			patchLayers: []map[string]*int{{"a": ptr.To(1)}},
			mutatePatchSet: func(ps *PatchSet[string, int]) {
				ps.DeleteCurrent("a")
			},
			searchKey:     "a",
			wantInCurrent: false,
		},
		"DeleteCurrent_ExistingKey_InLowerPatch_AfterFork": {
			patchLayers: []map[string]*int{{"a": ptr.To(1)}},
			mutatePatchSet: func(ps *PatchSet[string, int]) {
				ps.Fork()
				ps.DeleteCurrent("a")
			},
			searchKey:     "a",
			wantInCurrent: false,
		},
		"DeleteCurrent_NonExistentKey_OnExistingEmptyLayer": {
			patchLayers: []map[string]*int{{}},
			mutatePatchSet: func(ps *PatchSet[string, int]) {
				ps.DeleteCurrent("x")
			},
			searchKey:     "x",
			wantInCurrent: false,
		},
		"DeleteCurrent_NonExistentKey_InitiallyEmptyPatchSet": {
			patchLayers: []map[string]*int{}, // Starts with no layers
			mutatePatchSet: func(ps *PatchSet[string, int]) {
				ps.DeleteCurrent("x")
			},
			searchKey:     "x",
			wantInCurrent: false,
		},
		"InCurrentPatch_KeySetInCurrent": {
			patchLayers:    []map[string]*int{{"a": ptr.To(1)}},
			mutatePatchSet: func(ps *PatchSet[string, int]) {},
			searchKey:      "a",
			wantInCurrent:  true,
		},
		"InCurrentPatch_KeySetInLower_NotInCurrent_AfterFork": {
			patchLayers:    []map[string]*int{{"a": ptr.To(1)}},
			mutatePatchSet: func(ps *PatchSet[string, int]) { ps.Fork() },
			searchKey:      "a",
			wantInCurrent:  false,
		},
		"InCurrentPatch_KeyDeletedInCurrent": {
			patchLayers: []map[string]*int{{"a": ptr.To(1)}},
			mutatePatchSet: func(ps *PatchSet[string, int]) {
				ps.DeleteCurrent("a")
			},
			searchKey:     "a",
			wantInCurrent: false, // Get in current patch won't find it.
		},
		"InCurrentPatch_NonExistentKey_InitiallyEmptyPatchSet": {
			patchLayers:    []map[string]*int{},
			mutatePatchSet: func(ps *PatchSet[string, int]) {},
			searchKey:      "a",
			wantInCurrent:  false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ps := buildTestPatchSet(t, tc.patchLayers)
			tc.mutatePatchSet(ps)

			inCurrent := ps.InCurrentPatch(tc.searchKey)
			if inCurrent != tc.wantInCurrent {
				t.Errorf("InCurrentPatch(%q): got %v, want %v", tc.searchKey, inCurrent, tc.wantInCurrent)
			}
		})
	}
}

func buildTestPatchSet[K comparable, V any](t *testing.T, patchLayers []map[K]*V) *PatchSet[K, V] {
	t.Helper()

	patchesNumber := len(patchLayers)
	patches := make([]*Patch[K, V], patchesNumber)
	for i := 0; i < patchesNumber; i++ {
		layerMap := patchLayers[i]
		currentPatch := NewPatch[K, V]()
		for k, vPtr := range layerMap {
			if vPtr != nil {
				currentPatch.Set(k, *vPtr)
			} else {
				currentPatch.Delete(k)
			}
		}
		patches[i] = currentPatch
	}

	return NewPatchSet(patches...)
}

func TestPatchSetValuesSliceAndLen(t *testing.T) {
	tests := map[string]struct {
		patchLayers    []map[string]*int
		mutatePatchSet func(ps *PatchSet[string, int])
		wantValues     []int
	}{
		"Empty_NoLayers": {
			patchLayers:    []map[string]*int{},
			mutatePatchSet: func(ps *PatchSet[string, int]) {},
			wantValues:     []int{},
		},
		"Empty_SingleEmptyLayer": {
			patchLayers:    []map[string]*int{{}},
			mutatePatchSet: func(ps *PatchSet[string, int]) {},
			wantValues:     []int{},
		},
		"Populated_MultipleLayers": {
			patchLayers:    []map[string]*int{{"a": ptr.To(1), "b": ptr.To(2)}, {"b": nil, "c": ptr.To(3)}, {"a": ptr.To(11)}},
			mutatePatchSet: func(ps *PatchSet[string, int]) {},
			wantValues:     []int{3, 11},
		},
		"Populated_AfterSetAndDeleteInFork": {
			patchLayers: []map[string]*int{{"a": ptr.To(1), "b": ptr.To(2)}},
			mutatePatchSet: func(ps *PatchSet[string, int]) {
				ps.AsMap()
				ps.Fork()
				ps.SetCurrent("c", 3)
				ps.DeleteCurrent("a")
			},
			wantValues: []int{2, 3},
		},
		"Reverted_AfterAsMap": {
			patchLayers: []map[string]*int{{"a": ptr.To(1), "b": ptr.To(2)}},
			mutatePatchSet: func(ps *PatchSet[string, int]) {
				ps.AsMap()
				ps.Fork()
				ps.SetCurrent("c", 3)
				ps.DeleteCurrent("a")
				ps.Revert()
			},
			wantValues: []int{1, 2},
		},
		"Reverted_AfterFindValue": {
			patchLayers: []map[string]*int{{"a": ptr.To(1), "b": ptr.To(2)}, {"a": nil, "c": ptr.To(3)}},
			mutatePatchSet: func(ps *PatchSet[string, int]) {
				ps.FindValue("a")
				ps.FindValue("c")
				ps.Revert()
			},
			wantValues: []int{1, 2},
		},
		"AfterFindValueMisses": {
			patchLayers: []map[string]*int{{"a": ptr.To(1), "b": nil}},
			mutatePatchSet: func(ps *PatchSet[string, int]) {
				ps.FindValue("b")
				ps.FindValue("missing")
			},
			wantValues: []int{1},
		},
		"AfterFindValueMissAndAsMap": {
			patchLayers: []map[string]*int{{"a": ptr.To(1)}},
			mutatePatchSet: func(ps *PatchSet[string, int]) {
				ps.AsMap()
				ps.FindValue("missing")
			},
			wantValues: []int{1},
		},
		"AfterDeleteCurrentInFork": {
			patchLayers: []map[string]*int{{"a": ptr.To(1), "b": ptr.To(2)}},
			mutatePatchSet: func(ps *PatchSet[string, int]) {
				ps.Fork()
				ps.DeleteCurrent("a")
				ps.DeleteCurrent("missing")
			},
			wantValues: []int{2},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ps := buildTestPatchSet(t, tc.patchLayers)
			tc.mutatePatchSet(ps)

			if got := ps.Len(); got != len(tc.wantValues) {
				t.Errorf("Len() got %d, want %d", got, len(tc.wantValues))
			}
			gotValues := ps.ValuesSlice()
			if gotValues == nil {
				t.Errorf("ValuesSlice() got nil, want non-nil slice")
			}
			slices.Sort(gotValues)
			if !slices.Equal(gotValues, tc.wantValues) {
				t.Errorf("ValuesSlice() got %v, want %v", gotValues, tc.wantValues)
			}
			if iterValues := slices.Sorted(ps.Values()); !slices.Equal(iterValues, tc.wantValues) {
				t.Errorf("Values() got %v, want %v", iterValues, tc.wantValues)
			}
		})
	}
}

func TestPatchSetValuesEarlyBreak(t *testing.T) {
	ps := buildTestPatchSet(t, []map[string]*int{{"a": ptr.To(1), "b": ptr.To(2), "c": ptr.To(3)}})
	count := 0
	for range ps.Values() {
		count++
		if count == 2 {
			break
		}
	}
	if count != 2 {
		t.Errorf("Values() early break: got %d iterations, want 2", count)
	}
}

// depth returns the number of layers, including the base layer.
func (p *PatchSet[K, V]) depth() int {
	return len(p.patches)
}

func TestPatchSetCommitThenRevert(t *testing.T) {
	// The base layer has a=1, b=2, c=3. Both forked layers modify and delete
	// overlapping keys. After committing layer 2 into layer 1, the effective
	// state must not change, and reverting the merged layer must restore the
	// base state.
	tests := map[string]struct {
		layer1 func(ps *PatchSet[string, int])
		layer2 func(ps *PatchSet[string, int])
	}{
		"LowerLayerTouchesMoreKeys": {
			layer1: func(ps *PatchSet[string, int]) {
				ps.SetCurrent("a", 10)
				ps.SetCurrent("b", 20)
				ps.SetCurrent("x", 100)
				ps.DeleteCurrent("y")
			},
			layer2: func(ps *PatchSet[string, int]) {
				ps.DeleteCurrent("b")
				ps.SetCurrent("d", 4)
			},
		},
		"UpperLayerTouchesMoreKeys": {
			layer1: func(ps *PatchSet[string, int]) {
				ps.SetCurrent("a", 10)
			},
			layer2: func(ps *PatchSet[string, int]) {
				ps.SetCurrent("a", 11)
				ps.DeleteCurrent("b")
				ps.SetCurrent("c", 30)
				ps.SetCurrent("d", 4)
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ps := NewPatchSet(NewPatchFromMap(map[string]int{"a": 1, "b": 2, "c": 3}))
			ps.Fork()
			tc.layer1(ps)
			ps.Fork()
			tc.layer2(ps)
			wantCommitted := ps.AsMap()

			ps.Commit()
			if got := ps.AsMap(); !maps.Equal(got, wantCommitted) {
				t.Errorf("AsMap() after Commit() got %v, want %v", got, wantCommitted)
			}
			if ps.depth() != 2 {
				t.Errorf("depth() after Commit() got %d, want 2", ps.depth())
			}

			ps.Revert()
			if got, want := ps.AsMap(), map[string]int{"a": 1, "b": 2, "c": 3}; !maps.Equal(got, want) {
				t.Errorf("AsMap() after Revert() got %v, want %v", got, want)
			}
			if ps.depth() != 1 {
				t.Errorf("depth() after Revert() got %d, want 1", ps.depth())
			}
		})
	}
}

func TestPatchSetNestedForkRevert(t *testing.T) {
	ps := buildTestPatchSet(t, []map[string]*int{{"a": ptr.To(1), "b": ptr.To(2), "c": ptr.To(3)}})

	assertState := func(step string, want map[string]int) {
		t.Helper()
		if got := ps.AsMap(); !maps.Equal(got, want) {
			t.Errorf("%s: AsMap() got %v, want %v", step, got, want)
		}
		if got := ps.Len(); got != len(want) {
			t.Errorf("%s: Len() got %d, want %d", step, got, len(want))
		}
		for _, key := range []string{"a", "b", "c", "d", "e", "missing"} {
			wantValue, wantFound := want[key]
			if gotValue, gotFound := ps.FindValue(key); gotFound != wantFound || gotValue != wantValue {
				t.Errorf("%s: FindValue(%q) got (%v, %v), want (%v, %v)", step, key, gotValue, gotFound, wantValue, wantFound)
			}
		}
	}

	assertState("initial", map[string]int{"a": 1, "b": 2, "c": 3})

	ps.Fork()
	ps.SetCurrent("a", 10)
	ps.SetCurrent("d", 4)
	assertState("fork#1", map[string]int{"a": 10, "b": 2, "c": 3, "d": 4})

	ps.Fork()
	ps.SetCurrent("b", 20)
	ps.DeleteCurrent("a")
	ps.DeleteCurrent("c")
	ps.SetCurrent("e", 5)
	assertState("fork#2", map[string]int{"b": 20, "d": 4, "e": 5})

	ps.Revert()
	assertState("revert fork#2", map[string]int{"a": 10, "b": 2, "c": 3, "d": 4})

	ps.Fork()
	ps.DeleteCurrent("d")
	ps.SetCurrent("c", 30)
	ps.Commit()
	assertState("commit fork#2'", map[string]int{"a": 10, "b": 2, "c": 30})

	ps.Revert()
	assertState("revert fork#1", map[string]int{"a": 1, "b": 2, "c": 3})

	// Reverting the base layer is a no-op.
	ps.Revert()
	assertState("revert base", map[string]int{"a": 1, "b": 2, "c": 3})

	// Deep fork stack with the same key touched at every level:
	// layers: a=100, del(a), a=102, del(a), a=104.
	for i := 0; i < 5; i++ {
		ps.Fork()
		if i%2 == 0 {
			ps.SetCurrent("a", 100+i)
		} else {
			ps.DeleteCurrent("a")
		}
	}
	assertState("deep fork", map[string]int{"a": 104, "b": 2, "c": 3})
	for i, want := range []map[string]int{
		{"b": 2, "c": 3},
		{"a": 102, "b": 2, "c": 3},
		{"b": 2, "c": 3},
		{"a": 100, "b": 2, "c": 3},
		{"a": 1, "b": 2, "c": 3},
	} {
		ps.Revert()
		assertState(fmt.Sprintf("deep revert#%d", i+1), want)
	}
}

// TestPatchSetAllocs guards against heap allocations in the O(1) operations.
func TestPatchSetAllocs(t *testing.T) {
	ps := NewPatchSet(NewPatchFromMap(map[string][]int{"a": {1}, "b": {2}}))
	for range 50 {
		ps.Fork()
	}
	ps.SetCurrent("a", []int{3})
	for name, run := range map[string]func(){
		"FindValue hit":                      func() { ps.FindValue("a") },
		"FindValue miss":                     func() { ps.FindValue("missing") },
		"FindValue of a base layer key":      func() { ps.FindValue("b") },
		"SetCurrent of a key touched before": func() { ps.SetCurrent("a", nil) },
		"InCurrentPatch":                     func() { ps.InCurrentPatch("a") },
		"Len":                                func() { ps.Len() },
	} {
		if allocs := testing.AllocsPerRun(100, run); allocs > 0 {
			t.Errorf("%s: got %v allocs, want 0", name, allocs)
		}
	}
}

func TestClonePatchSet(t *testing.T) {
	ps := NewPatchSet(NewPatchFromMap(map[string]*int{"a": ptr.To(1)}))
	ps.Fork()
	ps.SetCurrent("a", ptr.To(2))
	ps.SetCurrent("b", ptr.To(3))
	ps.DeleteCurrent("missing")

	cloneValue := func(v *int) *int { return ptr.To(*v) }
	cloned := ClonePatchSet(ps, func(k string) string { return k }, cloneValue)
	if got, want := *cloned.AsMap()["a"], 2; got != want {
		t.Errorf("cloned a got %d, want %d", got, want)
	}
	if cloned.effective["a"] == ps.effective["a"] {
		t.Errorf("cloned value shares the pointer with the original")
	}

	cloned.Revert()
	if got := cloned.AsMap(); len(got) != 1 || *got["a"] != 1 {
		t.Errorf("cloned AsMap() after Revert() got %v, want {a: 1}", got)
	}
	if got := ps.AsMap(); len(got) != 2 || *got["a"] != 2 || *got["b"] != 3 {
		t.Errorf("original AsMap() changed after reverting the clone: got %v", got)
	}
	if ClonePatchSet[string, int](nil, nil, nil) != nil {
		t.Errorf("ClonePatchSet(nil) got non-nil")
	}
}

var benchSizes = []int{1000, 10000}

func newBenchPatchSet(n int) *PatchSet[int, *int] {
	base := make(map[int]*int, n)
	for i := range n {
		base[i] = &i
	}
	ps := NewPatchSet(NewPatchFromMap(base))
	ps.AsMap() // sync the cache, as happens after the first List in a loop
	return ps
}

func listViaAsMap(ps *PatchSet[int, *int]) []*int {
	m := ps.AsMap()
	res := make([]*int, 0, len(m))
	for _, v := range m {
		res = append(res, v)
	}
	return res
}

var benchSink []*int
var benchFoundSink bool

// BenchmarkPatchSetRevert measures a typical simulation cycle:
// Fork -> modify a handful of keys -> Revert -> (optionally) list all values.
func BenchmarkPatchSetRevert(b *testing.B) {
	modify := func(ps *PatchSet[int, *int], i, n int) {
		ps.Fork()
		for k := range 5 {
			ps.SetCurrent((i+k)%n, &k)
		}
		ps.DeleteCurrent((i + 7) % n)
	}
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("N=%d/ForkModifyRevert", n), func(b *testing.B) {
			ps := newBenchPatchSet(n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				modify(ps, i, n)
				ps.Revert()
			}
		})
		b.Run(fmt.Sprintf("N=%d/ForkModifyRevertAsMapList", n), func(b *testing.B) {
			ps := newBenchPatchSet(n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				modify(ps, i, n)
				ps.Revert()
				benchSink = listViaAsMap(ps)
			}
		})
		b.Run(fmt.Sprintf("N=%d/ForkModifyRevertValuesSlice", n), func(b *testing.B) {
			ps := newBenchPatchSet(n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				modify(ps, i, n)
				ps.Revert()
				benchSink = ps.ValuesSlice()
			}
		})
	}
}

// BenchmarkPatchSetValuesSlice compares listing values through AsMap() vs ValuesSlice() / Values().
func BenchmarkPatchSetValuesSlice(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("N=%d/AsMapToSlice", n), func(b *testing.B) {
			ps := newBenchPatchSet(n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchSink = listViaAsMap(ps)
			}
		})
		b.Run(fmt.Sprintf("N=%d/ValuesSlice", n), func(b *testing.B) {
			ps := newBenchPatchSet(n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchSink = ps.ValuesSlice()
			}
		})
		b.Run(fmt.Sprintf("N=%d/ValuesIter", n), func(b *testing.B) {
			ps := newBenchPatchSet(n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				count := 0
				for range ps.Values() {
					count++
				}
				benchFoundSink = count == n
			}
		})
	}
}

// BenchmarkPatchSetColdAsMap measures the first full read of a freshly created PatchSet,
// which needs to build the cache from scratch.
func BenchmarkPatchSetColdAsMap(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			base := make(map[int]*int, n)
			for i := range n {
				base[i] = &i
			}
			patch := NewPatchFromMap(base)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ps := NewPatchSet(patch)
				benchSink = listViaAsMap(ps)
			}
		})
	}
}

// BenchmarkPatchSetFindValueMiss measures lookups of keys absent from the PatchSet.
func BenchmarkPatchSetFindValueMiss(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("N=%d/Forked", n), func(b *testing.B) {
			ps := newBenchPatchSet(n)
			ps.Fork()
			ps.Fork()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, benchFoundSink = ps.FindValue(n + i)
			}
		})
	}
}

// BenchmarkPatchSetFindValueHit measures lookups of keys present in a synced PatchSet.
func BenchmarkPatchSetFindValueHit(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("N=%d/Forked", n), func(b *testing.B) {
			ps := newBenchPatchSet(n)
			ps.Fork()
			ps.Fork()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, benchFoundSink = ps.FindValue(i % n)
			}
		})
	}
}

// BenchmarkPatchSetSetCurrent measures SetCurrent of existing keys in a forked PatchSet.
func BenchmarkPatchSetSetCurrent(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("N=%d/Forked", n), func(b *testing.B) {
			ps := newBenchPatchSet(n)
			ps.Fork()
			v := 1
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ps.SetCurrent(i%n, &v)
			}
		})
	}
}

// BenchmarkPatchSetFork50Revert measures resolving keys from a deep fork stack: 50 forks,
// one modified key in each, then reverting all of them.
func BenchmarkPatchSetFork50Revert(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			ps := newBenchPatchSet(n)
			v := 1
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for d := 0; d < 50; d++ {
					ps.Fork()
					ps.SetCurrent((i+d)%n, &v)
				}
				for d := 0; d < 50; d++ {
					ps.Revert()
				}
			}
		})
	}
}

// BenchmarkPatchSetNew measures constructing a PatchSet from a base map and reading it once.
func BenchmarkPatchSetNew(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("N=%d/NewThenValuesSlice", n), func(b *testing.B) {
			base := make(map[int]*int, n)
			for i := 0; i < n; i++ {
				v := i
				base[i] = &v
			}
			patch := NewPatchFromMap(base)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchSink = NewPatchSet(patch).ValuesSlice()
			}
		})
	}
}
