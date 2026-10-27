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
	"math/rand/v2"
	"slices"
	"testing"
)

// referencePatchSet is a deliberately naive model of PatchSet used for
// differential testing: it stores the full effective state for every layer.
type referencePatchSet struct {
	states []map[int]int
}

func (r *referencePatchSet) effective() map[int]int {
	if len(r.states) == 0 {
		return map[int]int{}
	}
	return r.states[len(r.states)-1]
}

func (r *referencePatchSet) top() map[int]int {
	if len(r.states) == 0 {
		r.states = append(r.states, map[int]int{})
	}
	return r.states[len(r.states)-1]
}

func (r *referencePatchSet) fork() {
	r.states = append(r.states, maps.Clone(r.effective()))
}

func (r *referencePatchSet) commit() {
	if len(r.states) < 2 {
		return
	}
	r.states[len(r.states)-2] = r.states[len(r.states)-1]
	r.states = r.states[:len(r.states)-1]
}

func (r *referencePatchSet) revert() {
	if len(r.states) <= 1 {
		return
	}
	r.states = r.states[:len(r.states)-1]
}

// byteReader hands out the bytes of a fuzz input one by one, and zeros once it's exhausted.
type byteReader struct {
	data []byte
}

func (r *byteReader) next() byte {
	if len(r.data) == 0 {
		return 0
	}
	b := r.data[0]
	r.data = r.data[1:]
	return b
}

// Operation codes of the FuzzPatchSetDifferential input (taken modulo patchSetOpCount).
const (
	opFork = iota
	opCommit
	opRevert
	opSetCurrent
	opDeleteCurrent
	opFindValue
	opAsMap
	opValues
	opLen
	patchSetOpCount
)

// differentialKeySpace is small on purpose, so that operations frequently hit the same keys.
const differentialKeySpace = 16

// FuzzPatchSetDifferential interprets the input as a sequence of operations, runs them
// against both PatchSet and referencePatchSet, and compares the results of all read
// operations after every step.
//
// Input format:
//   - 1 byte: the number of initial patches passed to NewPatchSet (mod 4), then for each
//     patch 1 byte with its number of entries (mod 8) and 2 bytes per entry: key and value
//     (a value divisible by 4 deletes the key instead).
//   - The rest: operations, each 2 bytes (operation code, key), plus 1 value byte for
//     SetCurrent.
//
// `go test` runs the seed corpus below. To search for new failures, run e.g.:
//
//	go test -run '^$' -fuzz FuzzPatchSetDifferential -fuzztime=30s ./pkg/simulator/common/
func FuzzPatchSetDifferential(f *testing.F) {
	f.Add([]byte{})
	// No initial patches: Set, Fork, Set, Delete, Find, Revert, Find, Values, Len, AsMap.
	f.Add([]byte{
		0,
		opSetCurrent, 1, 5, opFork, 0, opSetCurrent, 1, 6, opDeleteCurrent, 2,
		opFindValue, 1, opRevert, 0, opFindValue, 1, opValues, 0, opLen, 0, opAsMap, 0,
	})
	// Base patch {1: 7, 2: 9} and a patch deleting 1, then nested forks with a Commit and Reverts.
	f.Add([]byte{
		2, 2, 1, 7, 2, 9, 1, 1, 4,
		opAsMap, 0, opFork, 0, opSetCurrent, 3, 9, opDeleteCurrent, 2, opFork, 0, opSetCurrent, 1, 1,
		opCommit, 0, opValues, 0, opRevert, 0, opFindValue, 1, opFindValue, 2, opFindValue, 3,
		opLen, 0, opRevert, 0, opAsMap, 0, opValues, 0,
	})
	// A few long pseudo-random sequences, so that plain `go test` also exercises
	// long interleavings of forks, commits and reverts.
	for seed := range uint64(8) {
		rng := rand.New(rand.NewPCG(seed, 0x5eed))
		data := make([]byte, 3000)
		for i := range data {
			data[i] = byte(rng.UintN(256))
		}
		f.Add(data)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		r := &byteReader{data: data}
		ref := &referencePatchSet{}

		var patches []*Patch[int, int]
		for range r.next() % 4 {
			patch := NewPatch[int, int]()
			state := maps.Clone(ref.effective())
			for range r.next() % 8 {
				key, value := int(r.next()%differentialKeySpace), int(r.next())
				if value%4 == 0 {
					patch.Delete(key)
					delete(state, key)
				} else {
					patch.Set(key, value)
					state[key] = value
				}
			}
			patches = append(patches, patch)
			ref.states = append(ref.states, state)
		}
		ps := NewPatchSet(patches...)

		var history []string
		fail := func(format string, args ...any) {
			t.Helper()
			const maxHistory = 30
			recent := history[max(0, len(history)-maxHistory):]
			t.Fatalf("after %d ops (last %d: %v): "+format, append([]any{len(history), len(recent), recent}, args...)...)
		}

		for len(r.data) > 0 {
			op, key := r.next()%patchSetOpCount, int(r.next()%differentialKeySpace)
			switch op {
			case opFork:
				history = append(history, "Fork")
				ps.Fork()
				ref.fork()
			case opCommit:
				history = append(history, "Commit")
				ps.Commit()
				ref.commit()
			case opRevert:
				history = append(history, "Revert")
				ps.Revert()
				ref.revert()
			case opSetCurrent:
				value := int(r.next())
				history = append(history, fmt.Sprintf("SetCurrent(%d, %d)", key, value))
				ps.SetCurrent(key, value)
				ref.top()[key] = value
			case opDeleteCurrent:
				history = append(history, fmt.Sprintf("DeleteCurrent(%d)", key))
				ps.DeleteCurrent(key)
				delete(ref.top(), key)
			case opFindValue:
				history = append(history, fmt.Sprintf("FindValue(%d)", key))
				gotValue, gotFound := ps.FindValue(key)
				wantValue, wantFound := ref.effective()[key]
				if gotValue != wantValue || gotFound != wantFound {
					fail("FindValue(%d) got (%d, %v), want (%d, %v)", key, gotValue, gotFound, wantValue, wantFound)
				}
			case opAsMap:
				history = append(history, "AsMap")
				if got, want := ps.AsMap(), ref.effective(); !maps.Equal(got, want) {
					fail("AsMap() got %v, want %v", got, want)
				}
			case opValues:
				history = append(history, "ValuesSlice/Values")
				want := slices.Sorted(maps.Values(ref.effective()))
				got := ps.ValuesSlice()
				slices.Sort(got)
				if !slices.Equal(got, want) {
					fail("ValuesSlice() got %v, want %v", got, want)
				}
				if got := slices.Sorted(ps.Values()); !slices.Equal(got, want) {
					fail("Values() got %v, want %v", got, want)
				}
			case opLen:
				history = append(history, "Len")
				if got, want := ps.Len(), len(ref.effective()); got != want {
					fail("Len() got %d, want %d", got, want)
				}
			}

			if len(ps.patches) != len(ref.states) {
				fail("got %d patches, want %d", len(ps.patches), len(ref.states))
			}
		}
	})
}
