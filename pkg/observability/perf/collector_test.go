/*
Copyright The Kubernetes Authors.

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

package perf

import "testing"

type recordingCollector struct {
	started []string
	stopped []string
}

func (r *recordingCollector) Measure(phase string) func() {
	r.started = append(r.started, phase)
	return func() { r.stopped = append(r.stopped, phase) }
}

func TestMeasureNilCollectorIsNoop(t *testing.T) {
	stop := Measure(nil, "phase")
	if stop == nil {
		t.Fatal("Measure(nil) returned a nil stop func")
	}
	stop() // must not panic
}

func TestMeasureDelegatesToCollector(t *testing.T) {
	c := &recordingCollector{}
	stop := Measure(c, "estimate")
	if got := c.started; len(got) != 1 || got[0] != "estimate" {
		t.Fatalf("started = %v, want [estimate]", got)
	}
	if len(c.stopped) != 0 {
		t.Fatalf("stop recorded before stop() was called: %v", c.stopped)
	}
	stop()
	if got := c.stopped; len(got) != 1 || got[0] != "estimate" {
		t.Fatalf("stopped = %v, want [estimate]", got)
	}
}

var sink int

// BenchmarkMeasureNil is the cost of an instrumented call site in
// production, where the collector is nil.
func BenchmarkMeasureNil(b *testing.B) {
	b.ReportAllocs()
	var c Collector
	for i := 0; i < b.N; i++ {
		stop := Measure(c, "scale_up_estimate")
		stop()
		sink++
	}
}
