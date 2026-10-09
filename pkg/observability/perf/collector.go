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

// Package perf provides an optional, injectable performance measurement sink.
//
// It exists so that arbitrary regions of core autoscaler code -- regions that
// are not behind a pluggable interface and therefore cannot be wrapped by a
// test-only decorator -- can be measured without adding a bespoke hook for each
// one. The Collector is carried on the AutoscalingContext and is nil in
// production, so instrumented call sites add no measurable overhead when it is
// unset.
package perf

// Collector is an optional performance sink. Production leaves it nil (zero
// overhead). Tests inject an implementation to measure named code regions.
//
// Implementations must be safe to call from the goroutine that invokes Measure.
// A measured region should not intentionally span goroutine boundaries: work
// done on other goroutines while the region is open is attributed to it by
// implementations that account process-wide, and is lost by those that do
// not.
type Collector interface {
	// Measure starts measuring a named phase and returns a stop function that
	// records the measurement when called. Intended usage:
	//
	//	defer perf.Measure(ctx.PerfCollector, "phase_name")()
	Measure(phase string) (stop func())
}

// noop is a shared, allocation-free stop function used when no Collector is set.
func noop() {}

// Measure is a nil-safe entry point for instrumenting a code region. It returns
// a no-op stop function when c is nil, so call sites never need their own
// nil-check and add no allocation on the hot path when instrumentation is off.
func Measure(c Collector, phase string) (stop func()) {
	if c == nil {
		return noop
	}
	return c.Measure(phase)
}
