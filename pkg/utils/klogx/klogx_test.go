/*
Copyright 2018 The Kubernetes Authors.

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

package klogx

import (
	"bytes"
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	klog "k8s.io/klog/v2"
)

// Left, UpTo and Over should work as expected.
func TestLoggingQuota(t *testing.T) {
	q := NewLoggingQuota(3)

	for i := 0; i < 5; i++ {
		assert.Equal(t, 3-i, q.Left())
		assert.Equal(t, i < 3, V(0).UpTo(q).enabled)
		assert.Equal(t, i >= 3, V(0).Over(q).enabled)
	}
}

// Reset should restore left to the original limit.
func TestReset(t *testing.T) {
	q := NewLoggingQuota(3)

	for i := 0; i < 5; i++ {
		assert.Equal(t, i < 3, V(0).UpTo(q).enabled)
	}

	q.Reset()

	assert.Equal(t, 3, q.Left())
	assert.False(t, V(0).Over(q).enabled)
	assert.True(t, V(0).UpTo(q).enabled)
}

// Tests that quota isn't used up by calls limited by verbosity.
func TestVFalse(t *testing.T) {
	// XXX: this is a hack to get a disabled V, since klog v2 no longer
	// provides an easy way to create it
	v := V(10000)
	q := NewLoggingQuota(3)

	assert.False(t, v.UpTo(q).v.Enabled())
	assert.Equal(t, 3, q.Left())
}

// Tests that V limits calls based on verbosity the same way as klog.V.
func TestV(t *testing.T) {
	for i := klog.Level(0); i <= 10; i++ {
		assert.Equal(t, klog.V(i).Enabled(), V(i).v.Enabled())
	}
}

// The log header should name the caller of Infof, Info and Warningf, not
// klogx.go.
func TestHeaderNamesCaller(t *testing.T) {
	defer klog.CaptureState().Restore()
	var buf bytes.Buffer
	klog.LogToStderr(false)
	klog.SetOutput(&buf)

	for name, log := range map[string]func(Verbose){
		"Infof":    func(v Verbose) { v.Infof("message from %s", "Infof") },
		"Info":     func(v Verbose) { v.Info("message from Info") },
		"Warningf": func(v Verbose) { v.Warningf("message from %s", "Warningf") },
	} {
		buf.Reset()
		log(V(0).UpTo(NewLoggingQuota(1)))
		assert.Contains(t, buf.String(), "klogx_test.go:", "%s should report the caller in the header", name)
		assert.NotContains(t, buf.String(), "klogx.go:", "%s should not report the wrapper in the header", name)
	}
}

// A -vmodule entry for the calling file, not for klogx.go, should decide
// whether a V(n) call logs. Without a matching entry, -v decides.
func TestVModuleAppliesToCallerFile(t *testing.T) {
	testCases := []struct {
		name       string
		vmodule    string
		log        func(Verbose)
		wantLogged bool
	}{
		{
			name:       "Infof matches caller file",
			vmodule:    "klogx_test=4",
			log:        func(v Verbose) { v.Infof("message") },
			wantLogged: true,
		},
		{
			name:       "Info matches caller file",
			vmodule:    "klogx_test=4",
			log:        func(v Verbose) { v.Info("message") },
			wantLogged: true,
		},
		{
			name:       "Warningf matches caller file",
			vmodule:    "klogx_test=4",
			log:        func(v Verbose) { v.Warningf("message") },
			wantLogged: true,
		},
		{
			name:       "Infof ignores wrapper file",
			vmodule:    "klogx=4",
			log:        func(v Verbose) { v.Infof("message") },
			wantLogged: false,
		},
		{
			name:       "Info ignores wrapper file",
			vmodule:    "klogx=4",
			log:        func(v Verbose) { v.Info("message") },
			wantLogged: false,
		},
		{
			name:       "Warningf ignores wrapper file",
			vmodule:    "klogx=4",
			log:        func(v Verbose) { v.Warningf("message") },
			wantLogged: false,
		},
		{
			name:       "Warningf without -vmodule respects -v",
			vmodule:    "",
			log:        func(v Verbose) { v.Warningf("message") },
			wantLogged: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer klog.CaptureState().Restore()
			var buf bytes.Buffer
			klog.LogToStderr(false)
			klog.SetOutput(&buf)
			// A local flag set reaches klog's -vmodule without registering the
			// klog flags for the whole test binary.
			var fs flag.FlagSet
			klog.InitFlags(&fs)
			require.NoError(t, fs.Set("vmodule", tc.vmodule))

			tc.log(V(4))
			assert.Equal(t, tc.wantLogged, buf.Len() > 0, "-vmodule=%s, klog output: %q", tc.vmodule, buf.String())
		})
	}
}
