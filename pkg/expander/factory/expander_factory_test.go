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

package factory

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/cluster-autoscaler/pkg/expander"
)

// AvailableExpanders is only used for the --expander help text, so nothing
// else catches it drifting from the registered expanders.
func TestAvailableExpandersMatchRegisteredExpanders(t *testing.T) {
	f := NewFactory()
	// Dependencies are only used inside the create funcs, which are not called here.
	f.RegisterDefaultExpanders(nil, nil, nil, "", "", "")

	var registered []string
	for name := range f.createFunc {
		registered = append(registered, name)
	}
	assert.ElementsMatch(t, expander.AvailableExpanders, registered)
}
