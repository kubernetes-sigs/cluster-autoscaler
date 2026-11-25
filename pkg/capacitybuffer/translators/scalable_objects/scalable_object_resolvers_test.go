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

package scalableobject

import (
	"testing"
)

func TestSupportedScalableObjectResolversAreKeyedByReferencedApiGroup(t *testing.T) {
	resolvers := map[string]ScalableObjectTemplateResolver{}
	for _, resolver := range GetSupportedScalableObjectResolvers(nil) {
		resolvers[resolver.GetResolverKey()] = resolver
	}

	tests := []struct {
		name     string
		apiGroup string
		kind     string
	}{
		{name: "deployment", apiGroup: "apps", kind: DeploymentKind},
		{name: "replica set", apiGroup: "apps", kind: ReplicaSetKind},
		{name: "stateful set", apiGroup: "apps", kind: StatefulSetKind},
		{name: "job", apiGroup: "batch", kind: JobKind},
		{name: "replication controller in the core group", apiGroup: "", kind: ReplicationControllerKind},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, found := resolvers[GetResolverKey(test.apiGroup, test.kind)]
			if !found {
				t.Errorf("failed to find resolver for apiGroup: %s, kind: %s", test.apiGroup, test.kind)
			}
		})
	}
}
