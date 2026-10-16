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

package occupancy

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
)

// Version identifies the opt-in occupancy contract, which is not part of the
// standard Kubernetes scheduler extender API.
const Version = "experimental-full-occupancy-v1alpha1"

// Preserve requires the evaluator to validate and retain the Pod's assignment.
// Allocate requests a fresh assignment, ignoring scheduler-generated allocation
// results in Pod metadata but retaining user device-selection constraints.
const (
	Preserve = "preserve"
	Allocate = "allocate"
)

// Allocation constrains evaluation without exposing the caller's lifecycle.
// Preserve requires the evaluator to extract its allocation metadata from Pod
// and validate its binding, inventory, demand and user constraints. Missing or
// invalid evidence must fail, never fall back to allocate. Allocate ignores only
// evaluator-generated assignment results in Pod, retaining user constraints.
type Allocation struct {
	Mode string `json:"mode"`
}

// Resident describes a Pod occupying a candidate node and whether its device
// assignment must be preserved or computed afresh during feasibility evaluation.
type Resident struct {
	Pod        *v1.Pod    `json:"pod"`
	Allocation Allocation `json:"allocation"`
}

// Input supplies the complete occupancy for each candidate Node. Empty arrays
// mean empty occupancy; missing/null arrays are invalid. Real-cluster Pod caches
// must not supplement this input. Pod order carries no caller execution order.
type Input struct {
	// Resources names the configured extender resources that must be evaluated.
	Resources []string              `json:"resources"`
	Version   string                `json:"version"`
	Nodes     map[string][]Resident `json:"nodes"`
}

// Ack confirms interpretation of the extension and correlates the raw request
// bytes. It is neither authentication nor proof of a correct allocator.
type Ack struct {
	Version string `json:"version"`
	Digest  string `json:"requestDigest"`
}

// Request extends the existing Filter body without changing its URL. With an
// extension, Pod is always evaluated for a fresh assignment on each candidate.
// With no extension, legacy handler semantics are outside this contract.
type Request struct {
	extenderv1.ExtenderArgs
	Simulation *Input `json:"simulation,omitempty"`
}

// Result extends a Filter response with acknowledgement of the occupancy request.
// A successful occupancy evaluation requires Simulation to match the request's
// protocol version and digest.
type Result struct {
	extenderv1.ExtenderFilterResult
	Simulation *Ack `json:"simulation,omitempty"`
}

// Validate checks completeness and unambiguous constraints without mutating the
// input. It does not validate device-specific record contents, inventory, Pod
// demand, authorization, or allocation feasibility; those belong to the evaluator.
func Validate(r Request) error {
	if r.Simulation == nil || r.Simulation.Version != Version {
		return fmt.Errorf("missing or unsupported feasibility version")
	}
	if len(r.Simulation.Resources) == 0 {
		return fmt.Errorf("missing managed resource declaration")
	}
	if r.Pod == nil || r.Pod.UID == "" || r.Nodes == nil || len(r.Nodes.Items) == 0 || r.NodeNames != nil {
		return fmt.Errorf("candidate Pod UID and explicit Nodes are required")
	}
	if len(r.Simulation.Nodes) != len(r.Nodes.Items) {
		return fmt.Errorf("occupancy must cover exactly the candidate nodes")
	}
	nodes := make(map[string]bool)
	pods := map[string]bool{string(r.Pod.UID): true}
	for _, node := range r.Nodes.Items {
		if node.Name == "" || nodes[node.Name] {
			return fmt.Errorf("missing or duplicate node name")
		}
		nodes[node.Name] = true
		residents, ok := r.Simulation.Nodes[node.Name]
		if !ok || residents == nil {
			return fmt.Errorf("missing complete occupancy for node %s", node.Name)
		}
		for _, resident := range residents {
			if resident.Pod == nil || resident.Pod.UID == "" || pods[string(resident.Pod.UID)] {
				return fmt.Errorf("missing or duplicate Pod identity")
			}
			pods[string(resident.Pod.UID)] = true
			a := resident.Allocation
			switch a.Mode {
			case Preserve, Allocate:
			default:
				return fmt.Errorf("unknown allocation constraint %q", a.Mode)
			}
		}
	}
	return nil
}
