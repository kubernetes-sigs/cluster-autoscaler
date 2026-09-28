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
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	fwk "k8s.io/kube-scheduler/framework"
	config "k8s.io/kubernetes/pkg/scheduler/apis/config"
)

const bodyLimit = 1 << 20

// Extender sends complete node-local occupancy to an explicitly enabled evaluator.
type Extender struct {
	fwk.Extender
	endpoint  string
	client    *http.Client
	resources []string
}

// Filter evaluates pod against each node and its complete resident occupancy
// without mutating the supplied objects. It returns feasible nodes and failure
// reasons, or an error if the request, transport or acknowledged response is invalid.
func (e *Extender) Filter(pod *v1.Pod, nodes []fwk.NodeInfo) ([]fwk.NodeInfo, extenderv1.FailedNodesMap, extenderv1.FailedNodesMap, error) {
	body, err := e.buildRequest(pod, nodes)
	if err != nil {
		return nil, nil, nil, err
	}
	return e.exchange(body, nodes)
}

// Kept separate so request construction can be checked for object mutation.
func (e *Extender) buildRequest(pod *v1.Pod, nodes []fwk.NodeInfo) ([]byte, error) {
	args := Request{
		ExtenderArgs: extenderv1.ExtenderArgs{Pod: pod.DeepCopy(), Nodes: &v1.NodeList{}},
		Simulation:   &Input{Version: Version, Resources: e.resources, Nodes: make(map[string][]Resident)},
	}
	for _, node := range nodes {
		name := node.Node().Name
		args.Nodes.Items = append(args.Nodes.Items, *node.Node().DeepCopy())
		residents := make([]Resident, 0, len(node.GetPods()))
		for _, info := range node.GetPods() {
			resident := info.GetPod()
			allocation, err := e.residentAllocation(name, info)
			if err != nil {
				return nil, err
			}
			residents = append(residents, Resident{Pod: resident.DeepCopy(), Allocation: allocation})
		}
		sort.Slice(residents, func(i, j int) bool { return residents[i].Pod.UID < residents[j].Pod.UID })
		args.Simulation.Nodes[name] = residents
	}
	if err := Validate(args); err != nil {
		return nil, err
	}
	return json.Marshal(args)
}

func (e *Extender) exchange(body []byte, nodes []fwk.NodeInfo) ([]fwk.NodeInfo, extenderv1.FailedNodesMap, extenderv1.FailedNodesMap, error) {
	if len(body) > bodyLimit {
		return nil, nil, nil, fmt.Errorf("feasibility request exceeds size limit")
	}
	resp, err := e.client.Post(e.endpoint, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, nil, fmt.Errorf("filter HTTP status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, bodyLimit+1))
	if err != nil {
		return nil, nil, nil, err
	}
	if len(data) > bodyLimit {
		return nil, nil, nil, fmt.Errorf("feasibility response exceeds size limit")
	}
	var result Result
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, nil, nil, err
	}
	if result.Error != "" {
		return nil, nil, nil, fmt.Errorf("filter: %s", result.Error)
	}
	if result.Simulation == nil || result.Simulation.Version != Version || result.Simulation.Digest != fmt.Sprintf("%x", sha256.Sum256(body)) {
		return nil, nil, nil, fmt.Errorf("missing or mismatched simulation acknowledgement")
	}
	if result.Nodes == nil || result.NodeNames != nil {
		return nil, nil, nil, fmt.Errorf("expected explicit Nodes result")
	}
	byName := make(map[string]fwk.NodeInfo, len(nodes))
	for _, node := range nodes {
		byName[node.Node().Name] = node
	}
	filtered := make([]fwk.NodeInfo, 0, len(result.Nodes.Items))
	for _, node := range result.Nodes.Items {
		original, ok := byName[node.Name]
		if !ok {
			return nil, nil, nil, fmt.Errorf("unknown or duplicate response node %q", node.Name)
		}
		// Return the snapshot object, never a Node rewritten by the endpoint.
		filtered = append(filtered, original)
		delete(byName, node.Name)
	}
	for _, failed := range []extenderv1.FailedNodesMap{result.FailedNodes, result.FailedAndUnresolvableNodes} {
		for name := range failed {
			if _, ok := byName[name]; !ok {
				return nil, nil, nil, fmt.Errorf("unknown or contradictory failed node %q", name)
			}
			delete(byName, name)
		}
	}
	if len(byName) != 0 {
		return nil, nil, nil, fmt.Errorf("incomplete feasibility response")
	}
	return filtered, result.FailedNodes, result.FailedAndUnresolvableNodes, nil
}

// New explicitly enables the experimental full-occupancy extension on one Filter.
// Other extender methods keep their existing transport and behavior.
func New(base fwk.Extender, cfg *config.Extender) (*Extender, error) {
	if base.IsIgnorable() || cfg.FilterVerb == "" {
		return nil, fmt.Errorf("full occupancy requires a non-ignorable Filter")
	}
	resources := make([]string, 0, len(cfg.ManagedResources))
	for _, r := range cfg.ManagedResources {
		resources = append(resources, r.Name)
	}
	if len(resources) == 0 {
		return nil, fmt.Errorf("full occupancy requires explicit managedResources")
	}
	var tls rest.TLSClientConfig
	if c := cfg.TLSConfig; c != nil {
		tls = rest.TLSClientConfig{Insecure: c.Insecure, ServerName: c.ServerName, CertFile: c.CertFile, KeyFile: c.KeyFile, CAFile: c.CAFile, CertData: c.CertData, KeyData: c.KeyData, CAData: c.CAData}
	}
	transport, err := rest.TransportFor(&rest.Config{TLSClientConfig: tls})
	if err != nil {
		return nil, err
	}
	timeout := cfg.HTTPTimeout.Duration
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Extender{Extender: base, resources: resources, endpoint: strings.TrimRight(cfg.URLPrefix, "/") + "/" + strings.TrimLeft(cfg.FilterVerb, "/"), client: &http.Client{Transport: transport, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (e *Extender) residentAllocation(node string, info fwk.PodInfo) (Allocation, error) {
	if !e.IsInterested(info.GetPod()) {
		return Allocation{Mode: Allocate}, nil
	}
	provenance, ok := info.(interface{ DevicePlacement() (string, bool) })
	if !ok {
		return Allocation{}, fmt.Errorf("missing Pod placement provenance")
	}
	original, simulated := provenance.DevicePlacement()
	if simulated {
		return Allocation{Mode: Allocate}, nil
	}
	p := info.GetPod()
	if original == "" || original != node || p.Spec.NodeName != node {
		return Allocation{}, fmt.Errorf("unknown or inconsistent real Pod placement")
	}
	// Only the evaluator understands allocation metadata inside the full Pod.
	return Allocation{Mode: Preserve}, nil
}
