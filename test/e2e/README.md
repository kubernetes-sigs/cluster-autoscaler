# Cluster Autoscaler E2E Tests

This directory contains end-to-end (E2E) tests for Cluster Autoscaler built on [`sigs.k8s.io/e2e-framework`](https://github.com/kubernetes-sigs/e2e-framework).

## Provider Configuration (`TestConfig`)

Test environment parameters (node group name and label key, single-node CPU and memory capacities, taint tolerations, and operation timeouts) are parameterized in [`config.go`](./config.go) via `TestConfig`.

The test suite initializes a global `testCfg = KwokTestConfig()` in [`suite_test.go`](./suite_test.go), and helper functions such as `NewTestPod` and `NewTestPodWithResourceFraction` allocate pod CPU and memory requests as fractions of `testCfg.NodeCPU` and `testCfg.NodeMemory`.

## KWOK E2E Architecture (`kwok/`)

By default, the E2E suite runs against a local Kind cluster backed by the KWOK (Kubernetes WithOut Kubelet) provider:

- `kwok/` App: [`kwok/main.go`](../../kwok/main.go) builds a Cluster Autoscaler binary registered with the `kwok` cloud provider (`k8s.io/autoscaler/cluster-autoscaler/cloudprovider/kwok`). It is packaged into a container image via [`kwok/Dockerfile`](../../kwok/Dockerfile) and deployed to the Kind cluster using the Helm chart in [`kwok/charts/`](../../kwok/charts/).
- Simulated Node Templates (`kwok/charts/templates/configmap.yaml`): The `kwok-provider-templates` ConfigMap defines the simulated node template (`kind-worker`) used by Cluster Autoscaler and KWOK:
  - Node group label: `kwok-nodegroup: kind-worker`
  - Node taint: `kwok-provider=true:NoSchedule`
  - Simulated node capacity / allocatable resources: `cpu: "12"` and `memory: 32781516Ki` (`~32Gi`), matching `KwokTestConfig()` in [`config.go`](./config.go).

## Running E2E Tests with KWOK (`Makefile`)

From the repository root, you can run the full workflow or execute individual steps using the `Makefile` targets:

### Full End-to-End Run

```bash
# Install tools, spin up Kind + KWOK cluster, deploy CA, run E2E tests, and tear down
make run-e2e
```

### Step-by-Step Workflow (Recommended for Local Development)

```bash
# Install local binary dependencies (go, helm, kind, kubectl, kwokctl) into ./bin
make install-dependencies

# Create the local Kind cluster and install the KWOK controller + node/pod stages
make e2e-kwok-cluster

# Build the kwok CA image, load it into Kind, and install the kwok Helm chart
make e2e-install-ca

# Run the E2E test suite against the running cluster
go test -tags e2e -timeout 20m -v ./test/e2e/...

# Delete the Kind cluster when finished
make e2e-teardown
```
