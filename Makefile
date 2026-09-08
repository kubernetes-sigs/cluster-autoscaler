# Copyright The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

export GOBIN ?= $(CURDIR)/bin
export PATH := $(GOBIN):$(PATH)

HELM      ?= go tool -modfile=tools/go.mod helm
KIND      ?= go tool -modfile=tools/go.mod kind
ENVTEST   ?= go tool -modfile=tools/go.mod setup-envtest
GINKGO    ?= go tool -modfile=tools/go.mod ginkgo

.PHONY: setup-envtest
setup-envtest: ENVTEST_K8S_VERSION ?= $(shell go list -m -f "{{ .Version }}" k8s.io/api | awk -F'[v.]' '{printf "1.%d", $$3}')
setup-envtest:
	@mkdir -p $(GOBIN)
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@$(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(GOBIN) -p path || { \
		echo "Warning: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		echo "Attempting fallback to latest available envtest binaries version."; \
		$(ENVTEST) use latest --bin-dir $(GOBIN) -p path || { \
			echo "Error: Failed to set up envtest binaries."; \
			exit 1; \
		}; \
	}
	@chmod -R +w $(GOBIN)/k8s

.PHONY: build-kwok
build-kwok:
	@CGO_ENABLED=0 GOOS=linux go build -o cluster-autoscaler-kwok ./kwok

.PHONY: image-kwok
image-kwok: TAG?=dev
image-kwok: build-kwok
	@docker build -t cluster-autoscaler-kwok:${TAG} -f kwok/Dockerfile .

.PHONY: test
test: setup-envtest
	@go test -race ./...

.PHONY: test-controllers
test-controllers: setup-envtest
	@$(GINKGO) --race ./pkg/test/integration/controllers/...

.PHONY: clean
clean:
	@rm -f cluster-autoscaler-kwok

.PHONY: format
format:
	test -z "$$(find . -path ./vendor -prune -type f -o -name '*.go' -exec gofmt -s -d {} + | tee /dev/stderr)" || \
	test -z "$$(find . -path ./vendor -prune -type f -o -name '*.go' -exec gofmt -s -w {} + | tee /dev/stderr)"

.PHONY: run-e2e
run-e2e: e2e-kwok-cluster e2e-install-ca
	@go test -tags e2e -v ./test/e2e/... -args -v=4
	@$(MAKE) e2e-teardown

E2E_CLUSTER_NAME ?= ca-e2e-kwok

.PHONY: e2e-kwok-cluster
e2e-kwok-cluster: E2E_KIND_CONFIG ?= kind-config.yaml
e2e-kwok-cluster: KWOK_REPO_URL ?= https://kwok.sigs.k8s.io/charts/
e2e-kwok-cluster:
	@$(KIND) create cluster --name $(E2E_CLUSTER_NAME) --config $(E2E_KIND_CONFIG)
	@$(HELM) repo add kwok-charts $(KWOK_REPO_URL)
	@$(HELM) upgrade --install kwok --namespace kube-system kwok-charts/kwok --set hostNetwork=true --wait
	@$(HELM) upgrade --install kwok-stage-fast --namespace kube-system kwok-charts/stage-fast --wait
	@kubectl apply -f pkg/apis/config/crd/

.PHONY: e2e-install-ca
e2e-install-ca: image-kwok
	@$(KIND) load docker-image cluster-autoscaler-kwok:dev --name $(E2E_CLUSTER_NAME)
	@$(HELM) upgrade --install cluster-autoscaler --namespace kube-system ./kwok/charts/ --wait \
		--set tolerations[0].key=node-role.kubernetes.io/control-plane \
		--set tolerations[0].operator=Exists \
		--set tolerations[0].effect=NoSchedule \

.PHONY: e2e-teardown
e2e-teardown:
	@$(KIND) delete cluster --name $(E2E_CLUSTER_NAME)
