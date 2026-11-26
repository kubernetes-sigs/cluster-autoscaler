#!/usr/bin/env bash

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

# This script checks if go versions from go.mod and the module files in tools/
# are in sync.

set -o errexit
set -o nounset
set -o pipefail

KUBE_ROOT=$(dirname "${BASH_SOURCE[0]}")/..

# Extract Go versions using awk
ROOT_GO_VER=$(awk '/^go [0-9]/ { print $2 }' "${KUBE_ROOT}/go.mod")

for MOD_FILE in tools/go.mod tools/golangci-lint.mod; do
  MOD_GO_VER=$(awk '/^go [0-9]/ { print $2 }' "${KUBE_ROOT}/${MOD_FILE}")

  if [ "${ROOT_GO_VER}" != "${MOD_GO_VER}" ]; then
    echo "Error: Go version mismatch detected!"
    echo "  Root go.mod: ${ROOT_GO_VER}"
    echo "  ${MOD_FILE}: ${MOD_GO_VER}"
    echo "Please align the versions by manually updating '${MOD_FILE}'."
    exit 1
  fi
done

echo "Go versions are aligned: ${ROOT_GO_VER}"
