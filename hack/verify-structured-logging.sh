#!/bin/bash

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

# This script runs logcheck (sigs.k8s.io/logtools/logcheck) on all packages in
# this module to catch unstructured klog calls (Infof, Errorf, ...) and
# malformed structured logging calls (bad key/value pairs, V(0), ...).
#
# It is based on hack/verify-structured-logging.sh from kubernetes/kubernetes,
# which has since been replaced there by logcheck running inside golangci-lint.
#
# Per-file exceptions live in hack/logcheck.conf.

set -o errexit
set -o nounset
set -o pipefail

KUBE_ROOT=$(dirname "${BASH_SOURCE[0]}")/..
cd "${KUBE_ROOT}"

LOGCHECK_VERSION=${LOGCHECK_VERSION:-v0.10.1}
LOGCHECK_CONFIG="hack/logcheck.conf"

echo "Running structured logging static check (logcheck ${LOGCHECK_VERSION})"
ret=0
go run "sigs.k8s.io/logtools/logcheck@${LOGCHECK_VERSION}" \
  -config "${LOGCHECK_CONFIG}" \
  ./... || ret=$?

if [ $ret -eq 0 ]; then
  echo "Structured logging static check passed :)"
else
  echo "Please fix above failures. You can locally test via:"
  echo "hack/verify-structured-logging.sh"
fi

exit $ret
