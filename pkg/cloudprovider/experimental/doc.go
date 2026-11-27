/*
Copyright 2026 The Kubernetes Authors.

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

// Package experimental hosts unstable, opt-in Cloud Provider interface extensions
// and their V1 fallback gateway helpers while iterating toward Cloud Provider V2.
//
// Interfaces and helpers in this package carry no backward-compatibility guarantees
// and may change or be removed without deprecation cycles. See README.md for
// iteration guidelines and the active interface registry.
package experimental
