module sigs.k8s.io/cluster-autoscaler

go 1.27.0

godebug default=go1.26

require (
	github.com/blang/semver/v4 v4.0.0
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.4
	github.com/google/go-cmp v0.7.0
	github.com/google/uuid v1.6.0
	github.com/onsi/ginkgo/v2 v2.32.2
	github.com/onsi/gomega v1.43.0
	github.com/prometheus/client_golang v1.24.1
	github.com/prometheus/client_model v0.6.2
	github.com/spf13/pflag v1.0.10
	github.com/stretchr/testify v1.12.1
	google.golang.org/grpc v1.83.2
	google.golang.org/protobuf v1.36.12
	gopkg.in/inf.v0 v0.9.1
	k8s.io/api v0.0.0-20260915221329-88a091c6770a
	// v0.36.0-alpha.2.0.20260912220903-93e88e8ed40d is actually more recent than v0.37.0:
	// https://github.com/kubernetes/apimachinery/compare/v0.37.0...93e88e8ed40d49a1d09564c2577130618b649a75
	k8s.io/apimachinery v0.36.0-alpha.2.0.20260912220903-93e88e8ed40d
	k8s.io/apiserver v0.0.0-20260915223511-e11e0d21202a
	k8s.io/autoscaler/cluster-autoscaler/apis v0.0.0-20260717085528-eec9bc4dc1d2
	k8s.io/client-go v0.0.0-20260915182852-f18b05d505a1
	k8s.io/cloud-provider v0.0.0-20260914231244-258b7f2bdece
	k8s.io/component-base v0.0.0-20260911142345-c58055e8ea54
	k8s.io/component-helpers v0.20.0-alpha.2.0.20260909235451-037166a3854b
	k8s.io/dynamic-resource-allocation v0.26.0-beta.0.0.20260916032245-68a37f254f41
	k8s.io/klog/v2 v2.140.0
	k8s.io/kube-scheduler v0.0.0-20260915112550-b1ca9017534c
	k8s.io/kubernetes v1.38.0-alpha.0.0.20260916011317-044ae9eb5e93
	k8s.io/utils v0.0.0-20260626114624-be93311217bd
	sigs.k8s.io/controller-runtime v0.24.1
	sigs.k8s.io/e2e-framework v0.7.0
	sigs.k8s.io/yaml v1.6.0
)

require (
	cel.dev/expr v0.25.2 // indirect
	github.com/Masterminds/semver/v3 v3.4.0 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/NYTimes/gziphandler v1.1.1 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/emicklei/go-restful/v3 v3.13.0 // indirect
	github.com/evanphx/json-patch/v5 v5.9.11 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/fxamacker/cbor/v2 v2.9.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-logr/zapr v1.3.0 // indirect
	github.com/go-openapi/jsonpointer v1.0.0 // indirect
	github.com/go-openapi/jsonreference v1.0.0 // indirect
	github.com/go-openapi/swag v0.27.1 // indirect
	github.com/go-openapi/swag/cmdutils v0.27.1 // indirect
	github.com/go-openapi/swag/conv v0.27.1 // indirect
	github.com/go-openapi/swag/fileutils v0.27.1 // indirect
	github.com/go-openapi/swag/jsonutils v0.27.1 // indirect
	github.com/go-openapi/swag/loading v0.27.1 // indirect
	github.com/go-openapi/swag/mangling v0.27.1 // indirect
	github.com/go-openapi/swag/netutils v0.27.1 // indirect
	github.com/go-openapi/swag/pools v0.27.1 // indirect
	github.com/go-openapi/swag/stringutils v0.27.1 // indirect
	github.com/go-openapi/swag/typeutils v0.27.1 // indirect
	github.com/go-openapi/swag/yamlutils v0.27.1 // indirect
	github.com/go-task/slim-sprig/v3 v3.0.0 // indirect
	github.com/google/cel-go v0.29.2 // indirect
	github.com/google/gnostic-models v0.7.0 // indirect
	github.com/google/pprof v0.0.0-20260402051712-545e8a4df936 // indirect
	github.com/gorilla/websocket v1.5.4-0.20250319132907-e064f32e3674 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.29.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/moby/spdystream v0.5.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/common v0.70.1 // indirect
	github.com/prometheus/procfs v0.21.1 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/spf13/cobra v1.10.2 // indirect
	github.com/stretchr/objx v0.5.3 // indirect
	github.com/vladimirvivien/gexe v0.5.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.68.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.69.0 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.44.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/sdk v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.opentelemetry.io/proto/otlp v1.10.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.1 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/exp v0.0.0-20260410095643-746e56fc9e2f // indirect
	golang.org/x/mod v0.38.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/term v0.45.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	golang.org/x/tools v0.48.0 // indirect
	gomodules.xyz/jsonpatch/v2 v2.4.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	gopkg.in/evanphx/json-patch.v4 v4.13.0 // indirect
	k8s.io/apiextensions-apiserver v0.0.0-20260914225209-bf2eceae6da0 // indirect
	k8s.io/code-generator v0.30.0-alpha.3.0.20260912222043-1ff4f8e78b35 // indirect
	k8s.io/controller-manager v0.20.0-alpha.1.0.20260914231024-93b629e26fd7 // indirect
	k8s.io/cri-api v0.38.0-alpha.0.0.20260911150216-8f649dc10621 // indirect
	k8s.io/cri-client v0.31.0-alpha.0.0.20260911150403-9d48a93aabfc // indirect
	k8s.io/csi-translation-lib v0.0.0-20260908232008-15b78256838b // indirect
	k8s.io/gengo/v2 v2.0.0-20260408192533-25e2208e0dc3 // indirect
	k8s.io/kube-openapi v0.0.0-20260908163437-c4db2bdfbfe6 // indirect
	k8s.io/kubelet v0.0.0-20260911150729-4427ac790632 // indirect
	k8s.io/streaming v0.38.0-alpha.0.0.20260914154742-f99df5dfe25e // indirect
	sigs.k8s.io/json v0.0.0-20250730193827-2d320260d730 // indirect
	sigs.k8s.io/randfill v1.0.0 // indirect
	sigs.k8s.io/structured-merge-diff/v6 v6.4.2 // indirect
	sigs.k8s.io/structured-merge-diff/v7 v7.0.0 // indirect
)

replace k8s.io/api => k8s.io/api v0.0.0-20260915221329-88a091c6770a

replace k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.0.0-20260914225209-bf2eceae6da0

replace k8s.io/apimachinery => k8s.io/apimachinery v0.36.0-alpha.2.0.20260912220903-93e88e8ed40d

replace k8s.io/apiserver => k8s.io/apiserver v0.0.0-20260915223511-e11e0d21202a

replace k8s.io/cli-runtime => k8s.io/cli-runtime v0.0.0-20260908225334-f08c4a065d0e

replace k8s.io/client-go => k8s.io/client-go v0.0.0-20260915182852-f18b05d505a1

replace k8s.io/cloud-provider => k8s.io/cloud-provider v0.0.0-20260914231244-258b7f2bdece

replace k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.0.0-20260914231654-e4b5a50b5233

replace k8s.io/code-generator => k8s.io/code-generator v0.30.0-alpha.3.0.20260912222043-1ff4f8e78b35

replace k8s.io/component-base => k8s.io/component-base v0.0.0-20260911142345-c58055e8ea54

replace k8s.io/component-helpers => k8s.io/component-helpers v0.20.0-alpha.2.0.20260909235451-037166a3854b

replace k8s.io/controller-manager => k8s.io/controller-manager v0.20.0-alpha.1.0.20260914231024-93b629e26fd7

replace k8s.io/cri-api => k8s.io/cri-api v0.38.0-alpha.0.0.20260911150216-8f649dc10621

replace k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.0.0-20260908232008-15b78256838b

replace k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.0.0-20260915192524-08d24b2331e2

replace k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.0.0-20260911151354-2c33c376fc14

replace k8s.io/kube-proxy => k8s.io/kube-proxy v0.0.0-20260911150020-37c174d8c41e

replace k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.0.0-20260915112550-b1ca9017534c

replace k8s.io/kubectl => k8s.io/kubectl v0.0.0-20260915204936-6b537a5ba8e5

replace k8s.io/kubelet => k8s.io/kubelet v0.0.0-20260911150729-4427ac790632

replace k8s.io/metrics => k8s.io/metrics v0.0.0-20260908225054-0d348d155ae1

replace k8s.io/mount-utils => k8s.io/mount-utils v0.38.0-alpha.0.0.20260904192117-7d92b1b47f5e

replace k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.0.0-20260914223949-d464eb89bbb3

replace k8s.io/sample-cli-plugin => k8s.io/sample-cli-plugin v0.0.0-20260908225600-778040e77db8

replace k8s.io/sample-controller => k8s.io/sample-controller v0.0.0-20260910081544-473a527804a0

replace k8s.io/pod-security-admission => k8s.io/pod-security-admission v0.22.0-beta.0.0.20260914232632-06937cca2812

replace k8s.io/dynamic-resource-allocation => k8s.io/dynamic-resource-allocation v0.26.0-beta.0.0.20260916032245-68a37f254f41

replace k8s.io/kms => k8s.io/kms v0.27.0-alpha.0.0.20260911142736-6cba54f24502

replace k8s.io/endpointslice => k8s.io/endpointslice v0.28.0-alpha.4.0.20260911153116-87371c499e9a

replace k8s.io/cri-client => k8s.io/cri-client v0.31.0-alpha.0.0.20260911150403-9d48a93aabfc

replace k8s.io/externaljwt => k8s.io/externaljwt v0.38.0-alpha.0.0.20260911153257-8482a20ad3dd

replace k8s.io/cri-streaming => k8s.io/cri-streaming v0.36.0-alpha.2.0.20260911150544-e8a01d8d7768

replace k8s.io/streaming => k8s.io/streaming v0.38.0-alpha.0.0.20260914154742-f99df5dfe25e
