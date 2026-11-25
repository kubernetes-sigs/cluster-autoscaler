# Code Conventions and API Policy

This document outlines the coding standards and API policies for the `cluster-autoscaler` project. As Cluster Autoscaler evolves into a reusable library, maintaining API stability and backward compatibility is crucial for downstream consumers and integrators.

## API Stability Policy

Directly modifying function signatures of exported functions breaks backward compatibility and disrupts downstream library consumers. To prevent this, contributors should follow these guidelines when changing or extending public APIs.

### 1. Prefer Extensible Signatures

When introducing new exported functions, design them to be extensible from the beginning. Avoid long lists of positional arguments.

#### Pattern A: Configuration Structs

Group parameters into a configuration struct. Adding fields to a struct is generally non-breaking, provided zero/nil values are handled gracefully.

```go
// NodeInfo Example

type NodeInfoConfig struct {
    Node    *apiv1.Node
    Pods    []*PodInfo
    CSINode *storagev1.CSINode
}

func NewNodeInfo(cfg NodeInfoConfig) *NodeInfo
```

Alternatively preserve a set of **required** parameters.

```go
func NewNodeInfo(node *apiv1.Node, cfg NodeInfoConfig) *NodeInfo
```

#### Pattern B: Functional Options (Variadic Options)

Use functional options to allow adding parameters in the future without breaking existing calls. This is the recommended pattern for complex constructors.

```go
// NodeInfo Example

// Extensible Signature
func NewNodeInfo(node *apiv1.Node, opts ...NodeInfoOption) *NodeInfo

type NodeInfoOption func(*NodeInfo)

func WithCSINode(csiNode *storagev1.CSINode) NodeInfoOption {
    return func(n *NodeInfo) {
        n.CSINode = csiNode
    }
}
```

This is recommended when the number of options is relatively small and not expected to grow intensively. 

### 2. Deprecation Policy

When an exported API *must* be changed in a breaking way or replaced:

1.  **Do not remove or change the old function signature immediately.**
2.  **Introduce the new API alongside the old one.**
3.  **Mark the old API as Deprecated:** Use a standard Go deprecation comment.
    ```go
    // Deprecated: Use NewNodeInfoV2 instead.
    func NewNodeInfo(node *apiv1.Node, ...) *NodeInfo
    ```
4.  **Retention:** Retain the deprecated API for at least **two minor release cycles** (or as agreed by maintainers) before removal to give downstream consumers time to migrate.

---

*Note: These guidelines apply primarily to exported APIs (public interfaces, functions, and structs). Internal utilities may be modified more freely, but caution is advised.*
