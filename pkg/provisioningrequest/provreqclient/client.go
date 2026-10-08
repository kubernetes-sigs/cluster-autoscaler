/*
Copyright 2023 The Kubernetes Authors.

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

package provreqclient

import (
	"context"
	"fmt"
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	v1ac "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/client/applyconfiguration/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/client/clientset/versioned"
	listers "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/client/listers/autoscaling.x-k8s.io/v1"
	corev1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/utils/clock"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"

	klog "k8s.io/klog/v2"
)

const (
	provisioningRequestClientCallTimeout = 4 * time.Second
	// pendingAdmissionTTL is how long an admission is shown without reading the API, while the
	// informer cache doesn't show it. After that, the API decides what is shown, because a cache
	// that lags this far behind can't show that the request was admitted, or that it changed since.
	pendingAdmissionTTL = 10 * time.Minute
	// pendingAdmissionCheckInterval is how often the API is read for a request after
	// pendingAdmissionTTL, until the cache catches up. In between, and while the API can't be
	// read, the newest state known for the request is shown.
	pendingAdmissionCheckInterval = time.Minute
	// maxPendingAdmissionChecks bounds the API reads made by a single read of the client, so that
	// a stalled cache doesn't flood the API server, whose rate limit CA's own writes share.
	maxPendingAdmissionChecks = 2
)

// ProvisioningRequestClient represents client for v1 ProvReq CRD.
type ProvisioningRequestClient struct {
	client         versioned.Interface
	provReqLister  listers.ProvisioningRequestLister
	podTemplLister corev1.PodTemplateLister
	// pendingAdmissions holds requests admitted by a successful write that the informer cache
	// doesn't show yet, so that selection and capacity booking see the admission right away.
	// admissionsMu guards them, and is never held while calling the API.
	admissionsMu      sync.Mutex
	pendingAdmissions map[types.NamespacedName]pendingAdmission
	// admissionVersion numbers the writes recorded in pendingAdmissions.
	admissionVersion uint64
	clock            clock.PassiveClock
}

type pendingAdmission struct {
	// request is the newest state known for the request: the write recorded last, or what the
	// API returned when it was read since.
	request *v1.ProvisioningRequest
	// deleted reports that the API didn't have the request when it was read last.
	deleted bool
	written metav1.Condition
	// acknowledged is when the API server accepted the admission.
	acknowledged time.Time
	// checked is when the API was read for the request last.
	checked time.Time
	// version identifies the write recorded last, so that an API read that raced with a newer
	// write doesn't replace it.
	version uint64
}

// admissionCheck is an API read of a request whose admission the cache didn't show in time.
type admissionCheck struct {
	key     types.NamespacedName
	version uint64
	current *v1.ProvisioningRequest
	err     error
}

// supersededBy reports whether the cached request shows the written condition or a later
// transition of it. Transition times only have a resolution of a second, so a transition to
// another status in the same second may be older than the write; it doesn't count.
func (a pendingAdmission) supersededBy(cached *v1.ProvisioningRequest) bool {
	observed := apimeta.FindStatusCondition(cached.Status.Conditions, a.written.Type)
	if observed == nil {
		return false
	}
	if observed.Status == a.written.Status {
		return !observed.LastTransitionTime.Before(&a.written.LastTransitionTime)
	}
	return a.written.LastTransitionTime.Before(&observed.LastTransitionTime)
}

// caughtUp reports whether the cache shows the newest state known for the request. Resource
// versions are opaque, so only their equality is meaningful.
func (a pendingAdmission) caughtUp(cached *v1.ProvisioningRequest) bool {
	return !a.deleted && cached.UID == a.request.UID && cached.ResourceVersion != "" &&
		cached.ResourceVersion == a.request.ResourceVersion
}

// NewProvisioningRequestClient supports dependency injection.
// kubeclient, provreq lister and podtemplate lister are initialized outside of the ProvisioningRequestClient.
func NewProvisioningRequestClient(
	client versioned.Interface,
	provReqLister listers.ProvisioningRequestLister,
	podTemplLister corev1.PodTemplateLister,
) *ProvisioningRequestClient {
	return &ProvisioningRequestClient{
		client:            client,
		provReqLister:     provReqLister,
		podTemplLister:    podTemplLister,
		pendingAdmissions: make(map[types.NamespacedName]pendingAdmission),
		clock:             clock.RealClock{},
	}
}

// ProvisioningRequest gets a specific ProvisioningRequest CR, including an admission written by
// this client that the informer cache doesn't show yet. The caller may modify the returned
// request, but not its pod templates, which are shared with the informer cache.
func (c *ProvisioningRequestClient) ProvisioningRequest(ctx context.Context, namespace, name string) (*provreqwrapper.ProvisioningRequest, error) {
	ctx, cancel := context.WithTimeout(ctx, provisioningRequestClientCallTimeout)
	defer cancel()
	v1PRs, err := c.withPendingAdmissions(ctx, func() ([]*v1.ProvisioningRequest, error) {
		v1PR, err := c.provReqLister.ProvisioningRequests(namespace).Get(name)
		if err != nil {
			if errors.IsNotFound(err) {
				delete(c.pendingAdmissions, types.NamespacedName{Namespace: namespace, Name: name})
			}
			return nil, err
		}
		return []*v1.ProvisioningRequest{v1PR}, nil
	})
	if err != nil {
		return nil, err
	}
	if len(v1PRs) == 0 {
		return nil, errors.NewNotFound(v1.Resource("provisioningrequests"), name)
	}
	podTemplates, err := c.FetchPodTemplates(ctx, v1PRs[0])
	if err != nil {
		return nil, fmt.Errorf("while fetching pod templates for Get Provisioning Request %s/%s got error: %v", namespace, name, err)
	}
	return provreqwrapper.NewProvisioningRequest(v1PRs[0], podTemplates), nil
}

// ProvisioningRequests gets all ProvisioningRequest CRs, with the same view of admissions as
// ProvisioningRequest. The caller may modify the returned requests, but not their pod templates,
// which are shared with the informer cache.
func (c *ProvisioningRequestClient) ProvisioningRequests(ctx context.Context) ([]*provreqwrapper.ProvisioningRequest, error) {
	// One time limit covers the API reads for all requests.
	ctx, cancel := context.WithTimeout(ctx, provisioningRequestClientCallTimeout)
	defer cancel()
	v1PRs, err := c.withPendingAdmissions(ctx, func() ([]*v1.ProvisioningRequest, error) {
		cached, err := c.provReqLister.List(labels.Everything())
		if err != nil {
			return nil, fmt.Errorf("error fetching provisioningRequests: %w", err)
		}
		present := make(map[types.NamespacedName]bool, len(cached))
		for _, v1PR := range cached {
			present[types.NamespacedName{Namespace: v1PR.Namespace, Name: v1PR.Name}] = true
		}
		for key := range c.pendingAdmissions {
			if !present[key] {
				delete(c.pendingAdmissions, key)
			}
		}
		return cached, nil
	})
	if err != nil {
		return nil, err
	}
	prs := make([]*provreqwrapper.ProvisioningRequest, 0, len(v1PRs))
	for _, v1PR := range v1PRs {
		podTemplates, errPodTemplates := c.FetchPodTemplates(ctx, v1PR)
		if errPodTemplates != nil {
			return nil, fmt.Errorf("while fetching pod templates for List Provisioning Request %s/%s got error: %v", v1PR.Namespace, v1PR.Name, errPodTemplates)
		}
		prs = append(prs, provreqwrapper.NewProvisioningRequest(v1PR, podTemplates))
	}
	return prs, nil
}

// withPendingAdmissions reads requests from the informer cache with read, which is called with
// admissionsMu held, and returns copies of them with pending admissions shown in their place.
// Once an admission is older than pendingAdmissionTTL, the newest state known for the request is
// shown instead, and the API is read for it about every pendingAdmissionCheckInterval until the
// cache shows that state. Requests the API no longer has are left out. The API is read without
// holding admissionsMu, and failing to read it doesn't fail the read.
func (c *ProvisioningRequestClient) withPendingAdmissions(ctx context.Context, read func() ([]*v1.ProvisioningRequest, error)) ([]*v1.ProvisioningRequest, error) {
	c.admissionsMu.Lock()
	cached, err := read()
	if err != nil {
		c.admissionsMu.Unlock()
		return nil, err
	}
	checks := c.claimAdmissionChecks(ctx, cached)
	if len(checks) > 0 {
		c.admissionsMu.Unlock()
		for i := range checks {
			if checks[i].err = ctx.Err(); checks[i].err == nil {
				checks[i].current, checks[i].err = c.client.AutoscalingV1().ProvisioningRequests(checks[i].key.Namespace).Get(ctx, checks[i].key.Name, metav1.GetOptions{})
			}
		}
		c.admissionsMu.Lock()
		c.applyAdmissionChecks(ctx, checks)
		// Another read may have retired an admission meanwhile, because it saw a newer cache, so
		// read the cache again: the snapshot from before the API calls may not show the admission,
		// or may still show a request that was deleted.
		cached, err = read()
		if err != nil {
			c.admissionsMu.Unlock()
			return nil, err
		}
	}
	defer c.admissionsMu.Unlock()
	shown := make([]*v1.ProvisioningRequest, 0, len(cached))
	for _, v1PR := range cached {
		if request := c.shownRequest(v1PR); request != nil {
			shown = append(shown, request)
		}
	}
	return shown, nil
}

// claimAdmissionChecks picks requests whose admission the cache didn't show in time and that are
// due for an API read, and records them as read now, so that concurrent reads don't read them
// too. The caller must hold admissionsMu.
func (c *ProvisioningRequestClient) claimAdmissionChecks(ctx context.Context, cached []*v1.ProvisioningRequest) []admissionCheck {
	if ctx.Err() != nil {
		return nil
	}
	now := c.clock.Now()
	var checks []admissionCheck
	for _, v1PR := range cached {
		if len(checks) == maxPendingAdmissionChecks {
			break
		}
		key := types.NamespacedName{Namespace: v1PR.Namespace, Name: v1PR.Name}
		pending, found := c.pendingAdmissions[key]
		if !found || now.Before(pending.acknowledged.Add(pendingAdmissionTTL)) ||
			now.Before(pending.checked.Add(pendingAdmissionCheckInterval)) || pending.caughtUp(v1PR) {
			continue
		}
		pending.checked = now
		c.pendingAdmissions[key] = pending
		checks = append(checks, admissionCheck{key: key, version: pending.version})
	}
	return checks
}

// applyAdmissionChecks records what the API returned as the newest state known for each request,
// unless a newer write was recorded for it meanwhile. The caller must hold admissionsMu.
func (c *ProvisioningRequestClient) applyAdmissionChecks(ctx context.Context, checks []admissionCheck) {
	for _, check := range checks {
		pending, found := c.pendingAdmissions[check.key]
		if !found || pending.version != check.version {
			continue
		}
		switch {
		case errors.IsNotFound(check.err):
			pending.deleted = true
		case check.err != nil:
			klog.FromContext(ctx).Error(check.err, "Failed to read a ProvisioningRequest whose admission the informer cache doesn't show; showing the newest state known",
				"provReq", klog.KRef(check.key.Namespace, check.key.Name))
			continue
		default:
			pending.request, pending.deleted = check.current, false
		}
		c.pendingAdmissions[check.key] = pending
	}
}

// shownRequest returns a copy of what is shown for a cached request, or nil if the API no longer
// has it, and retires the pending admission once it's no longer needed. The caller must hold
// admissionsMu.
func (c *ProvisioningRequestClient) shownRequest(cached *v1.ProvisioningRequest) *v1.ProvisioningRequest {
	key := types.NamespacedName{Namespace: cached.Namespace, Name: cached.Name}
	pending, found := c.pendingAdmissions[key]
	if !found {
		return cached.DeepCopy()
	}
	if c.clock.Now().Before(pending.acknowledged.Add(pendingAdmissionTTL)) {
		if cached.UID != pending.request.UID || pending.supersededBy(cached) ||
			apimeta.IsStatusConditionTrue(cached.Status.Conditions, v1.Failed) ||
			apimeta.IsStatusConditionTrue(cached.Status.Conditions, v1.BookingExpired) {
			delete(c.pendingAdmissions, key)
			return cached.DeepCopy()
		}
		return pending.request.DeepCopy()
	}
	// After the TTL, only the API shows whether the cache has caught up: a cached state that
	// supersedes the admission may still be older than what the API returned since.
	if pending.caughtUp(cached) {
		delete(c.pendingAdmissions, key)
		return cached.DeepCopy()
	}
	if pending.deleted {
		return nil
	}
	return pending.request.DeepCopy()
}

// rememberAdmission records an acknowledged write, never a tentative or failed one.
func (c *ProvisioningRequestClient) rememberAdmission(pr *v1.ProvisioningRequest) {
	c.admissionsMu.Lock()
	defer c.admissionsMu.Unlock()
	key := types.NamespacedName{Namespace: pr.Namespace, Name: pr.Name}
	c.admissionVersion++
	if apimeta.IsStatusConditionTrue(pr.Status.Conditions, v1.Provisioned) {
		condition := apimeta.FindStatusCondition(pr.Status.Conditions, v1.Provisioned)
		for _, terminal := range []string{v1.Failed, v1.BookingExpired} {
			if apimeta.IsStatusConditionTrue(pr.Status.Conditions, terminal) {
				condition = apimeta.FindStatusCondition(pr.Status.Conditions, terminal)
				break
			}
		}
		c.pendingAdmissions[key] = pendingAdmission{request: pr.DeepCopy(), written: *condition, acknowledged: c.clock.Now(), version: c.admissionVersion}
		return
	}
	pending, found := c.pendingAdmissions[key]
	if !found {
		return
	}
	if c.clock.Now().Before(pending.acknowledged.Add(pendingAdmissionTTL)) {
		delete(c.pendingAdmissions, key)
		return
	}
	// After the TTL, the cache may still show the old admission, so the API keeps deciding what
	// is shown; until it's read again, that's this write.
	pending.request, pending.deleted, pending.version = pr.DeepCopy(), false, c.admissionVersion
	c.pendingAdmissions[key] = pending
}

// FetchPodTemplates fetches PodTemplates referenced by the Provisioning Request.
func (c *ProvisioningRequestClient) FetchPodTemplates(ctx context.Context, pr *v1.ProvisioningRequest) ([]*apiv1.PodTemplate, error) {
	logger := klog.FromContext(ctx)
	podTemplates := make([]*apiv1.PodTemplate, 0, len(pr.Spec.PodSets))
	for _, podSpec := range pr.Spec.PodSets {
		podTemplate, err := c.podTemplLister.PodTemplates(pr.Namespace).Get(podSpec.PodTemplateRef.Name)
		if errors.IsNotFound(err) {
			logger.Info("Received not found error while fetching Pod Template for Provisioning Request", "provReq", klog.KObj(pr))
			continue
		} else if err != nil {
			return nil, err
		}
		podTemplates = append(podTemplates, podTemplate)
	}
	return podTemplates, nil
}

// UpdateProvisioningRequest updates the given ProvisioningRequest CR by propagating the changes using the ProvisioningRequestInterface and returns the updated instance or the original one in case of an error.
func (c *ProvisioningRequestClient) UpdateProvisioningRequest(ctx context.Context, pr *v1.ProvisioningRequest) (*v1.ProvisioningRequest, error) {
	logger := klog.FromContext(ctx)
	ctx, cancel := context.WithTimeout(ctx, provisioningRequestClientCallTimeout)
	defer cancel()

	updatedPr, err := c.client.AutoscalingV1().ProvisioningRequests(pr.Namespace).UpdateStatus(ctx, pr, metav1.UpdateOptions{})
	if err != nil {
		return pr, err
	}
	c.rememberAdmission(updatedPr)
	logger.V(4).Info("Updated ProvisioningRequest", "provReq", klog.KObj(updatedPr), "status", updatedPr.Status)
	return updatedPr, nil
}

// ApplyProvisioningRequest applies the given ProvisioningRequest with Server-Side Apply.
func (c *ProvisioningRequestClient) ApplyProvisioningRequest(prAC *v1ac.ProvisioningRequestApplyConfiguration, fieldOwner string) (*v1.ProvisioningRequest, error) {
	ctx, cancel := context.WithTimeout(context.Background(), provisioningRequestClientCallTimeout)
	defer cancel()
	if prAC.Name == nil || prAC.Namespace == nil {
		return nil, fmt.Errorf("failed to apply ProvisioningRequest: name and namespace are required")
	}
	pr, err := c.client.AutoscalingV1().ProvisioningRequests(*prAC.Namespace).ApplyStatus(ctx, prAC, metav1.ApplyOptions{FieldManager: fieldOwner, Force: true})
	if err != nil {
		return nil, err
	}
	c.rememberAdmission(pr)
	return pr, nil
}

// ProvisioningRequestsForPods returns the distinct ProvisioningRequests owning the supplied pods.
func ProvisioningRequestsForPods(ctx context.Context, client *ProvisioningRequestClient, unschedulablePods []*apiv1.Pod) []*provreqwrapper.ProvisioningRequest {
	logger := klog.FromContext(ctx)
	prMap := make(map[types.NamespacedName]*provreqwrapper.ProvisioningRequest)
	prList := []*provreqwrapper.ProvisioningRequest{}
	if len(unschedulablePods) == 0 {
		return prList
	}
	for _, pod := range unschedulablePods {
		if len(pod.OwnerReferences) == 0 {
			logger.Error(nil, "Pod has no OwnerReference", "pod", klog.KObj(pod))
			continue
		}
		key := types.NamespacedName{Namespace: pod.Namespace, Name: pod.OwnerReferences[0].Name}
		if _, found := prMap[key]; found {
			continue
		}
		provReq, err := client.ProvisioningRequest(ctx, pod.Namespace, pod.OwnerReferences[0].Name)
		if err != nil {
			logger.Error(err, "Failed to retrieve ProvisioningRequest from unschedulable pod")
			continue
		}
		prMap[key] = provReq
	}
	for _, pr := range prMap {
		prList = append(prList, pr)
	}
	return prList
}

// DeleteProvisioningRequest deletes the given ProvisioningRequest CR using the ProvisioningRequestInterface and returns an error in case of failure.
func (c *ProvisioningRequestClient) DeleteProvisioningRequest(ctx context.Context, pr *v1.ProvisioningRequest) error {
	logger := klog.FromContext(ctx)
	ctx, cancel := context.WithTimeout(context.Background(), provisioningRequestClientCallTimeout)
	defer cancel()

	err := c.client.AutoscalingV1().ProvisioningRequests(pr.Namespace).Delete(ctx, pr.Name, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("error deleting ProvisioningRequest %s/%s: %w", pr.Namespace, pr.Name, err)
	}
	logger.V(4).Info("Deleted ProvisioningRequest", "provReq", klog.KObj(pr))
	return nil
}

// FilterOutProvisioningClass filters out ProvReqs that belongs to certain Provisioning Class
func FilterOutProvisioningClass(ctx context.Context, prList []*provreqwrapper.ProvisioningRequest, class string, checkCapacityProcessorInstance string) []*provreqwrapper.ProvisioningRequest {
	newPrList := []*provreqwrapper.ProvisioningRequest{}
	for _, pr := range prList {
		if matchesProvisioningClass(ctx, pr, class, checkCapacityProcessorInstance) {
			newPrList = append(newPrList, pr)
		}
	}
	return newPrList
}

func matchesProvisioningClass(ctx context.Context, pr *provreqwrapper.ProvisioningRequest, class string, checkCapacityProcessorInstance string) bool {
	switch class {
	case v1.ProvisioningClassCheckCapacity:
		return provisioningrequest.SupportedCheckCapacityClass(ctx, pr.ProvisioningRequest, checkCapacityProcessorInstance)
	case v1.ProvisioningClassBestEffortAtomicScaleUp:
		return pr.Spec.ProvisioningClassName == v1.ProvisioningClassBestEffortAtomicScaleUp
	default:
		return false
	}
}
