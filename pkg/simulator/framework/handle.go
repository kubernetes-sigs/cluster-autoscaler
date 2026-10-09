/*
Copyright 2024 The Kubernetes Authors.

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

package framework

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/client-go/informers"
	schedulerinterface "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler"
	schedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	schedulerconfiglatest "k8s.io/kubernetes/pkg/scheduler/apis/config/latest"
	schedulerimpl "k8s.io/kubernetes/pkg/scheduler/framework"
	schedulerplugins "k8s.io/kubernetes/pkg/scheduler/framework/plugins"
	noderesources "k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodevolumelimits"
	schedulerframeworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	schedulermetrics "k8s.io/kubernetes/pkg/scheduler/metrics"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/dynamicresources"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/extender/occupancy"
)

var (
	initMetricsOnce sync.Once
)

// Handle is meant for interacting with the scheduler framework.
type Handle struct {
	Framework        schedulerimpl.Framework
	DelegatingLister *DelegatingSchedulerSharedLister
	Extenders        []schedulerinterface.Extender
}

// NewHandle builds a framework Handle based on the provided informers and scheduler config.
func NewHandle(ctx context.Context, informerFactory informers.SharedInformerFactory, schedConfig *schedulerconfig.KubeSchedulerConfiguration, draEnabled bool, csiEnabled bool) (*Handle, error) {
	if schedConfig == nil {
		var err error
		schedConfig, err = schedulerconfiglatest.Default()
		if err != nil {
			return nil, fmt.Errorf("couldn't create scheduler config: %v", err)
		}
	}
	if len(schedConfig.Profiles) != 1 {
		return nil, fmt.Errorf("unexpected scheduler config: expected one scheduler profile only (found %d profiles)", len(schedConfig.Profiles))
	}
	if err := configureIgnoredExtenderResources(schedConfig); err != nil {
		return nil, fmt.Errorf("couldn't configure extender-managed resources: %v", err)
	}

	sharedLister := NewDelegatingSchedulerSharedLister()
	sharedCSIManager := nodevolumelimits.NewCSIManager(informerFactory.Storage().V1().CSINodes().Lister())

	opts := []schedulerframeworkruntime.Option{
		schedulerframeworkruntime.WithInformerFactory(informerFactory),
		schedulerframeworkruntime.WithSnapshotSharedLister(sharedLister),
		schedulerframeworkruntime.WithSharedCSIManager(sharedCSIManager),
	}

	if draEnabled {
		opts = append(opts, schedulerframeworkruntime.WithSharedDRAManager(sharedLister))
	} else {
		opts = append(opts, schedulerframeworkruntime.WithSharedDRAManager(dynamicresources.NewNoOpDRAManager()))
	}

	// TODO: We should always use sharedLister once this CSINode aware changes in CAS are
	// enabled by default.
	if csiEnabled {
		opts = append(opts, schedulerframeworkruntime.WithSharedCSIManager(sharedLister))
	} else {
		sharedCSIManager := nodevolumelimits.NewCSIManager(informerFactory.Storage().V1().CSINodes().Lister())
		opts = append(opts, schedulerframeworkruntime.WithSharedCSIManager(sharedCSIManager))
	}
	initMetricsOnce.Do(func() {
		schedulermetrics.InitMetrics()
	})
	framework, err := schedulerframeworkruntime.NewFramework(
		ctx,
		schedulerplugins.NewInTreeRegistry(),
		&schedConfig.Profiles[0],
		opts...,
	)

	if err != nil {
		return nil, fmt.Errorf("couldn't create scheduler framework; %v", err)
	}

	var extenders []schedulerinterface.Extender
	for i := range schedConfig.Extenders {
		extender, err := scheduler.NewHTTPExtender(&schedConfig.Extenders[i])
		if err != nil {
			return nil, fmt.Errorf("couldn't create HTTP extender for %q: %v", schedConfig.Extenders[i].URLPrefix, err)
		}
		extenders = append(extenders, extender)
	}

	return &Handle{
		Framework:        framework,
		DelegatingLister: sharedLister,
		Extenders:        extenders,
	}, nil
}

// configureIgnoredExtenderResources configures NodeResourcesFit to leave resources
// marked IgnoredByScheduler to the corresponding scheduler extenders. This mirrors
// kube-scheduler's extender setup and allows extenders to evaluate resources that
// are not present in a node group's template node.
func configureIgnoredExtenderResources(schedConfig *schedulerconfig.KubeSchedulerConfiguration) error {
	var ignoredResources []string
	for _, extender := range schedConfig.Extenders {
		for _, managedResource := range extender.ManagedResources {
			if managedResource.IgnoredByScheduler {
				ignoredResources = append(ignoredResources, managedResource.Name)
			}
		}
	}
	if len(ignoredResources) == 0 {
		return nil
	}

	for i := range schedConfig.Profiles {
		profile := &schedConfig.Profiles[i]
		found := false
		for j := range profile.PluginConfig {
			if profile.PluginConfig[j].Name != noderesources.Name {
				continue
			}

			args, ok := profile.PluginConfig[j].Args.(*schedulerconfig.NodeResourcesFitArgs)
			if !ok {
				return fmt.Errorf("want args to be of type NodeResourcesFitArgs, got %T", profile.PluginConfig[j].Args)
			}
			args.IgnoredResources = ignoredResources
			found = true
			break
		}
		if !found {
			return fmt.Errorf("can't find NodeResourcesFitArgs in plugin config")
		}
	}
	return nil
}

// ConfigureExtenderOccupancy opts selected extender URLs into complete occupancy filtering.
func (h *Handle) ConfigureExtenderOccupancy(cfg *schedulerconfig.KubeSchedulerConfiguration, urls []string) error {
	for _, url := range urls {
		found := false
		if cfg != nil {
			for i, c := range cfg.Extenders {
				if c.URLPrefix == url {
					if _, ok := h.Extenders[i].(*occupancy.Extender); ok {
						found = true
						continue
					}
					adapter, err := occupancy.New(h.Extenders[i], &c)
					if err != nil {
						return err
					}
					h.Extenders[i] = adapter
					found = true
				}
			}
		}
		if !found {
			return fmt.Errorf("occupancy URL %q is not a configured extender", url)
		}
	}
	return nil
}
