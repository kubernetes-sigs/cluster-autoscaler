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

package loop

import "testing"

func TestLogTriggerReasonDoesNotDrainPendingPodSignal(t *testing.T) {
	podChan := make(chan any, 1)
	podChan <- struct{}{}
	trigger := &LoopTrigger{
		podObserver: &UnschedulablePodObserver{unschedulablePodChan: podChan},
	}

	trigger.logTriggerReason("Autoscaler loop triggered immediately after a scale up")

	select {
	case <-podChan:
	default:
		t.Fatal("logTriggerReason drained a pending unschedulable pod signal; the next Wait call will miss it")
	}
}
