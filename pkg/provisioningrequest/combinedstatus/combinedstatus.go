/*
Copyright The Kubernetes Authors.

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

// Package combinedstatus aggregates the per-ProvisioningRequest ScaleUpStatuses
// produced while processing a batch of ProvisioningRequests into the single
// ScaleUpStatus that the scale-up orchestrator contract expects.
package combinedstatus

import (
	"fmt"
	"sort"
	"strings"

	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
)

// resultPriority represents the priority of a ScaleUpResult. The final result of a
// batch is the one with the highest priority in the set:
//   - If even one ScaleUpSuccessful is present, the final result is ScaleUpSuccessful.
//   - If not, and even one ScaleUpError is present, the final result is ScaleUpError.
//   - If not, and even one ScaleUpNoOptionsAvailable is present, the final result is
//     ScaleUpNoOptionsAvailable.
//   - If not, and even one ScaleUpNotNeeded is present, the final result is ScaleUpNotNeeded.
//   - Otherwise the final result is ScaleUpNotTried.
//
// Reporting ScaleUpSuccessful whenever any single ProvisioningRequest in the batch was
// provisioned is deliberate: it makes the LoopTrigger start the next iteration
// immediately, which is what drains a backlog of ProvisioningRequests quickly.
var resultPriority = map[status.ScaleUpResult]int{
	status.ScaleUpNotTried:           0,
	status.ScaleUpNotNeeded:          1,
	status.ScaleUpNoOptionsAvailable: 2,
	status.ScaleUpError:              3,
	status.ScaleUpSuccessful:         4,
}

// Set combines multiple ScaleUpStatuses into one. It keeps track of the best result
// and all errors that occurred during the ScaleUp process.
type Set struct {
	Result        status.ScaleUpResult
	ScaleupErrors map[*errors.AutoscalerError]bool
}

// New creates a new, empty Set.
func New() Set {
	return Set{
		Result:        status.ScaleUpNotTried,
		ScaleupErrors: make(map[*errors.AutoscalerError]bool),
	}
}

// Add adds a ScaleUpStatus to the Set.
func (c *Set) Add(newStatus *status.ScaleUpStatus) {
	if newStatus == nil {
		return
	}
	if resultPriority[c.Result] < resultPriority[newStatus.Result] {
		c.Result = newStatus.Result
	}
	if newStatus.ScaleUpError != nil {
		if _, found := c.ScaleupErrors[newStatus.ScaleUpError]; !found {
			c.ScaleupErrors[newStatus.ScaleUpError] = true
		}
	}
}

// formatMessageFromBatchErrors formats a message from a list of errors.
func (c *Set) formatMessageFromBatchErrors(errs []errors.AutoscalerError) string {
	firstErr := errs[0]
	var builder strings.Builder
	builder.WriteString(firstErr.Error())
	builder.WriteString(" ...and other concurrent errors: [")
	formattedErrs := map[errors.AutoscalerError]bool{
		firstErr: true,
	}
	for _, err := range errs {
		if _, has := formattedErrs[err]; has {
			continue
		}
		formattedErrs[err] = true
		message := err.Error()
		if len(formattedErrs) > 2 {
			builder.WriteString(", ")
		}
		builder.WriteString(fmt.Sprintf("%q", message))
	}
	builder.WriteString("]")
	return builder.String()
}

// combineBatchScaleUpErrors combines multiple errors into one. If there is only one
// error, it returns that error. If there are multiple errors, it combines them into one
// error with a message that contains all the errors.
func (c *Set) combineBatchScaleUpErrors() *errors.AutoscalerError {
	if len(c.ScaleupErrors) == 0 {
		return nil
	}
	if len(c.ScaleupErrors) == 1 {
		for err := range c.ScaleupErrors {
			return err
		}
	}
	uniqueMessages := make(map[string]bool)
	for err := range c.ScaleupErrors {
		uniqueMessages[(*err).Error()] = true
	}
	if len(uniqueMessages) == 1 {
		for err := range c.ScaleupErrors {
			return err
		}
	}
	// sort to stabilize the results and easier log aggregation
	errs := make([]errors.AutoscalerError, 0, len(c.ScaleupErrors))
	for err := range c.ScaleupErrors {
		errs = append(errs, *err)
	}
	sort.Slice(errs, func(i, j int) bool {
		return errs[i].Error() < errs[j].Error()
	})
	message := c.formatMessageFromBatchErrors(errs)
	combinedErr := errors.NewAutoscalerError(errors.InternalError, message)
	return &combinedErr
}

// Export converts the Set into a ScaleUpStatus.
func (c *Set) Export() (*status.ScaleUpStatus, errors.AutoscalerError) {
	result := &status.ScaleUpStatus{Result: c.Result}
	if len(c.ScaleupErrors) > 0 {
		result.ScaleUpError = c.combineBatchScaleUpErrors()
	}

	var resErr errors.AutoscalerError

	if result.Result == status.ScaleUpError {
		resErr = *result.ScaleUpError
	}

	return result, resErr
}
