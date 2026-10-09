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

package config

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

const (
	// SchedulerConfigDefault is logged in place of AutoscalingOptions.SchedulerConfig
	// when --scheduler-config-file isn't set.
	SchedulerConfigDefault = "default"
	// SchedulerConfigCustom is logged in place of AutoscalingOptions.SchedulerConfig
	// when it was loaded from --scheduler-config-file.
	SchedulerConfigCustom = "custom (--scheduler-config-file)"
)

// LoggableAutoscalingOptions is AutoscalingOptions that can be passed as a
// value to structured logging calls. Passing AutoscalingOptions directly makes
// loggers encode it as JSON, which prints durations as nanoseconds,
// SchedulerConfig as the whole scheduler config (or null), and interface
// fields as {}.
//
// In the logged value SchedulerConfig only says whether a custom scheduler
// config is used, and interface fields (implementations set in code, not
// configuration) are left out.
type LoggableAutoscalingOptions AutoscalingOptions

// logField is a single logged field. value is either a leaf value or a
// []logField for a nested struct.
type logField struct {
	name  string
	value interface{}
}

// String implements fmt.Stringer, used by loggers preferring text output.
func (o LoggableAutoscalingOptions) String() string {
	var b strings.Builder
	writeLogFields(&b, o.logFields())
	return b.String()
}

// MarshalLog implements logr.Marshaler, used by loggers preferring structured output.
func (o LoggableAutoscalingOptions) MarshalLog() interface{} {
	return logFieldsToMap(o.logFields())
}

func (o LoggableAutoscalingOptions) logFields() []logField {
	fields := structLogFields(reflect.ValueOf(AutoscalingOptions(o)))
	for i := range fields {
		if fields[i].name == "SchedulerConfig" {
			fields[i].value = SchedulerConfigDefault
			if o.SchedulerConfig != nil {
				fields[i].value = SchedulerConfigCustom
			}
		}
	}
	return fields
}

// structLogFields returns the exported fields of the struct v in declaration
// order. Nested structs are expanded, interface fields are skipped.
func structLogFields(v reflect.Value) []logField {
	fields := make([]logField, 0, v.NumField())
	for i := 0; i < v.NumField(); i++ {
		field, value := v.Type().Field(i), v.Field(i)
		if !field.IsExported() || value.Kind() == reflect.Interface {
			continue
		}
		if value.Kind() == reflect.Struct {
			fields = append(fields, logField{name: field.Name, value: structLogFields(value)})
			continue
		}
		fields = append(fields, logField{name: field.Name, value: value.Interface()})
	}
	return fields
}

// writeLogFields writes fields in the same format as fmt's %+v.
func writeLogFields(b *strings.Builder, fields []logField) {
	b.WriteByte('{')
	for i, f := range fields {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(f.name)
		b.WriteByte(':')
		if nested, ok := f.value.([]logField); ok {
			writeLogFields(b, nested)
		} else {
			fmt.Fprintf(b, "%+v", f.value)
		}
	}
	b.WriteByte('}')
}

func logFieldsToMap(fields []logField) map[string]interface{} {
	m := make(map[string]interface{}, len(fields))
	for _, f := range fields {
		switch value := f.value.(type) {
		case []logField:
			m[f.name] = logFieldsToMap(value)
		case time.Duration:
			m[f.name] = value.String()
		default:
			m[f.name] = value
		}
	}
	return m
}
