/*
Copyright 2026 The Kubeflow authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func init() {
	SchemeBuilder.Register(&ExecutorScaling{}, &ExecutorScalingList{})
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=escaling,singular=executorscaling
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:JSONPath=.spec.target,name=Target,type=integer
// +kubebuilder:printcolumn:JSONPath=.status.admitted,name=Admitted,type=integer
// +kubebuilder:printcolumn:JSONPath=.spec.admissionMode,name=AdmissionMode,type=string
// +kubebuilder:printcolumn:JSONPath=.metadata.creationTimestamp,name=Age,type=date
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient

// ExecutorScaling is the scaling request written by a Spark driver.
//
// The driver writes spec.target and reads status.admitted. An admission
// controller may use spec.target to request resources from a scheduling
// backend and write the admitted executor count back to status.admitted.
type ExecutorScaling struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec   ExecutorScalingSpec   `json:"spec"`
	Status ExecutorScalingStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ExecutorScalingList contains a list of ExecutorScaling objects.
type ExecutorScalingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ExecutorScaling `json:"items"`
}

// ExecutorScalingSpec defines the desired executor allocation.
type ExecutorScalingSpec struct {
	// Target is the number of executors currently requested by the Spark
	// driver.
	// +kubebuilder:validation:Minimum=0
	Target int32 `json:"target"`

	// AdmissionMode controls whether an external admission controller owns
	// status.admitted. In default mode Spark performs loopback admission. In
	// injection mode an external controller, such as Kueue, supplies the
	// admitted count.
	// +kubebuilder:validation:Enum=default;injection
	AdmissionMode string `json:"admissionMode"`
}

// ExecutorScalingStatus defines the observed admission result.
type ExecutorScalingStatus struct {
	// Admitted is the number of executors that the admission controller
	// currently allows the Spark driver to create.
	// +optional
	Admitted *int32 `json:"admitted,omitempty"`
}

var _ runtime.Object = &ExecutorScaling{}
var _ runtime.Object = &ExecutorScalingList{}
