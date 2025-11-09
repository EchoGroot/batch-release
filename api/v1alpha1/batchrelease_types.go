/*
Copyright 2025.

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

package v1alpha1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// BatchReleaseSpec defines the desired state of BatchRelease
type BatchReleaseSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	WorkloadRef *WorkloadRef `json:"workloadRef"`
	Strategy    *Strategy    `json:"strategy"`
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Schemaless
	Template v1.PodTemplateSpec `json:"template" protobuf:"bytes,3,opt,name=template"`
}

type WorkloadRef struct {
	ApiVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
}

type Strategy struct {
	Steps []Step `json:"steps"`
}

type Step struct {
	Replicas intstr.IntOrString `json:"replicas"`
}

type Phase string

const (
	PhaseInitial       Phase = "Initial"
	PhaseRollingUpdate Phase = "RollingUpdate"
	PhaseFinalizing    Phase = "Finalizing"
	PhaseCompleted     Phase = "Completed"
)

type StepState string

const (
	StepStateInitial   StepState = "Initial"
	StepStateUpgrade   StepState = "Upgrade"
	StepStateBlocking  StepState = "Blocking"
	StepStateCompleted StepState = "Completed"
)

const (
	StepBlockingMessage = "Step is in blocking state and needs to be continued manually"
)

// BatchReleaseStatus defines the observed state of BatchRelease.
type BatchReleaseStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	Phase                Phase              `json:"phase,omitempty"`
	CurrentStepIndex     int32              `json:"currentStepIndex,omitempty"`
	CurrentStepState     StepState          `json:"currentStepState,omitempty"`
	UpdatedReadyReplicas int32              `json:"updatedReadyReplicas,omitempty"`
	Reason               BatchReleaseReason `json:"reason,omitempty"`
	Message              string             `json:"message,omitempty"`
	ObservedGeneration   int64              `json:"observedGeneration,omitempty"`
	LastUpdateTime       *metav1.Time       `json:"lastUpdateTime,omitempty"`
}

type BatchReleaseReason string

const (
	BatchReleaseReasonStepBlocking BatchReleaseReason = "StepBlocking"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// BatchRelease is the Schema for the batchreleases API
type BatchRelease struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of BatchRelease
	// +required
	Spec BatchReleaseSpec `json:"spec"`

	// status defines the observed state of BatchRelease
	// +optional
	Status BatchReleaseStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// BatchReleaseList contains a list of BatchRelease
type BatchReleaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BatchRelease `json:"items"`
}

func init() {
	SchemeBuilder.Register(&BatchRelease{}, &BatchReleaseList{})
}

const BatchReleaseControlInfoAnno = "batch-release.rollouts.yuyy.com/control-info"
