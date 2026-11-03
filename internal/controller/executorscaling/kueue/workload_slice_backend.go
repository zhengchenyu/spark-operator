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

// Package kueue contains the Kueue WorkloadSlicing admission backend for
// ExecutorScaling requests.
package kueue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	executorscalingv1alpha1 "github.com/kubeflow/spark-operator/v2/api/executorscalingv1alpha1"
	sparkv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

const (
	group   = "kueue.x-k8s.io"
	version = "v1beta1"
	kind    = "Workload"

	// WorkloadSliceReplacementFor is the annotation understood by Kueue's
	// workload-slicing scheduler.
	WorkloadSliceReplacementFor = "kueue.x-k8s.io/workload-slice-replacement-for"
	// ElasticJobAnnotation enables Kueue workload slicing for a Workload.
	ElasticJobAnnotation = "kueue.x-k8s.io/elastic-job"
	// ResourceInUseFinalizer protects an admitted Workload until Kueue releases
	// its quota reservation.
	ResourceInUseFinalizer = "kueue.x-k8s.io/resource-in-use"

	// ExecutorScalingUIDLabel identifies slices managed by this backend.
	ExecutorScalingUIDLabel = "spark.apache.org/executor-scaling-uid"
	// JobUIDLabel is copied by Kueue's SparkApplication integration.
	JobUIDLabel = "kueue.x-k8s.io/job-uid"

	executorPodSetName = "executor"
)

var workloadGVK = schema.GroupVersionKind{
	Group: group, Version: version, Kind: kind,
}

var workloadListGVK = schema.GroupVersionKind{
	Group: group, Version: version, Kind: kind + "List",
}

// WorkloadSliceBackend implements admission through Kueue Workload slices.
type WorkloadSliceBackend struct {
	client client.Client
}

var _ interface {
	Reconcile(context.Context, *executorscalingv1alpha1.ExecutorScaling) (*int32, error)
	Finalize(context.Context, *executorscalingv1alpha1.ExecutorScaling) error
} = (*WorkloadSliceBackend)(nil)

// NewWorkloadSliceBackend creates a backend using the operator's Kubernetes
// client. Kueue objects are intentionally accessed as unstructured objects so
// the operator does not import Kueue's full Go module.
func NewWorkloadSliceBackend(client client.Client) *WorkloadSliceBackend {
	return &WorkloadSliceBackend{client: client}
}

// WorkloadObject returns an unstructured Kueue Workload object suitable for
// controller-runtime caches and watches.
func WorkloadObject() *unstructured.Unstructured {
	workload := &unstructured.Unstructured{}
	workload.SetGroupVersionKind(workloadGVK)
	return workload
}

// WorkloadObject returns the object type watched by the ExecutorScaling
// controller.
func (b *WorkloadSliceBackend) WorkloadObject() client.Object {
	return WorkloadObject()
}

// MapWorkloadToRequests maps a Workload event to the ExecutorScaling named by
// the bridge label.
func (b *WorkloadSliceBackend) MapWorkloadToRequests(ctx context.Context, object client.Object) []reconcile.Request {
	uid := object.GetLabels()[ExecutorScalingUIDLabel]
	if uid != "" {
		scalings := &executorscalingv1alpha1.ExecutorScalingList{}
		if err := b.client.List(ctx, scalings, client.InNamespace(object.GetNamespace())); err != nil {
			return nil
		}
		for i := range scalings.Items {
			if string(scalings.Items[i].UID) == uid {
				return []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(&scalings.Items[i])}}
			}
		}
	}

	jobUID := object.GetLabels()[JobUIDLabel]
	if jobUID == "" {
		return nil
	}
	apps := &sparkv1beta2.SparkApplicationList{}
	if err := b.client.List(ctx, apps, client.InNamespace(object.GetNamespace())); err != nil {
		return nil
	}
	appID := ""
	for i := range apps.Items {
		if string(apps.Items[i].UID) == jobUID {
			appID = apps.Items[i].Status.SparkApplicationID
			break
		}
	}
	if appID == "" {
		return nil
	}

	scalings := &executorscalingv1alpha1.ExecutorScalingList{}
	if err := b.client.List(ctx, scalings, client.InNamespace(object.GetNamespace())); err != nil {
		return nil
	}
	for i := range scalings.Items {
		if scalings.Items[i].GetLabels()["spark-app-id"] == appID {
			return []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(&scalings.Items[i])}}
		}
	}
	return nil
}

// Reconcile creates or updates a Workload slice and returns the complete
// executor admission count currently available to Spark.
func (b *WorkloadSliceBackend) Reconcile(ctx context.Context, scaling *executorscalingv1alpha1.ExecutorScaling) (*int32, error) {
	if scaling.Spec.Target < 0 {
		return nil, fmt.Errorf("ExecutorScaling target must not be negative: %d", scaling.Spec.Target)
	}

	sparkApp, err := b.findSparkApplication(ctx, scaling)
	if err != nil {
		return nil, err
	}

	workloads, err := b.findActiveWorkloads(ctx, scaling, sparkApp)
	if err != nil {
		return nil, err
	}
	if len(workloads) == 0 {
		return nil, fmt.Errorf(
			"waiting for the Kueue Workload for SparkApplication %s/%s",
			sparkApp.Namespace, sparkApp.Name,
		)
	}
	if len(workloads) > 2 {
		return nil, fmt.Errorf("found more than two active Workload slices for ExecutorScaling %s/%s", scaling.Namespace, scaling.Name)
	}

	active, pending := selectWorkloads(workloads)
	target := scaling.Spec.Target
	activeCount, err := workloadPodSetCount(active, executorPodSetName)
	if err != nil {
		return nil, fmt.Errorf("reading active Workload %s/%s: %w", active.GetNamespace(), active.GetName(), err)
	}
	activeIsBridge := active.GetLabels()[ExecutorScalingUIDLabel] == string(scaling.UID)

	switch {
	case pending != nil && isAdmitted(pending):
		pendingCount, countErr := workloadPodSetCount(pending, executorPodSetName)
		if countErr != nil {
			return nil, countErr
		}
		if target < pendingCount {
			if err := updateWorkloadPodSetCount(ctx, b.client, pending, executorPodSetName, target); err != nil {
				return nil, err
			}
		}
	case pending != nil && target < activeCount && activeIsBridge:
		if err := deleteWorkload(ctx, b.client, pending); err != nil {
			return nil, err
		}
		if err := updateWorkloadPodSetCount(ctx, b.client, active, executorPodSetName, target); err != nil {
			return nil, err
		}
	case pending != nil && target < activeCount:
		if mustWorkloadPodSetCount(pending, executorPodSetName) != activeCount {
			if err := updateWorkloadPodSetCount(ctx, b.client, pending, executorPodSetName, activeCount); err != nil {
				return nil, err
			}
		}
	case pending != nil && target != mustWorkloadPodSetCount(pending, executorPodSetName):
		if err := updateWorkloadPodSetCount(ctx, b.client, pending, executorPodSetName, target); err != nil {
			return nil, err
		}
	case !isAdmitted(active) && !activeIsBridge && target != activeCount:
		if err := updateWorkloadPodSetCount(ctx, b.client, active, executorPodSetName, target); err != nil {
			return nil, err
		}
	case !activeIsBridge:
		replacementTarget := target
		if replacementTarget < activeCount {
			replacementTarget = activeCount
		}
		slice := newReplacementSlice(active, scaling, replacementTarget)
		if err := b.client.Create(ctx, slice); err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("creating Workload replacement slice: %w", err)
		}
	case target > activeCount:
		slice := newReplacementSlice(active, scaling, target)
		if err := b.client.Create(ctx, slice); err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("creating Workload replacement slice: %w", err)
		}
	case target != activeCount:
		if err := updateWorkloadPodSetCount(ctx, b.client, active, executorPodSetName, target); err != nil {
			return nil, err
		}
	}

	admitted, err := workloadAdmittedPodSetCount(active, executorPodSetName)
	if err != nil {
		return nil, fmt.Errorf("reading admitted count from Workload %s/%s: %w", active.GetNamespace(), active.GetName(), err)
	}
	if pending != nil && isAdmitted(pending) {
		admitted, err = workloadAdmittedPodSetCount(pending, executorPodSetName)
		if err != nil {
			return nil, fmt.Errorf("reading admitted count from replacement Workload %s/%s: %w", pending.GetNamespace(), pending.GetName(), err)
		}
	}
	if admitted < 0 {
		admitted = 0
	}
	if admitted > target {
		admitted = target
	}
	return ptr.To(admitted), nil
}

// Finalize deactivates and deletes all replacement slices owned by the
// ExecutorScaling object. Kueue remains responsible for removing its resource
// reservation finalizer and releasing quota.
func (b *WorkloadSliceBackend) Finalize(ctx context.Context, scaling *executorscalingv1alpha1.ExecutorScaling) error {
	workloads := &unstructured.UnstructuredList{}
	workloads.SetGroupVersionKind(workloadListGVK)
	if err := b.client.List(ctx, workloads, client.InNamespace(scaling.Namespace)); err != nil {
		return fmt.Errorf("listing ExecutorScaling Workload slices: %w", err)
	}

	for i := range workloads.Items {
		workload := &workloads.Items[i]
		if workload.GetLabels()[ExecutorScalingUIDLabel] != string(scaling.UID) {
			continue
		}
		if !workload.GetDeletionTimestamp().IsZero() {
			continue
		}
		updated := workload.DeepCopy()
		if err := unstructured.SetNestedField(updated.Object, false, "spec", "active"); err != nil {
			return fmt.Errorf("deactivating Workload %s/%s: %w", updated.GetNamespace(), updated.GetName(), err)
		}
		updated.SetFinalizers(removeString(updated.GetFinalizers(), ResourceInUseFinalizer))
		if err := b.client.Update(ctx, updated); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("releasing Workload %s/%s: %w", updated.GetNamespace(), updated.GetName(), err)
		}
		if err := b.client.Delete(ctx, updated); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting Workload %s/%s: %w", updated.GetNamespace(), updated.GetName(), err)
		}
	}
	return nil
}

func (b *WorkloadSliceBackend) findSparkApplication(
	ctx context.Context,
	scaling *executorscalingv1alpha1.ExecutorScaling,
) (*sparkv1beta2.SparkApplication, error) {
	appID := scaling.GetLabels()["spark-app-id"]
	if appID == "" {
		return nil, fmt.Errorf("ExecutorScaling %s/%s is missing the spark-app-id label", scaling.Namespace, scaling.Name)
	}

	pods := &corev1.PodList{}
	if err := b.client.List(ctx, pods, client.InNamespace(scaling.Namespace), client.MatchingLabels{
		common.LabelSparkApplicationSelector: appID,
		common.LabelSparkRole:                common.SparkRoleDriver,
	}); err != nil {
		return nil, fmt.Errorf("finding Spark driver Pod for application ID %q: %w", appID, err)
	}
	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("waiting for Spark driver Pod with %s=%s", common.LabelSparkApplicationSelector, appID)
	}

	appName := pods.Items[0].GetLabels()[common.LabelSparkAppName]
	if appName == "" {
		return nil, fmt.Errorf("Spark driver Pod %s/%s is missing the %s label", pods.Items[0].Namespace, pods.Items[0].Name, common.LabelSparkAppName)
	}
	app := &sparkv1beta2.SparkApplication{}
	if err := b.client.Get(ctx, types.NamespacedName{
		Namespace: scaling.Namespace,
		Name:      appName,
	}, app); err != nil {
		return nil, fmt.Errorf("getting SparkApplication %s/%s: %w", scaling.Namespace, appName, err)
	}
	return app, nil
}

func (b *WorkloadSliceBackend) findActiveWorkloads(
	ctx context.Context,
	scaling *executorscalingv1alpha1.ExecutorScaling,
	app *sparkv1beta2.SparkApplication,
) ([]*unstructured.Unstructured, error) {
	workloads := &unstructured.UnstructuredList{}
	workloads.SetGroupVersionKind(workloadListGVK)
	if err := b.client.List(ctx, workloads, client.InNamespace(scaling.Namespace)); err != nil {
		return nil, fmt.Errorf("listing Kueue Workloads: %w", err)
	}

	result := make([]*unstructured.Unstructured, 0, 2)
	for i := range workloads.Items {
		workload := &workloads.Items[i]
		if isFinished(workload) {
			continue
		}
		if workload.GetLabels()[ExecutorScalingUIDLabel] == string(scaling.UID) ||
			isOwnedBySparkApplication(workload, app) {
			result = append(result, workload)
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].GetCreationTimestamp().Time.Before(result[j].GetCreationTimestamp().Time)
	})
	return result, nil
}

func selectWorkloads(workloads []*unstructured.Unstructured) (active, pending *unstructured.Unstructured) {
	if len(workloads) == 2 {
		for i, candidate := range workloads {
			other := workloads[1-i]
			if candidate.GetAnnotations()[WorkloadSliceReplacementFor] == workloadKey(other) {
				return other, candidate
			}
		}
	}
	for _, workload := range workloads {
		if isAdmitted(workload) {
			active = workload
		} else if pending == nil {
			pending = workload
		}
	}
	if active == nil {
		active = workloads[0]
		if pending == active {
			pending = nil
		}
	}
	return active, pending
}

func workloadKey(workload *unstructured.Unstructured) string {
	return fmt.Sprintf("%s/%s", workload.GetNamespace(), workload.GetName())
}

func isOwnedBySparkApplication(workload *unstructured.Unstructured, app *sparkv1beta2.SparkApplication) bool {
	for _, owner := range workload.GetOwnerReferences() {
		if owner.UID == app.UID && owner.Name == app.Name && owner.Kind == "SparkApplication" {
			return true
		}
	}
	return workload.GetLabels()[JobUIDLabel] == string(app.UID)
}

func isFinished(workload *unstructured.Unstructured) bool {
	conditions, found, err := unstructured.NestedSlice(workload.Object, "status", "conditions")
	if err != nil || !found {
		return false
	}
	for _, item := range conditions {
		condition, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		if condition["type"] == "Finished" && condition["status"] == "True" {
			return true
		}
	}
	return false
}

func isAdmitted(workload *unstructured.Unstructured) bool {
	conditions, found, err := unstructured.NestedSlice(workload.Object, "status", "conditions")
	if err != nil || !found {
		return false
	}
	for _, item := range conditions {
		condition, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		if condition["type"] == "Admitted" && condition["status"] == "True" {
			return true
		}
	}
	return false
}

func workloadPodSetCount(workload *unstructured.Unstructured, name string) (int32, error) {
	podSets, found, err := unstructured.NestedSlice(workload.Object, "spec", "podSets")
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, fmt.Errorf("spec.podSets is missing")
	}
	for _, item := range podSets {
		podSet, ok := item.(map[string]interface{})
		if !ok || podSet["name"] != name {
			continue
		}
		value, ok, err := unstructured.NestedNumberAsFloat64(podSet, "count")
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, fmt.Errorf("podSet %q count is missing", name)
		}
		return int32(value), nil
	}
	return 0, fmt.Errorf("podSet %q is missing", name)
}

func mustWorkloadPodSetCount(workload *unstructured.Unstructured, name string) int32 {
	count, _ := workloadPodSetCount(workload, name)
	return count
}

func workloadAdmittedPodSetCount(workload *unstructured.Unstructured, name string) (int32, error) {
	if !isAdmitted(workload) {
		return 0, nil
	}
	assignments, found, err := unstructured.NestedSlice(workload.Object, "status", "admission", "podSetAssignments")
	if err != nil {
		return 0, err
	}
	if found {
		for _, item := range assignments {
			assignment, ok := item.(map[string]interface{})
			if !ok || assignment["name"] != name {
				continue
			}
			count, countFound, countErr := unstructured.NestedNumberAsFloat64(assignment, "count")
			if countErr != nil {
				return 0, countErr
			}
			if countFound {
				return int32(count), nil
			}
		}
	}
	return workloadPodSetCount(workload, name)
}

func updateWorkloadPodSetCount(
	ctx context.Context,
	k8sClient client.Client,
	workload *unstructured.Unstructured,
	name string,
	count int32,
) error {
	if count < 0 {
		return fmt.Errorf("Workload %s/%s cannot have a negative podSet count", workload.GetNamespace(), workload.GetName())
	}
	updated := workload.DeepCopy()
	podSets, found, err := unstructured.NestedSlice(updated.Object, "spec", "podSets")
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("Workload %s/%s has no spec.podSets", updated.GetNamespace(), updated.GetName())
	}
	for _, item := range podSets {
		podSet, ok := item.(map[string]interface{})
		if !ok || podSet["name"] != name {
			continue
		}
		podSet["count"] = int64(count)
		if err := unstructured.SetNestedField(updated.Object, podSets, "spec", "podSets"); err != nil {
			return err
		}
		if err := k8sClient.Update(ctx, updated); err != nil {
			return fmt.Errorf("updating Workload %s/%s podSet %q to %d: %w", updated.GetNamespace(), updated.GetName(), name, count, err)
		}
		return nil
	}
	return fmt.Errorf("Workload %s/%s is missing podSet %q", updated.GetNamespace(), updated.GetName(), name)
}

func deleteWorkload(
	ctx context.Context,
	k8sClient client.Client,
	workload *unstructured.Unstructured,
) error {
	updated := workload.DeepCopy()
	updated.SetFinalizers(removeString(updated.GetFinalizers(), ResourceInUseFinalizer))
	if err := k8sClient.Update(ctx, updated); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("releasing Workload %s/%s: %w", updated.GetNamespace(), updated.GetName(), err)
	}
	if err := k8sClient.Delete(ctx, updated); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("deleting Workload %s/%s: %w", updated.GetNamespace(), updated.GetName(), err)
	}
	return nil
}

func newReplacementSlice(
	active *unstructured.Unstructured,
	scaling *executorscalingv1alpha1.ExecutorScaling,
	target int32,
) *unstructured.Unstructured {
	slice := active.DeepCopy()
	name := replacementName(scaling, scaling.Generation)
	slice.SetName(name)
	slice.SetUID("")
	slice.SetResourceVersion("")
	slice.SetGeneration(0)
	slice.SetCreationTimestamp(metav1.Time{})
	slice.SetManagedFields(nil)
	slice.SetDeletionTimestamp(nil)
	slice.SetDeletionGracePeriodSeconds(nil)
	slice.SetFinalizers([]string{ResourceInUseFinalizer})
	slice.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion:         executorscalingv1alpha1.GroupVersion.String(),
		Kind:               "ExecutorScaling",
		Name:               scaling.Name,
		UID:                scaling.UID,
		Controller:         ptr.To(true),
		BlockOwnerDeletion: ptr.To(true),
	}})

	labelsCopy := make(map[string]string, len(slice.GetLabels())+1)
	for key, value := range slice.GetLabels() {
		labelsCopy[key] = value
	}
	labelsCopy[ExecutorScalingUIDLabel] = string(scaling.UID)
	slice.SetLabels(labelsCopy)

	annotations := make(map[string]string, len(slice.GetAnnotations())+2)
	for key, value := range slice.GetAnnotations() {
		annotations[key] = value
	}
	annotations[ElasticJobAnnotation] = "true"
	annotations[WorkloadSliceReplacementFor] = fmt.Sprintf("%s/%s", active.GetNamespace(), active.GetName())
	slice.SetAnnotations(annotations)

	unstructured.RemoveNestedField(slice.Object, "status")
	_ = updatePodSetCountInObject(slice, executorPodSetName, target)
	return slice
}

func updatePodSetCountInObject(workload *unstructured.Unstructured, name string, count int32) error {
	podSets, found, err := unstructured.NestedSlice(workload.Object, "spec", "podSets")
	if err != nil || !found {
		return err
	}
	for _, item := range podSets {
		podSet, ok := item.(map[string]interface{})
		if ok && podSet["name"] == name {
			podSet["count"] = int64(count)
		}
	}
	return unstructured.SetNestedField(workload.Object, podSets, "spec", "podSets")
}

func replacementName(scaling *executorscalingv1alpha1.ExecutorScaling, generation int64) string {
	prefix := strings.ToLower(fmt.Sprintf("%s-scaling", scaling.Name))
	if len(prefix) > 45 {
		prefix = prefix[:45]
	}
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%d", scaling.Namespace, scaling.UID, generation)))
	return fmt.Sprintf("%s-%s", strings.TrimSuffix(prefix, "-"), hex.EncodeToString(hash[:])[:8])
}

func removeString(values []string, value string) []string {
	result := values[:0]
	for _, current := range values {
		if current != value {
			result = append(result, current)
		}
	}
	return result
}
