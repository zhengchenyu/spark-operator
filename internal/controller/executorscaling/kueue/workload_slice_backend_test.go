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

package kueue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	executorscalingv1alpha1 "github.com/kubeflow/spark-operator/v2/api/executorscalingv1alpha1"
	sparkv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

const (
	testNamespace  = "default"
	testAppName    = "spark-pi"
	testAppID      = "spark-application-123"
	testAppUID     = types.UID("spark-app-uid")
	testScalingUID = types.UID("executor-scaling-uid")
)

func TestReconcileCreatesReplacementAndReportsAdmission(t *testing.T) {
	ctx := context.Background()
	scaling := testScaling(20)
	app := testSparkApplication()
	driver := testDriverPod()
	initial := testWorkload("sparkapplication-spark-pi-initial", 10, 10, app, nil)
	k8sClient := testClient(t, scaling, app, driver, initial)
	backend := NewWorkloadSliceBackend(k8sClient)

	admitted, err := backend.Reconcile(ctx, scaling)
	require.NoError(t, err)
	require.Equal(t, int32(10), *admitted)

	workloads := listWorkloads(t, ctx, k8sClient)
	require.Len(t, workloads.Items, 2)
	replacement := findBridgeWorkload(t, workloads)
	require.Equal(t, int32(20), mustWorkloadPodSetCount(replacement, executorPodSetName))
	require.Equal(t, testNamespace+"/"+initial.GetName(), replacement.GetAnnotations()[WorkloadSliceReplacementFor])
	require.Equal(t, "true", replacement.GetAnnotations()[ElasticJobAnnotation])
	require.Equal(t, string(testScalingUID), replacement.GetLabels()[ExecutorScalingUIDLabel])
	require.Equal(t, []string{ResourceInUseFinalizer}, replacement.GetFinalizers())
	require.Equal(t, "ExecutorScaling", replacement.GetOwnerReferences()[0].Kind)
	require.Equal(t, testScalingUID, replacement.GetOwnerReferences()[0].UID)
	_, found, err := unstructured.NestedMap(replacement.Object, "status")
	require.NoError(t, err)
	require.False(t, found)

	setAdmission(replacement, 15)
	require.NoError(t, k8sClient.Update(ctx, replacement))

	admitted, err = backend.Reconcile(ctx, scaling)
	require.NoError(t, err)
	require.Equal(t, int32(15), *admitted)
}

func TestReconcileRetargetsPendingReplacement(t *testing.T) {
	ctx := context.Background()
	scaling := testScaling(30)
	app := testSparkApplication()
	initial := testWorkload("initial", 10, 10, app, nil)
	replacement := testWorkload("replacement", 20, -1, nil, scaling)
	replacement.SetAnnotations(map[string]string{
		ElasticJobAnnotation:        "true",
		WorkloadSliceReplacementFor: testNamespace + "/" + initial.GetName(),
	})
	k8sClient := testClient(t, scaling, app, testDriverPod(), initial, replacement)

	admitted, err := NewWorkloadSliceBackend(k8sClient).Reconcile(ctx, scaling)
	require.NoError(t, err)
	require.Equal(t, int32(10), *admitted)

	stored := WorkloadObject()
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(replacement), stored))
	require.Equal(t, int32(30), mustWorkloadPodSetCount(stored, executorPodSetName))
}

func TestReconcileScalesDownBridgeWorkload(t *testing.T) {
	ctx := context.Background()
	scaling := testScaling(8)
	app := testSparkApplication()
	active := testWorkload("active", 20, 15, nil, scaling)
	k8sClient := testClient(t, scaling, app, testDriverPod(), active)

	admitted, err := NewWorkloadSliceBackend(k8sClient).Reconcile(ctx, scaling)
	require.NoError(t, err)
	require.Equal(t, int32(8), *admitted)

	stored := WorkloadObject()
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(active), stored))
	require.Equal(t, int32(8), mustWorkloadPodSetCount(stored, executorPodSetName))
}

func TestReconcileCreatesTakeoverSliceBeforeScalingDownInitialWorkload(t *testing.T) {
	ctx := context.Background()
	scaling := testScaling(5)
	app := testSparkApplication()
	initial := testWorkload("initial", 10, 10, app, nil)
	k8sClient := testClient(t, scaling, app, testDriverPod(), initial)

	admitted, err := NewWorkloadSliceBackend(k8sClient).Reconcile(ctx, scaling)
	require.NoError(t, err)
	require.Equal(t, int32(5), *admitted)

	workloads := listWorkloads(t, ctx, k8sClient)
	require.Len(t, workloads.Items, 2)
	replacement := findBridgeWorkload(t, workloads)
	require.Equal(t, int32(10), mustWorkloadPodSetCount(replacement, executorPodSetName))
	storedInitial := WorkloadObject()
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(initial), storedInitial))
	require.Equal(t, int32(10), mustWorkloadPodSetCount(storedInitial, executorPodSetName))
}

func TestReconcileRemovesPendingReplacementBeforeScaleDown(t *testing.T) {
	ctx := context.Background()
	scaling := testScaling(5)
	app := testSparkApplication()
	active := testWorkload("active", 10, 10, nil, scaling)
	pending := testWorkload("pending", 20, -1, nil, scaling)
	pending.SetAnnotations(map[string]string{
		ElasticJobAnnotation:        "true",
		WorkloadSliceReplacementFor: testNamespace + "/" + active.GetName(),
	})
	k8sClient := testClient(t, scaling, app, testDriverPod(), active, pending)

	admitted, err := NewWorkloadSliceBackend(k8sClient).Reconcile(ctx, scaling)
	require.NoError(t, err)
	require.Equal(t, int32(5), *admitted)

	stored := WorkloadObject()
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(active), stored))
	require.Equal(t, int32(5), mustWorkloadPodSetCount(stored, executorPodSetName))
	err = k8sClient.Get(ctx, client.ObjectKeyFromObject(pending), WorkloadObject())
	require.True(t, apierrors.IsNotFound(err))
}

func TestFinalizeReleasesAndDeletesBridgeSlices(t *testing.T) {
	ctx := context.Background()
	scaling := testScaling(10)
	workload := testWorkload("active", 10, 10, nil, scaling)
	k8sClient := testClient(t, scaling, testSparkApplication(), testDriverPod(), workload)

	require.NoError(t, NewWorkloadSliceBackend(k8sClient).Finalize(ctx, scaling))

	err := k8sClient.Get(ctx, client.ObjectKeyFromObject(workload), WorkloadObject())
	require.True(t, apierrors.IsNotFound(err))
}

func TestMapWorkloadToRequestsUsesScalingUID(t *testing.T) {
	ctx := context.Background()
	scaling := testScaling(10)
	workload := testWorkload("active", 10, 10, nil, scaling)
	backend := NewWorkloadSliceBackend(testClient(t, scaling, workload))

	requests := backend.MapWorkloadToRequests(ctx, workload)
	require.Equal(t, []reconcile.Request{{
		NamespacedName: types.NamespacedName{
			Namespace: testNamespace,
			Name:      scaling.Name,
		},
	}}, requests)
}

func TestMapInitialWorkloadToRequestsUsesSparkApplicationID(t *testing.T) {
	ctx := context.Background()
	scaling := testScaling(10)
	app := testSparkApplication()
	workload := testWorkload("initial", 10, 10, app, nil)
	backend := NewWorkloadSliceBackend(testClient(t, scaling, app, workload))

	requests := backend.MapWorkloadToRequests(ctx, workload)
	require.Equal(t, []reconcile.Request{{
		NamespacedName: types.NamespacedName{
			Namespace: testNamespace,
			Name:      scaling.Name,
		},
	}}, requests)
}

func testClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, sparkv1beta2.AddToScheme(scheme))
	require.NoError(t, executorscalingv1alpha1.AddToScheme(scheme))
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		Build()
}

func testScaling(target int32) *executorscalingv1alpha1.ExecutorScaling {
	return &executorscalingv1alpha1.ExecutorScaling{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "spark-pi-scaling",
			Namespace:  testNamespace,
			UID:        testScalingUID,
			Generation: 2,
			Labels: map[string]string{
				"spark-app-id": testAppID,
			},
		},
		Spec: executorscalingv1alpha1.ExecutorScalingSpec{
			Target:        target,
			AdmissionMode: "injection",
		},
	}
}

func testSparkApplication() *sparkv1beta2.SparkApplication {
	return &sparkv1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testAppName,
			Namespace: testNamespace,
			UID:       testAppUID,
		},
		Status: sparkv1beta2.SparkApplicationStatus{
			SparkApplicationID: testAppID,
		},
	}
}

func testDriverPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-pi-driver",
			Namespace: testNamespace,
			Labels: map[string]string{
				common.LabelSparkApplicationSelector: testAppID,
				common.LabelSparkRole:                common.SparkRoleDriver,
				common.LabelSparkAppName:             testAppName,
			},
		},
	}
}

func testWorkload(
	name string,
	specCount, admittedCount int32,
	app *sparkv1beta2.SparkApplication,
	scaling *executorscalingv1alpha1.ExecutorScaling,
) *unstructured.Unstructured {
	workload := WorkloadObject()
	workload.SetName(name)
	workload.SetNamespace(testNamespace)
	workload.SetUID(types.UID(name + "-uid"))
	workload.SetFinalizers([]string{ResourceInUseFinalizer})
	workload.Object["spec"] = map[string]interface{}{
		"queueName": "default",
		"podSets": []interface{}{
			map[string]interface{}{"name": "driver", "count": int64(1)},
			map[string]interface{}{"name": executorPodSetName, "count": int64(specCount)},
		},
	}
	if app != nil {
		workload.SetOwnerReferences([]metav1.OwnerReference{{
			APIVersion: sparkv1beta2.GroupVersion.String(),
			Kind:       "SparkApplication",
			Name:       app.Name,
			UID:        app.UID,
		}})
		workload.SetLabels(map[string]string{JobUIDLabel: string(app.UID)})
	}
	if scaling != nil {
		workload.SetOwnerReferences([]metav1.OwnerReference{{
			APIVersion: executorscalingv1alpha1.GroupVersion.String(),
			Kind:       "ExecutorScaling",
			Name:       scaling.Name,
			UID:        scaling.UID,
		}})
		workload.SetLabels(map[string]string{ExecutorScalingUIDLabel: string(scaling.UID)})
	}
	if admittedCount >= 0 {
		setAdmission(workload, admittedCount)
	}
	return workload
}

func setAdmission(workload *unstructured.Unstructured, admitted int32) {
	workload.Object["status"] = map[string]interface{}{
		"conditions": []interface{}{
			map[string]interface{}{"type": "Admitted", "status": "True"},
			map[string]interface{}{"type": "QuotaReserved", "status": "True"},
		},
		"admission": map[string]interface{}{
			"clusterQueue": "cluster-queue",
			"podSetAssignments": []interface{}{
				map[string]interface{}{"name": "driver", "count": int64(1)},
				map[string]interface{}{"name": executorPodSetName, "count": int64(admitted)},
			},
		},
	}
}

func listWorkloads(t *testing.T, ctx context.Context, k8sClient client.Client) *unstructured.UnstructuredList {
	t.Helper()
	workloads := &unstructured.UnstructuredList{}
	workloads.SetGroupVersionKind(workloadListGVK)
	require.NoError(t, k8sClient.List(ctx, workloads, client.InNamespace(testNamespace)))
	return workloads
}

func findBridgeWorkload(t *testing.T, workloads *unstructured.UnstructuredList) *unstructured.Unstructured {
	t.Helper()
	for i := range workloads.Items {
		if workloads.Items[i].GetLabels()[ExecutorScalingUIDLabel] == string(testScalingUID) {
			return &workloads.Items[i]
		}
	}
	t.Fatal("bridge Workload not found")
	return nil
}
