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

package executorscaling_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	executorscalingv1alpha1 "github.com/kubeflow/spark-operator/v2/api/executorscalingv1alpha1"
	"github.com/kubeflow/spark-operator/v2/internal/controller/executorscaling"
)

type fakeBackend struct {
	admitted  *int32
	reconcile int
	finalize  int
}

func (b *fakeBackend) Reconcile(context.Context, *executorscalingv1alpha1.ExecutorScaling) (*int32, error) {
	b.reconcile++
	return b.admitted, nil
}

func (b *fakeBackend) Finalize(context.Context, *executorscalingv1alpha1.ExecutorScaling) error {
	b.finalize++
	return nil
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, executorscalingv1alpha1.AddToScheme(scheme))
	return scheme
}

func testRequest() reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{
		Namespace: "default",
		Name:      "spark-es-test",
	}}
}

func TestInjectionModeUpdatesAdmission(t *testing.T) {
	ctx := context.Background()
	admitted := int32(3)
	backend := &fakeBackend{admitted: &admitted}
	scaling := &executorscalingv1alpha1.ExecutorScaling{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-es-test",
			Namespace: "default",
		},
		Spec: executorscalingv1alpha1.ExecutorScalingSpec{
			Target:        10,
			AdmissionMode: executorscaling.AdmissionModeInjection,
		},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&executorscalingv1alpha1.ExecutorScaling{}).
		WithObjects(scaling).
		Build()
	r := executorscaling.NewReconciler(k8sClient.Scheme(), k8sClient, backend, executorscaling.Options{})

	_, err := r.Reconcile(ctx, testRequest())
	require.NoError(t, err)

	stored := &executorscalingv1alpha1.ExecutorScaling{}
	require.NoError(t, k8sClient.Get(ctx, testRequest().NamespacedName, stored))
	require.Equal(t, int32(3), *stored.Status.Admitted)
	require.Contains(t, stored.Finalizers, executorscaling.ExecutorScalingFinalizer)
	require.Equal(t, 1, backend.reconcile)
}

func TestInjectionModeAcceptsZeroAdmission(t *testing.T) {
	ctx := context.Background()
	admitted := int32(0)
	backend := &fakeBackend{admitted: &admitted}
	scaling := &executorscalingv1alpha1.ExecutorScaling{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-es-test",
			Namespace: "default",
		},
		Spec: executorscalingv1alpha1.ExecutorScalingSpec{
			Target:        10,
			AdmissionMode: executorscaling.AdmissionModeInjection,
		},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&executorscalingv1alpha1.ExecutorScaling{}).
		WithObjects(scaling).
		Build()
	r := executorscaling.NewReconciler(k8sClient.Scheme(), k8sClient, backend, executorscaling.Options{})

	_, err := r.Reconcile(ctx, testRequest())
	require.NoError(t, err)

	stored := &executorscalingv1alpha1.ExecutorScaling{}
	require.NoError(t, k8sClient.Get(ctx, testRequest().NamespacedName, stored))
	require.NotNil(t, stored.Status.Admitted)
	require.Equal(t, int32(0), *stored.Status.Admitted)
}

func TestDefaultModeIsHandledBySpark(t *testing.T) {
	ctx := context.Background()
	backend := &fakeBackend{}
	scaling := &executorscalingv1alpha1.ExecutorScaling{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-es-test",
			Namespace: "default",
		},
		Spec: executorscalingv1alpha1.ExecutorScalingSpec{
			Target:        10,
			AdmissionMode: executorscaling.AdmissionModeDefault,
		},
		Status: executorscalingv1alpha1.ExecutorScalingStatus{
			Admitted: ptr.To(int32(4)),
		},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&executorscalingv1alpha1.ExecutorScaling{}).
		WithObjects(scaling).
		Build()
	r := executorscaling.NewReconciler(k8sClient.Scheme(), k8sClient, backend, executorscaling.Options{})

	_, err := r.Reconcile(ctx, testRequest())
	require.NoError(t, err)
	require.Equal(t, 0, backend.reconcile)

	stored := &executorscalingv1alpha1.ExecutorScaling{}
	require.NoError(t, k8sClient.Get(ctx, testRequest().NamespacedName, stored))
	require.Equal(t, int32(4), *stored.Status.Admitted)
}

func TestInvalidAdmissionIsRejected(t *testing.T) {
	ctx := context.Background()
	admitted := int32(11)
	backend := &fakeBackend{admitted: &admitted}
	scaling := &executorscalingv1alpha1.ExecutorScaling{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-es-test",
			Namespace: "default",
		},
		Spec: executorscalingv1alpha1.ExecutorScalingSpec{
			Target:        10,
			AdmissionMode: executorscaling.AdmissionModeInjection,
		},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&executorscalingv1alpha1.ExecutorScaling{}).
		WithObjects(scaling).
		Build()
	r := executorscaling.NewReconciler(k8sClient.Scheme(), k8sClient, backend, executorscaling.Options{})

	_, err := r.Reconcile(ctx, testRequest())
	require.Error(t, err)
	require.Contains(t, err.Error(), "admitted=11")
}

func TestUnsupportedAdmissionModeIsRejected(t *testing.T) {
	ctx := context.Background()
	scaling := &executorscalingv1alpha1.ExecutorScaling{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-es-test",
			Namespace: "default",
		},
		Spec: executorscalingv1alpha1.ExecutorScalingSpec{
			Target:        10,
			AdmissionMode: "unsupported",
		},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&executorscalingv1alpha1.ExecutorScaling{}).
		WithObjects(scaling).
		Build()
	r := executorscaling.NewReconciler(
		k8sClient.Scheme(), k8sClient, &fakeBackend{}, executorscaling.Options{})

	_, err := r.Reconcile(ctx, testRequest())
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported ExecutorScaling admissionMode")
}

func TestDeletionFinalizesBackendAndRemovesFinalizer(t *testing.T) {
	ctx := context.Background()
	backend := &fakeBackend{}
	scaling := &executorscalingv1alpha1.ExecutorScaling{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "spark-es-test",
			Namespace:         "default",
			Finalizers:        []string{executorscaling.ExecutorScalingFinalizer},
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
		},
		Spec: executorscalingv1alpha1.ExecutorScalingSpec{
			Target:        10,
			AdmissionMode: executorscaling.AdmissionModeInjection,
		},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&executorscalingv1alpha1.ExecutorScaling{}).
		WithObjects(scaling).
		Build()
	r := executorscaling.NewReconciler(k8sClient.Scheme(), k8sClient, backend, executorscaling.Options{})

	_, err := r.Reconcile(ctx, testRequest())
	require.NoError(t, err)
	require.Equal(t, 1, backend.finalize)

	stored := &executorscalingv1alpha1.ExecutorScaling{}
	err = k8sClient.Get(ctx, testRequest().NamespacedName, stored)
	if err == nil {
		require.NotContains(t, stored.Finalizers, executorscaling.ExecutorScalingFinalizer)
	} else {
		require.True(t, apierrors.IsNotFound(err))
	}
}
