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

package executorscaling

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	executorscalingv1alpha1 "github.com/kubeflow/spark-operator/v2/api/executorscalingv1alpha1"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

const (
	// ExecutorScalingFinalizer protects an injection-mode request while the
	// admission backend releases any resources associated with it.
	ExecutorScalingFinalizer = "spark.apache.org/executor-scaling"

	// AdmissionModeDefault lets Spark perform loopback admission itself.
	AdmissionModeDefault = "default"

	// AdmissionModeInjection delegates admission to an external backend.
	AdmissionModeInjection = "injection"
)

// Options defines the namespaces watched by the ExecutorScaling controller.
type Options struct {
	Namespaces        []string
	NamespaceSelector string
}

// AdmissionBackend translates an ExecutorScaling request into an admission
// decision made by an external scheduling system.
//
// The backend owns the scheduling-system-specific part of the protocol. In
// particular, a future Kueue backend can create or update Workload slices and
// return the currently admitted executor count without exposing Kueue types to
// this controller.
type AdmissionBackend interface {
	Reconcile(context.Context, *executorscalingv1alpha1.ExecutorScaling) (*int32, error)
	Finalize(context.Context, *executorscalingv1alpha1.ExecutorScaling) error
}

// WorkloadEventSource lets an admission backend trigger reconciliation when
// its external scheduling objects change.
type WorkloadEventSource interface {
	WorkloadObject() client.Object
	MapWorkloadToRequests(context.Context, client.Object) []reconcile.Request
}

// NoopBackend is the default backend until an admission integration is
// configured. It deliberately does not admit injection-mode requests.
type NoopBackend struct{}

func (NoopBackend) Reconcile(context.Context, *executorscalingv1alpha1.ExecutorScaling) (*int32, error) {
	return nil, nil
}

func (NoopBackend) Finalize(context.Context, *executorscalingv1alpha1.ExecutorScaling) error {
	return nil
}

// Reconciler reconciles ExecutorScaling resources.
type Reconciler struct {
	scheme  *runtime.Scheme
	client  client.Client
	backend AdmissionBackend
	options Options
}

var _ reconcile.Reconciler = &Reconciler{}

// NewReconciler creates an ExecutorScaling reconciler.
func NewReconciler(
	scheme *runtime.Scheme,
	client client.Client,
	backend AdmissionBackend,
	options Options,
) *Reconciler {
	if backend == nil {
		backend = NoopBackend{}
	}
	return &Reconciler{
		scheme:  scheme,
		client:  client,
		backend: backend,
		options: options,
	}
}

// SetupWithManager sets up the ExecutorScaling controller.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, options controller.Options) error {
	namespacePredicate, err := util.NewNamespacePredicate(
		r.client,
		r.options.Namespaces,
		r.options.NamespaceSelector,
	)
	if err != nil {
		return fmt.Errorf("failed to create ExecutorScaling namespace predicate: %w", err)
	}

	builder := ctrl.NewControllerManagedBy(mgr).
		Named("executorscaling").
		For(&executorscalingv1alpha1.ExecutorScaling{}).
		WithEventFilter(namespacePredicate).
		WithOptions(options)

	if source, ok := r.backend.(WorkloadEventSource); ok {
		builder = builder.Watches(
			source.WorkloadObject(),
			handler.EnqueueRequestsFromMapFunc(source.MapWorkloadToRequests),
		)
	}

	return builder.Complete(r)
}

// +kubebuilder:rbac:groups=spark.apache.org,resources=executorscalings,verbs=get;list;watch;update
// +kubebuilder:rbac:groups=spark.apache.org,resources=executorscalings/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=spark.apache.org,resources=executorscalings/finalizers,verbs=update
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads/finalizers,verbs=update

// Reconcile applies the admission backend only to injection-mode requests.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	logger := log.FromContext(ctx)
	scaling := &executorscalingv1alpha1.ExecutorScaling{}
	if err := r.client.Get(ctx, types.NamespacedName{
		Namespace: req.Namespace,
		Name:      req.Name,
	}, scaling); err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	if !scaling.DeletionTimestamp.IsZero() {
		return r.reconcileDeletion(ctx, scaling)
	}

	switch scaling.Spec.AdmissionMode {
	case AdmissionModeDefault:
		return reconcile.Result{}, nil
	case AdmissionModeInjection:
		// Continue below.
	default:
		return reconcile.Result{}, fmt.Errorf(
			"unsupported ExecutorScaling admissionMode %q", scaling.Spec.AdmissionMode,
		)
	}

	if !controllerutil.ContainsFinalizer(scaling, ExecutorScalingFinalizer) {
		updated := scaling.DeepCopy()
		controllerutil.AddFinalizer(updated, ExecutorScalingFinalizer)
		if err := r.client.Update(ctx, updated); err != nil {
			return reconcile.Result{}, err
		}
		scaling = updated
	}

	admitted, err := r.backend.Reconcile(ctx, scaling.DeepCopy())
	if err != nil {
		return reconcile.Result{}, err
	}
	if admitted == nil {
		return reconcile.Result{}, nil
	}
	if *admitted < 0 || *admitted > scaling.Spec.Target {
		return reconcile.Result{}, fmt.Errorf(
			"admission backend returned admitted=%d for target=%d",
			*admitted, scaling.Spec.Target,
		)
	}

	if equality.Semantic.DeepEqual(scaling.Status.Admitted, admitted) {
		return reconcile.Result{}, nil
	}

	updated := scaling.DeepCopy()
	updated.Status.Admitted = ptr.To(*admitted)
	if err := r.client.Status().Patch(ctx, updated, client.MergeFrom(scaling)); err != nil {
		logger.Error(err, "Failed to update ExecutorScaling status", "admitted", *admitted)
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func (r *Reconciler) reconcileDeletion(
	ctx context.Context,
	scaling *executorscalingv1alpha1.ExecutorScaling,
) (reconcile.Result, error) {
	if !controllerutil.ContainsFinalizer(scaling, ExecutorScalingFinalizer) {
		return reconcile.Result{}, nil
	}

	if err := r.backend.Finalize(ctx, scaling.DeepCopy()); err != nil {
		return reconcile.Result{}, err
	}

	updated := scaling.DeepCopy()
	controllerutil.RemoveFinalizer(updated, ExecutorScalingFinalizer)
	if err := r.client.Update(ctx, updated); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}
