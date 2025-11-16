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

package controller

import (
	"context"
	"reflect"
	"time"

	v1alpha1 "github.com/EchoGroot/batch-release/api/v1alpha1"
	"github.com/EchoGroot/batch-release/internal/controller/partition"
	deploymentutil "github.com/EchoGroot/batch-release/pkg/util/deployment"
	apps "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const DefaultRetryDuration = 2 * time.Second

// BatchReleaseReconciler reconciles a BatchRelease object
type BatchReleaseReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	eventRecorder record.EventRecorder
}

// +kubebuilder:rbac:groups=rollouts.yuyy.com,resources=batchreleases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rollouts.yuyy.com,resources=batchreleases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=rollouts.yuyy.com,resources=batchreleases/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the BatchRelease object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
func (r *BatchReleaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.V(2).Info("Start reconciling BatchRelease")
	var br = &v1alpha1.BatchRelease{}
	if err := r.Get(ctx, req.NamespacedName, br); err != nil {
		if errors.IsNotFound(err) {
			log.V(2).Info("BatchRelease not found, skip")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	var de = &apps.Deployment{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: br.Spec.WorkloadRef.Name}, de); err != nil {
		if errors.IsNotFound(err) {
			log.V(1).Info("Deployment not found",
				"deployment", br.Spec.WorkloadRef.Name,
				"namespace", req.Namespace)
		}
		return ctrl.Result{}, err
	}

	var errList field.ErrorList
	executor := partition.NewExecutor(br.DeepCopy(), de.DeepCopy(), r.Client, r.eventRecorder, log)

	result, err := executor.SyncDeployment(ctx)
	if err != nil {
		errList = append(errList, field.InternalError(field.NewPath("executorSyncDeployment"), err))
	}

	if !reflect.DeepEqual(br.Status, executor.Br.Status) {
		log.V(1).Info("BatchRelease status changed",
			"name", br.Name,
			"oldPhase", br.Status.Phase,
			"newPhase", executor.Br.Status.Phase,
			"oldStep", br.Status.CurrentStepIndex,
			"newStep", executor.Br.Status.CurrentStepIndex)
		if err := partition.UpdateObjStatus(ctx, r.Client, br.DeepCopy(), func(object client.Object) {
			newBr := object.(*v1alpha1.BatchRelease)
			newBr.Status = executor.Br.Status
			newBr.Status.ObservedGeneration = br.Generation
			newBr.Status.LastUpdateTime = &metav1.Time{Time: time.Now()}
		}); err != nil {
			errList = append(errList, field.InternalError(field.NewPath("updateStatus"), err))
		}
	}
	if len(errList) > 0 {
		log.Error(errList.ToAggregate(), "BatchRelease reconcile error",
			"name", br.Name)
		return result, errList.ToAggregate()
	}

	if result != (ctrl.Result{}) {
		return result, nil
	}

	err = deploymentutil.DeploymentRolloutSatisfied(de, executor.Br.Spec.Strategy.Steps[executor.Br.Status.CurrentStepIndex].Replicas)
	if err != nil {
		log.V(4).Info("Deployment is still rolling",
			"deployment", de.Name,
			"namespace", de.Namespace,
			"reason", err.Error())
		return reconcile.Result{RequeueAfter: DefaultRetryDuration}, nil
	}
	return reconcile.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *BatchReleaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.eventRecorder = mgr.GetEventRecorderFor("batch-release-controller")

	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.BatchRelease{}).
		Named("batchrelease").
		Watches(&apps.ReplicaSet{}, &enqueueRequestForReplicaset{}).
		Watches(&apps.Deployment{}, &enqueueRequestForDeployment{}).
		Complete(r)
}
