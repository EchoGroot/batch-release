package controller

import (
	"context"
	"encoding/json"

	"github.com/EchoGroot/batch-release/api/v1alpha1"

	apps "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type enqueueRequestForReplicaset struct {
}

func (e *enqueueRequestForReplicaset) Create(ctx context.Context, evt event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	e.handleReplicaSet(ctx, evt.Object, q)
}

func (e *enqueueRequestForReplicaset) Delete(ctx context.Context, evt event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	e.handleReplicaSet(ctx, evt.Object, q)
}

func (e *enqueueRequestForReplicaset) Generic(ctx context.Context, evt event.GenericEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	e.handleReplicaSet(ctx, evt.Object, q)
}

func (e *enqueueRequestForReplicaset) Update(ctx context.Context, evt event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	e.handleReplicaSet(ctx, evt.ObjectNew, q)
}

// handleReplicaSet finds the BatchRelease that owns this ReplicaSet and enqueues it
func (e *enqueueRequestForReplicaset) handleReplicaSet(ctx context.Context, obj client.Object, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	rs, ok := obj.(*apps.ReplicaSet)
	if !ok {
		return
	}

	// Check if this ReplicaSet has control-info annotation
	if rs.Annotations == nil {
		return
	}

	controlInfo, ok := rs.Annotations[v1alpha1.BatchReleaseControlInfoAnno]
	if !ok {
		return
	}

	// Parse control info to get BatchRelease name
	// Expected format: {"name":"my-batchrelease","namespace":"default"}
	var info struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	}
	if err := json.Unmarshal([]byte(controlInfo), &info); err != nil {
		return
	}

	// Enqueue the BatchRelease
	q.Add(reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: info.Namespace,
			Name:      info.Name,
		},
	})
}

// enqueueRequestForDeployment handles Deployment events and enqueues BatchRelease reconcile requests
type enqueueRequestForDeployment struct {
}

func (e *enqueueRequestForDeployment) Create(ctx context.Context, evt event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	// Ignore Create events for Deployment
	// We only care about replicas changes during updates
}

func (e *enqueueRequestForDeployment) Delete(ctx context.Context, evt event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	// Ignore Delete events for Deployment
}

func (e *enqueueRequestForDeployment) Generic(ctx context.Context, evt event.GenericEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	// Ignore Generic events for Deployment
}

func (e *enqueueRequestForDeployment) Update(ctx context.Context, evt event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	oldDeploy, ok1 := evt.ObjectOld.(*apps.Deployment)
	newDeploy, ok2 := evt.ObjectNew.(*apps.Deployment)
	if !ok1 || !ok2 {
		return
	}

	// Filter: Only care about Spec.Replicas changes
	// Ignore Status-only updates to reduce unnecessary reconciliations
	if oldDeploy.Spec.Replicas != nil && newDeploy.Spec.Replicas != nil &&
		*oldDeploy.Spec.Replicas == *newDeploy.Spec.Replicas {
		return
	}

	e.handleDeployment(ctx, evt.ObjectNew, q)
}

// handleDeployment finds the BatchRelease that owns this Deployment and enqueues it
func (e *enqueueRequestForDeployment) handleDeployment(ctx context.Context, obj client.Object, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	deploy, ok := obj.(*apps.Deployment)
	if !ok {
		return
	}

	// Check if this Deployment has control-info annotation
	if deploy.Annotations == nil {
		return
	}

	controlInfo, ok := deploy.Annotations[v1alpha1.BatchReleaseControlInfoAnno]
	if !ok {
		// No BatchRelease controls this Deployment, ignore it
		return
	}

	// Parse control info (OwnerReference format)
	var info struct {
		Name string `json:"name"`
		// Namespace is not in OwnerReference, use the Deployment's namespace
	}
	if err := json.Unmarshal([]byte(controlInfo), &info); err != nil {
		return
	}

	// Enqueue the BatchRelease
	q.Add(reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: deploy.Namespace,
			Name:      info.Name,
		},
	})
}
