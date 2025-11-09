package controller

import (
	"context"
	"encoding/json"

	apps "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// ControllerLabelKey is the label key for BatchRelease controller
	ControllerLabelKey = "batch-release.rollouts.yuyy.com/controlled-by-batch-release-controller"
	// ControlInfoAnnotation stores the BatchRelease control info
	ControlInfoAnnotation = "batch-release.rollouts.yuyy.com/control-info"
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

	controlInfo, ok := rs.Annotations[ControlInfoAnnotation]
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
