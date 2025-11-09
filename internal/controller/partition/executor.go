package partition

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/EchoGroot/batch-release/api/v1alpha1"
	deploymentutil "github.com/EchoGroot/batch-release/pkg/util/deployment"
	jsonutil "github.com/EchoGroot/batch-release/pkg/util/json"
	labelsutil "github.com/EchoGroot/batch-release/pkg/util/labels"
	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	intstrutil "k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	"k8s.io/utils/integer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	maxRevHistoryLengthInChars = 2000
	DefaultDuration            = 2 * time.Second
)

var controllerKind = apps.SchemeGroupVersion.WithKind("Deployment")

type Executor struct {
	Br            *v1alpha1.BatchRelease
	de            *apps.Deployment
	client        client.Client
	eventRecorder record.EventRecorder
}

func NewExecutor(br *v1alpha1.BatchRelease, de *apps.Deployment, client client.Client, eventRecorder record.EventRecorder) *Executor {
	return &Executor{
		Br:            br,
		de:            de,
		client:        client,
		eventRecorder: eventRecorder,
	}
}

func (r *Executor) SyncDeployment(ctx context.Context) (ctrl.Result, error) {
	r.Br.Status.Reason, r.Br.Status.Message = "", ""
	switch r.Br.Status.Phase {
	default:
		r.Br.Status.Phase = v1alpha1.PhaseInitial
		fallthrough
	case v1alpha1.PhaseInitial:
		return r.init(ctx)
	case v1alpha1.PhaseRollingUpdate:
		return r.stepByStep(ctx)
	case v1alpha1.PhaseFinalizing:
		return r.finalize(ctx)
	case v1alpha1.PhaseCompleted:
		return ctrl.Result{}, nil
	}
}

func (r *Executor) finalize(ctx context.Context) (ctrl.Result, error) {
	err := UpdateObj(ctx, r.client, r.de, func(object client.Object) {
		newDe := object.(*apps.Deployment)
		newDe.Spec.Paused = false
		newDe.Spec.Strategy.Type = apps.RollingUpdateDeploymentStrategyType
		newDe.Spec.Strategy.RollingUpdate = &apps.RollingUpdateDeployment{MaxSurge: r.Br.Status.MaxSurge, MaxUnavailable: r.Br.Status.MaxUnavailable}
		delete(newDe.Annotations, v1alpha1.BatchReleaseControlInfoAnno)
	})
	if err != nil {
		klog.Errorf("update deployment %s/%s failed, err: %v", r.de.Namespace, r.de.Name, err)
		return ctrl.Result{RequeueAfter: DefaultDuration}, err
	}

	r.Br.Status.Phase = v1alpha1.PhaseCompleted
	return ctrl.Result{}, nil
}

func (r *Executor) init(ctx context.Context) (ctrl.Result, error) {
	r.Br.Status.CurrentStepIndex = 0
	r.Br.Status.CurrentStepState = ""
	r.Br.Status.UpdatedReadyReplicas = 0
	if r.de.Spec.Strategy.RollingUpdate == nil {
		return ctrl.Result{}, fmt.Errorf("deployment %s/%s does not have RollingUpdate strategy", r.de.Namespace, r.de.Name)
	}
	r.Br.Status.MaxUnavailable = r.de.Spec.Strategy.RollingUpdate.MaxUnavailable
	r.Br.Status.MaxSurge = r.de.Spec.Strategy.RollingUpdate.MaxSurge

	if err := UpdateObj(ctx, r.client, r.de, func(object client.Object) {
		d := object.(*apps.Deployment)
		d.Spec.Template = r.Br.Spec.Template
		d.Spec.Paused = true
		d.Spec.Strategy.Type = apps.RecreateDeploymentStrategyType
		d.Spec.Strategy.RollingUpdate = nil
		if d.Annotations == nil {
			d.Annotations = make(map[string]string)
		}
		d.Annotations[v1alpha1.BatchReleaseControlInfoAnno] = jsonutil.DumpJSON(metav1.NewControllerRef(r.Br, r.Br.GetObjectKind().GroupVersionKind()))
	}); err != nil {
		return ctrl.Result{}, err
	}
	klog.V(1).Infof("Successfully updated Deployment %s", r.de.Name)

	r.Br.Status.Phase = v1alpha1.PhaseRollingUpdate
	return ctrl.Result{}, nil
}

func (r *Executor) stepByStep(ctx context.Context) (ctrl.Result, error) {
	switch r.Br.Status.CurrentStepState {
	default:
		r.Br.Status.CurrentStepState = v1alpha1.StepStateInitial
		fallthrough
	case v1alpha1.StepStateInitial:
		klog.V(1).Infof("start release step %d", r.Br.Status.CurrentStepIndex)
		r.Br.Status.CurrentStepState = v1alpha1.StepStateUpgrade
	case v1alpha1.StepStateUpgrade:
		rsList, err := r.getReplicaSetsForDeployment(ctx, r.de)
		if err != nil {
			return ctrl.Result{}, err
		}
		newRS, oldRSs, err := r.getAllReplicaSetsAndSyncRevision(ctx, r.de, rsList, true)
		if err != nil {
			return ctrl.Result{}, err
		}
		allRSs := append(oldRSs, newRS)

		// Scale up, if we can.
		scaledUp, err := r.reconcileNewReplicaSet(ctx, allRSs, newRS)
		if err != nil {
			return ctrl.Result{}, err
		}
		if scaledUp {
			// Update DeploymentStatus
			return ctrl.Result{}, r.syncRolloutStatus(ctx, allRSs, newRS)
		}

		// Scale down, if we can.
		scaledDown, err := r.reconcileOldReplicaSets(ctx, allRSs, deploymentutil.FilterActiveReplicaSets(oldRSs), newRS, r.de)
		if err != nil {
			return ctrl.Result{}, err
		}
		if scaledDown {
			// Update DeploymentStatus
			return ctrl.Result{}, r.syncRolloutStatus(ctx, allRSs, newRS)
		}

		// Sync deployment status
		if err = r.syncRolloutStatus(ctx, allRSs, newRS); err != nil {
			return ctrl.Result{}, err
		}

		if err := r.IsBatchReady(); err != nil {
			klog.V(2).Infof("release step %d not ready, requeue, err: %v", r.Br.Status.CurrentStepIndex, err)
			return ctrl.Result{RequeueAfter: DefaultDuration}, err
		}

		r.Br.Status.CurrentStepState = v1alpha1.StepStateBlocking
		return ctrl.Result{}, nil
	case v1alpha1.StepStateBlocking:
		if r.Br.Status.CurrentStepIndex == int32(len(r.Br.Spec.Strategy.Steps)-1) {
			r.Br.Status.CurrentStepState = v1alpha1.StepStateCompleted
			return ctrl.Result{}, nil
		}
		r.Br.Status.Reason, r.Br.Status.Message = v1alpha1.BatchReleaseReasonStepBlocking, v1alpha1.StepBlockingMessage
		return ctrl.Result{}, nil
	case v1alpha1.StepStateCompleted:
		klog.V(1).Infof("release step %d completed", r.Br.Status.CurrentStepIndex)
		if r.Br.Status.CurrentStepIndex == int32(len(r.Br.Spec.Strategy.Steps))-1 {
			r.Br.Status.Phase = v1alpha1.PhaseFinalizing
			return ctrl.Result{}, nil
		}

		r.Br.Status.CurrentStepIndex++
		r.Br.Status.CurrentStepState = v1alpha1.StepStateInitial
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, nil
}

func (bc *Executor) IsBatchReady() error {
	currentBatch := bc.Br.Status.CurrentStepIndex
	desiredPartition := bc.Br.Spec.Strategy.Steps[currentBatch].Replicas
	DesiredUpdatedReplicas := deploymentutil.NewRSReplicasLimit(desiredPartition, bc.de)
	if bc.de.Status.UpdatedReplicas < DesiredUpdatedReplicas {
		return fmt.Errorf("current batch not ready: updated replicas not satisfied, UpdatedReplicas %d < DesiredUpdatedReplicas %d", bc.de.Status.UpdatedReplicas, DesiredUpdatedReplicas)
	}

	unavailableToleration := allowedUnavailable(bc.Br.Status.MaxUnavailable, bc.de.Status.UpdatedReplicas)
	if unavailableToleration+bc.Br.Status.UpdatedReadyReplicas < DesiredUpdatedReplicas {
		return fmt.Errorf("current batch not ready: updated ready replicas not satisfied, allowedUnavailable + UpdatedReadyReplicas %d < DesiredUpdatedReplicas %d", unavailableToleration+bc.Br.Status.UpdatedReadyReplicas, DesiredUpdatedReplicas)
	}

	if DesiredUpdatedReplicas > 0 && bc.Br.Status.UpdatedReadyReplicas == 0 {
		return fmt.Errorf("current batch not ready: no updated ready replicas, DesiredUpdatedReplicas %d > 0 and UpdatedReadyReplicas %d = 0", DesiredUpdatedReplicas, bc.Br.Status.UpdatedReadyReplicas)
	}
	return nil
}

func allowedUnavailable(threshold *intstr.IntOrString, replicas int32) int32 {
	failureThreshold := 0
	if threshold != nil {
		failureThreshold, _ = intstr.GetScaledValueFromIntOrPercent(threshold, int(replicas), true)
	}
	return int32(failureThreshold)
}

func (dc *Executor) reconcileOldReplicaSets(ctx context.Context, allRSs []*apps.ReplicaSet, oldRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet, deployment *apps.Deployment) (bool, error) {
	oldPodsCount := deploymentutil.GetReplicaCountForReplicaSets(oldRSs)
	if oldPodsCount == 0 {
		// Can't scale down further
		return false, nil
	}

	allPodsCount := deploymentutil.GetReplicaCountForReplicaSets(allRSs)
	klog.V(4).Infof("New replica set %s/%s has %d available pods.", newRS.Namespace, newRS.Name, newRS.Status.AvailableReplicas)
	maxUnavailable := dc.maxUnavailable()

	// Old RSes should obey the limitation of partition.
	ScaleDownOldLimit := ScaleDownLimitForOld(oldRSs, newRS, deployment, dc.Br.Spec.Strategy.Steps[dc.Br.Status.CurrentStepIndex].Replicas)
	if ScaleDownOldLimit <= 0 {
		// Old replica sets do not satisfied as partition expectation, scale up.
		return dc.scaleUpOldReplicaSets(ctx, oldRSs, -ScaleDownOldLimit, deployment)
	}

	// Check if we can scale down. We can scale down in the following 2 cases:
	// * Some old replica sets have unhealthy replicas, we could safely scale down those unhealthy replicas since that won't further
	//  increase unavailability.
	// * New replica set has scaled up and it's replicas becomes ready, then we can scale down old replica sets in a further step.
	//
	// maxScaledDown := allPodsCount - minAvailable - newReplicaSetPodsUnavailable
	// take into account not only maxUnavailable and any surge pods that have been created, but also unavailable pods from
	// the newRS, so that the unavailable pods from the newRS would not make us scale down old replica sets in a further
	// step(that will increase unavailability).
	//
	// Concrete example:
	//
	// * 10 replicas
	// * 2 maxUnavailable (absolute number, not percent)
	// * 3 maxSurge (absolute number, not percent)
	//
	// case 1:
	// * Deployment is updated, newRS is created with 3 replicas, oldRS is scaled down to 8, and newRS is scaled up to 5.
	// * The new replica set pods crashloop and never become available.
	// * allPodsCount is 13. minAvailable is 8. newRSPodsUnavailable is 5.
	// * A node fails and causes one of the oldRS pods to become unavailable. However, 13 - 8 - 5 = 0, so the oldRS won't be scaled down.
	// * The user notices the crashloop and does kubectl rollout undo to rollback.
	// * newRSPodsUnavailable is 1, since we rolled back to the good replica set, so maxScaledDown = 13 - 8 - 1 = 4. 4 of the crashlooping pods will be scaled down.
	// * The total number of pods will then be 9 and the newRS can be scaled up to 10.
	//
	// case 2:
	// Same example, but pushing a new pod template instead of rolling back (aka "roll over"):
	// * The new replica set created must start with 0 replicas because allPodsCount is already at 13.
	// * However, newRSPodsUnavailable would also be 0, so the 2 old replica sets could be scaled down by 5 (13 - 8 - 0), which would then
	// allow the new replica set to be scaled up by 5.
	minAvailable := *(deployment.Spec.Replicas) - maxUnavailable
	newRSUnavailablePodCount := *(newRS.Spec.Replicas) - newRS.Status.AvailableReplicas
	maxScaledDown := allPodsCount - minAvailable - newRSUnavailablePodCount
	// But, do not exceed the number of the desired partition.
	maxScaledDown = integer.Int32Min(maxScaledDown, ScaleDownOldLimit)
	if maxScaledDown <= 0 {
		return false, nil
	}

	// Clean up unhealthy replicas first, otherwise unhealthy replicas will block deployment
	// and cause timeout. See https://github.com/kubernetes/kubernetes/issues/16737
	oldRSs, cleanupCount, err := dc.cleanupUnhealthyReplicas(ctx, oldRSs, deployment, maxScaledDown)
	if err != nil {
		return false, nil
	}
	klog.V(4).Infof("Cleaned up unhealthy replicas from old RSes by %d", cleanupCount)

	// Scale down old replica sets, need check maxUnavailable to ensure we can scale down
	allRSs = append(oldRSs, newRS)
	scaledDownCount, err := dc.scaleDownOldReplicaSetsForRollingUpdate(ctx, allRSs, oldRSs, deployment)
	if err != nil {
		return false, nil
	}
	klog.V(4).Infof("Scaled down old RSes of deployment %s by %d", deployment.Name, scaledDownCount)

	totalScaledDown := cleanupCount + scaledDownCount
	return totalScaledDown > 0, nil
}

func (dc *Executor) maxUnavailable() int32 {
	if *(dc.de.Spec.Replicas) == 0 {
		return int32(0)
	}
	// Error caught by validation
	_, maxUnavailable, _ := deploymentutil.ResolveFenceposts(dc.Br.Status.MaxSurge, dc.Br.Status.MaxUnavailable, *(dc.de.Spec.Replicas))
	if maxUnavailable > *dc.de.Spec.Replicas {
		return *dc.de.Spec.Replicas
	}
	return maxUnavailable
}

func (dc *Executor) scaleUpOldReplicaSets(ctx context.Context, oldRSs []*apps.ReplicaSet, scaledUpCount int32, deployment *apps.Deployment) (bool, error) {
	if scaledUpCount <= 0 || len(oldRSs) == 0 {
		return false, nil
	}
	// Scale up the biggest one or older.
	sort.Sort(deploymentutil.ReplicaSetsBySizeOlder(oldRSs))
	newScale := (*oldRSs[0].Spec.Replicas) + scaledUpCount
	scaled, _, err := dc.scaleReplicaSetAndRecordEvent(ctx, oldRSs[0], newScale, deployment)
	return scaled, err
}

func (dc *Executor) scaleDownOldReplicaSetsForRollingUpdate(ctx context.Context, allRSs []*apps.ReplicaSet, oldRSs []*apps.ReplicaSet, deployment *apps.Deployment) (int32, error) {
	maxUnavailable := dc.maxUnavailable()

	// Check if we can scale down.
	minAvailable := *(deployment.Spec.Replicas) - maxUnavailable
	// Find the number of available pods.
	availablePodCount := deploymentutil.GetAvailableReplicaCountForReplicaSets(allRSs)
	if availablePodCount <= minAvailable {
		// Cannot scale down.
		return 0, nil
	}
	klog.V(4).Infof("Found %d available pods in deployment %s, scaling down old RSes", availablePodCount, deployment.Name)

	// We expected scaled down the middle revision firstly.
	sort.Sort(deploymentutil.ReplicaSetsBySmallerRevision(oldRSs))

	totalScaledDown := int32(0)
	totalScaleDownCount := availablePodCount - minAvailable
	newRS := deploymentutil.FindNewReplicaSet(deployment, allRSs)
	// Old RSes should obey the limitation of partition.
	ScaleDownOldLimit := ScaleDownLimitForOld(oldRSs, newRS, deployment, dc.Br.Spec.Strategy.Steps[dc.Br.Status.CurrentStepIndex].Replicas)
	totalScaleDownCount = integer.Int32Min(totalScaleDownCount, ScaleDownOldLimit)
	for _, targetRS := range oldRSs {
		if totalScaledDown >= totalScaleDownCount {
			// No further scaling required.
			break
		}
		if *(targetRS.Spec.Replicas) == 0 {
			// cannot scale down this ReplicaSet.
			continue
		}
		// Scale down.
		scaleDownCount := int32(integer.IntMin(int(*(targetRS.Spec.Replicas)), int(totalScaleDownCount-totalScaledDown)))
		newReplicasCount := *(targetRS.Spec.Replicas) - scaleDownCount
		if newReplicasCount > *(targetRS.Spec.Replicas) {
			return 0, fmt.Errorf("when scaling down old RS, got invalid request to scale down %s/%s %d -> %d", targetRS.Namespace, targetRS.Name, *(targetRS.Spec.Replicas), newReplicasCount)
		}
		_, _, err := dc.scaleReplicaSetAndRecordEvent(ctx, targetRS, newReplicasCount, deployment)
		if err != nil {
			return totalScaledDown, err
		}

		totalScaledDown += scaleDownCount
	}

	return totalScaledDown, nil
}

func (dc *Executor) cleanupUnhealthyReplicas(ctx context.Context, oldRSs []*apps.ReplicaSet, deployment *apps.Deployment, maxCleanupCount int32) ([]*apps.ReplicaSet, int32, error) {
	sort.Sort(deploymentutil.ReplicaSetsByCreationTimestamp(oldRSs))
	// Safely scale down all old replica sets with unhealthy replicas. Replica set will sort the pods in the order
	// such that not-ready < ready, unscheduled < scheduled, and pending < running. This ensures that unhealthy replicas will
	// been deleted first and won't increase unavailability.
	totalScaledDown := int32(0)
	for i, targetRS := range oldRSs {
		if totalScaledDown >= maxCleanupCount {
			break
		}
		if *(targetRS.Spec.Replicas) == 0 {
			// cannot scale down this replica set.
			continue
		}
		klog.V(4).Infof("Found %d available pods in old RS %s/%s", targetRS.Status.AvailableReplicas, targetRS.Namespace, targetRS.Name)
		if *(targetRS.Spec.Replicas) == targetRS.Status.AvailableReplicas {
			// no unhealthy replicas found, no scaling required.
			continue
		}

		scaledDownCount := int32(integer.IntMin(int(maxCleanupCount-totalScaledDown), int(*(targetRS.Spec.Replicas)-targetRS.Status.AvailableReplicas)))
		newReplicasCount := *(targetRS.Spec.Replicas) - scaledDownCount
		if newReplicasCount > *(targetRS.Spec.Replicas) {
			return nil, 0, fmt.Errorf("when cleaning up unhealthy replicas, got invalid request to scale down %s/%s %d -> %d", targetRS.Namespace, targetRS.Name, *(targetRS.Spec.Replicas), newReplicasCount)
		}
		_, updatedOldRS, err := dc.scaleReplicaSetAndRecordEvent(ctx, targetRS, newReplicasCount, deployment)
		if err != nil {
			return nil, totalScaledDown, err
		}
		totalScaledDown += scaledDownCount
		oldRSs[i] = updatedOldRS
	}
	return oldRSs, totalScaledDown, nil
}

func ScaleDownLimitForOld(oldRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet, deployment *apps.Deployment, partition intstrutil.IntOrString) int32 {
	newRSUpdateLimit := deploymentutil.NewRSReplicasLimit(partition, deployment)
	// Expected replicas of the new replica set under the partition settings.
	newRSDesiredCount := integer.Int32Max(newRSUpdateLimit, *newRS.Spec.Replicas)
	// Expected total replicas for old replica sets.
	oldRSDesiredCount := *(deployment.Spec.Replicas) - newRSDesiredCount
	// Actual total replicas for old replica sets.
	oldPodsCount := deploymentutil.GetReplicaCountForReplicaSets(oldRSs)
	// oldRSDesiredDiff is the gap between the reality and the desired.
	scaleDownOldLimit := oldPodsCount - oldRSDesiredCount

	klog.V(4).InfoS("Calculate scale down limit for ",
		"Deployment", klog.KObj(deployment),
		// About the new replica set
		"Replicas(New)", *(newRS.Spec.Replicas), "Replicas(New)", newRSDesiredCount,
		// About the old replica sets
		"ReplicaS(Old)", oldPodsCount, "Replicas(Old)", oldRSDesiredCount, "ScaleDownLimit(Old)", scaleDownOldLimit,
		// About the deployment
		"Replicas(Deployment)", *(deployment.Spec.Replicas), "Partition(Deployment)", newRSUpdateLimit)

	return scaleDownOldLimit
}

func (dc *Executor) syncRolloutStatus(ctx context.Context, allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet) error {
	newStatus := dc.calculateStatus(allRSs, newRS)

	// If there is no progressDeadlineSeconds set, remove any Progressing condition.
	if !deploymentutil.HasProgressDeadline(dc.de) {
		deploymentutil.RemoveDeploymentCondition(&newStatus, apps.DeploymentProgressing)
	}

	// If there is only one replica set that is active then that means we are not running
	// a new rollout and this is a resync where we don't need to estimate any progress.
	// In such a case, we should simply not estimate any progress for this deployment.
	currentCond := deploymentutil.GetDeploymentCondition(dc.de.Status, apps.DeploymentProgressing)
	isCompleteDeployment := newStatus.Replicas == newStatus.UpdatedReplicas && currentCond != nil && currentCond.Reason == deploymentutil.NewRSAvailableReason
	// Check for progress only if there is a progress deadline set and the latest rollout
	// hasn't completed yet.
	if deploymentutil.HasProgressDeadline(dc.de) && !isCompleteDeployment {
		switch {
		case deploymentutil.DeploymentComplete(dc.de, &newStatus):
			// Update the deployment conditions with a message for the new replica set that
			// was successfully deployed. If the condition already exists, we ignore this update.
			msg := fmt.Sprintf("Deployment %q has successfully progressed.", dc.de.Name)
			if newRS != nil {
				msg = fmt.Sprintf("ReplicaSet %q has successfully progressed.", newRS.Name)
			}
			condition := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionTrue, deploymentutil.NewRSAvailableReason, msg)
			deploymentutil.SetDeploymentCondition(&newStatus, *condition)

		case deploymentutil.DeploymentProgressing(dc.de, &newStatus):
			// If there is any progress made, continue by not checking if the deployment failed. This
			// behavior emulates the rolling updater progressDeadline check.
			msg := fmt.Sprintf("Deployment %q is progressing.", dc.de.Name)
			if newRS != nil {
				msg = fmt.Sprintf("ReplicaSet %q is progressing.", newRS.Name)
			}
			condition := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionTrue, deploymentutil.ReplicaSetUpdatedReason, msg)
			// Update the current Progressing condition or add a new one if it doesn't exist.
			// If a Progressing condition with status=true already exists, we should update
			// everything but lastTransitionTime. SetDeploymentCondition already does that but
			// it also is not updating conditions when the reason of the new condition is the
			// same as the old. The Progressing condition is a special case because we want to
			// update with the same reason and change just lastUpdateTime iff we notice any
			// progress. That's why we handle it here.
			if currentCond != nil {
				if currentCond.Status == v1.ConditionTrue {
					condition.LastTransitionTime = currentCond.LastTransitionTime
				}
				deploymentutil.RemoveDeploymentCondition(&newStatus, apps.DeploymentProgressing)
			}
			deploymentutil.SetDeploymentCondition(&newStatus, *condition)

		case deploymentutil.DeploymentTimedOut(dc.de, &newStatus):
			// Update the deployment with a timeout condition. If the condition already exists,
			// we ignore this update.
			msg := fmt.Sprintf("Deployment %q has timed out progressing.", dc.de.Name)
			if newRS != nil {
				msg = fmt.Sprintf("ReplicaSet %q has timed out progressing.", newRS.Name)
			}
			condition := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionFalse, deploymentutil.TimedOutReason, msg)
			deploymentutil.SetDeploymentCondition(&newStatus, *condition)
		}
	}

	// Move failure conditions of all replica sets in deployment conditions. For now,
	// only one failure condition is returned from getReplicaFailures.
	if replicaFailureCond := dc.getReplicaFailures(allRSs, newRS); len(replicaFailureCond) > 0 {
		// There will be only one ReplicaFailure condition on the replica set.
		deploymentutil.SetDeploymentCondition(&newStatus, replicaFailureCond[0])
	} else {
		deploymentutil.RemoveDeploymentCondition(&newStatus, apps.DeploymentReplicaFailure)
	}

	// Calculate extra status annotation
	// extraStatusAnno, err := dc.updateDeploymentExtraStatus(ctx, newRS, d)
	// if err != nil {
	// 	return nil // no need to retry
	// }

	updatedReadyReplicas := int32(0)
	if newRS != nil {
		updatedReadyReplicas = newRS.Status.ReadyReplicas
	}
	dc.Br.Status.UpdatedReadyReplicas = updatedReadyReplicas

	// Update both status and annotation
	err := dc.patchDeploymentStatusAndAnnotation(ctx, dc.de, newStatus)
	if err != nil {
		return err
	}

	// Requeue the deployment if required.
	dc.requeueStuckDeployment(dc.de, newStatus)
	return nil
}

func (dc *Executor) requeueStuckDeployment(d *apps.Deployment, newStatus apps.DeploymentStatus) time.Duration {
	currentCond := deploymentutil.GetDeploymentCondition(d.Status, apps.DeploymentProgressing)
	// Can't estimate progress if there is no deadline in the spec or progressing condition in the current status.
	if !deploymentutil.HasProgressDeadline(d) || currentCond == nil {
		return time.Duration(-1)
	}
	// No need to estimate progress if the rollout is complete or already timed out.
	if deploymentutil.DeploymentComplete(d, &newStatus) || currentCond.Reason == deploymentutil.TimedOutReason {
		return time.Duration(-1)
	}
	// If there is no sign of progress at this point then there is a high chance that the
	// deployment is stuck. We should resync this deployment at some point in the future[1]
	// and check whether it has timed out. We definitely need this, otherwise we depend on the
	// controller resync interval. See https://github.com/kubernetes/kubernetes/issues/34458.
	//
	// [1] ProgressingCondition.LastUpdatedTime + progressDeadlineSeconds - time.Now()
	//
	// For example, if a Deployment updated its Progressing condition 3 minutes ago and has a
	// deadline of 10 minutes, it would need to be resynced for a progress check after 7 minutes.
	//
	// lastUpdated: 			00:00:00
	// now: 					00:03:00
	// progressDeadlineSeconds: 600 (10 minutes)
	//
	// lastUpdated + progressDeadlineSeconds - now => 00:00:00 + 00:10:00 - 00:03:00 => 07:00
	after := currentCond.LastUpdateTime.Time.Add(time.Duration(*d.Spec.ProgressDeadlineSeconds) * time.Second).Sub(nowFn())
	// If the remaining time is less than a second, then requeue the deployment immediately.
	// Make it ratelimited so we stay on the safe side, eventually the Deployment should
	// transition either to a Complete or to a TimedOut condition.
	if after < time.Second {
		klog.V(4).Infof("Queueing up deployment %q for a progress check now", d.Name)
		// dc.enqueueRateLimited(d)  requeue
		return time.Duration(0)
	}
	klog.V(4).Infof("Queueing up deployment %q for a progress check after %ds", d.Name, int(after.Seconds()))
	// Add a second to avoid milliseconds skew in AddAfter.
	// See https://github.com/kubernetes/kubernetes/issues/39785#issuecomment-279959133 for more info.
	// dc.enqueueAfter(d, after+time.Second) requeue
	return after
}

var nowFn = func() time.Time { return time.Now() }

func (dc *Executor) patchDeploymentStatusAndAnnotation(ctx context.Context, d *apps.Deployment, newStatus apps.DeploymentStatus) error {
	statusNeedsUpdate := !reflect.DeepEqual(d.Status, newStatus)

	// If neither status nor annotation needs update, return early
	if !statusNeedsUpdate {
		return nil
	}

	// Create a copy for updating both status and annotation
	deploymentCopy := d.DeepCopy()

	// Update status if needed
	if statusNeedsUpdate {
		deploymentCopy.Status = newStatus
	}

	// Update annotation if needed
	// if annotationNeedsUpdate {
	// 	if deploymentCopy.Annotations == nil {
	// 		deploymentCopy.Annotations = make(map[string]string)
	// 	}
	// 	deploymentCopy.Annotations[rolloutsv1alpha1.DeploymentExtraStatusAnnotation] = extraStatusAnno
	// }

	// Use Strategic Merge Patch to update both status and annotation in one operation
	patch := client.MergeFrom(d)
	err := dc.client.Patch(ctx, deploymentCopy, patch)
	if err != nil {
		klog.Errorf("Failed to patch deployment status and annotation: %v", err)
		return err
	}

	return nil
}

func (dc *Executor) getReplicaFailures(allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet) []apps.DeploymentCondition {
	var conditions []apps.DeploymentCondition
	if newRS != nil {
		for _, c := range newRS.Status.Conditions {
			if c.Type != apps.ReplicaSetReplicaFailure {
				continue
			}
			conditions = append(conditions, deploymentutil.ReplicaSetToDeploymentCondition(c))
		}
	}

	// Return failures for the new replica set over failures from old replica sets.
	if len(conditions) > 0 {
		return conditions
	}

	for i := range allRSs {
		rs := allRSs[i]
		if rs == nil {
			continue
		}

		for _, c := range rs.Status.Conditions {
			if c.Type != apps.ReplicaSetReplicaFailure {
				continue
			}
			conditions = append(conditions, deploymentutil.ReplicaSetToDeploymentCondition(c))
		}
	}
	return conditions
}

func (dc *Executor) calculateStatus(allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet) apps.DeploymentStatus {
	availableReplicas := deploymentutil.GetAvailableReplicaCountForReplicaSets(allRSs)
	totalReplicas := deploymentutil.GetReplicaCountForReplicaSets(allRSs)
	unavailableReplicas := totalReplicas - availableReplicas
	// If unavailableReplicas is negative, then that means the Deployment has more available replicas running than
	// desired, e.g. whenever it scales down. In such a case we should simply default unavailableReplicas to zero.
	if unavailableReplicas < 0 {
		unavailableReplicas = 0
	}

	status := apps.DeploymentStatus{
		// TODO: Ensure that if we start retrying status updates, we won't pick up a new Generation value.
		ObservedGeneration:  dc.de.Generation,
		Replicas:            deploymentutil.GetActualReplicaCountForReplicaSets(allRSs),
		UpdatedReplicas:     deploymentutil.GetActualReplicaCountForReplicaSets([]*apps.ReplicaSet{newRS}),
		ReadyReplicas:       deploymentutil.GetReadyReplicaCountForReplicaSets(allRSs),
		AvailableReplicas:   availableReplicas,
		UnavailableReplicas: unavailableReplicas,
		CollisionCount:      dc.de.Status.CollisionCount,
	}

	// Copy conditions one by one so we won't mutate the original object.
	conditions := dc.de.Status.Conditions
	for i := range conditions {
		status.Conditions = append(status.Conditions, conditions[i])
	}

	if availableReplicas >= *(dc.de.Spec.Replicas)-dc.maxUnavailable() {
		minAvailability := deploymentutil.NewDeploymentCondition(apps.DeploymentAvailable, v1.ConditionTrue, deploymentutil.MinimumReplicasAvailable, "Deployment has minimum availability.")
		deploymentutil.SetDeploymentCondition(&status, *minAvailability)
	} else {
		noMinAvailability := deploymentutil.NewDeploymentCondition(apps.DeploymentAvailable, v1.ConditionFalse, deploymentutil.MinimumReplicasUnavailable, "Deployment does not have minimum availability.")
		deploymentutil.SetDeploymentCondition(&status, *noMinAvailability)
	}

	return status
}

func (dc *Executor) reconcileNewReplicaSet(ctx context.Context, allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet) (bool, error) {
	if *(newRS.Spec.Replicas) == *(dc.de.Spec.Replicas) {
		// Scaling not required.
		return false, nil
	}
	if *(newRS.Spec.Replicas) > *(dc.de.Spec.Replicas) {
		// Scale down.
		scaled, _, err := dc.scaleReplicaSetAndRecordEvent(ctx, newRS, *(dc.de.Spec.Replicas), dc.de)
		return scaled, err
	}
	newReplicasCount, err := dc.newRSNewReplicas(dc.de, allRSs, newRS, dc.Br.Spec.Strategy.Steps[dc.Br.Status.CurrentStepIndex])
	if err != nil {
		return false, err
	}
	scaled, _, err := dc.scaleReplicaSetAndRecordEvent(ctx, newRS, newReplicasCount, dc.de)
	return scaled, err
}
func (dc *Executor) scaleReplicaSetAndRecordEvent(ctx context.Context, rs *apps.ReplicaSet, newScale int32, deployment *apps.Deployment) (bool, *apps.ReplicaSet, error) {
	// No need to scale
	if *(rs.Spec.Replicas) == newScale {
		return false, rs, nil
	}
	var scalingOperation string
	if *(rs.Spec.Replicas) < newScale {
		scalingOperation = "up"
	} else {
		scalingOperation = "down"
	}
	scaled, newRS, err := dc.scaleReplicaSet(ctx, rs, newScale, deployment, scalingOperation)
	return scaled, newRS, err
}

func (dc *Executor) scaleReplicaSet(ctx context.Context, rs *apps.ReplicaSet, newScale int32, deployment *apps.Deployment, scalingOperation string) (bool, *apps.ReplicaSet, error) {

	sizeNeedsUpdate := *(rs.Spec.Replicas) != newScale

	annotationsNeedUpdate := deploymentutil.ReplicasAnnotationsNeedUpdate(rs, *(deployment.Spec.Replicas), *(deployment.Spec.Replicas)+dc.maxSurge())

	scaled := false
	var err error
	if sizeNeedsUpdate || annotationsNeedUpdate {
		oldScale := *(rs.Spec.Replicas)

		// Use existing state directly for patching, let API Server handle conflicts
		rsCopy := rs.DeepCopy()
		*(rsCopy.Spec.Replicas) = newScale
		deploymentutil.SetReplicasAnnotations(rsCopy, *(deployment.Spec.Replicas), *(deployment.Spec.Replicas)+dc.maxSurge())

		// Use MergeFrom with optimistic lock for patching, if ResourceVersion conflicts, API Server will return 409 error
		// Controller-runtime will automatically reschedule for reconciliation
		patch := client.MergeFromWithOptions(rs, client.MergeFromWithOptimisticLock{})
		err = dc.client.Patch(ctx, rsCopy, patch)
		if err != nil {
			return scaled, rs, err
		}

		rs = rsCopy
		if sizeNeedsUpdate {
			scaled = true
			dc.eventRecorder.Eventf(deployment, v1.EventTypeNormal, "ScalingReplicaSet", "Scaled %s replica set %s to %d from %d", scalingOperation, rs.Name, newScale, oldScale)
		}
	}
	return scaled, rs, err
}

func (r *Executor) getAllReplicaSetsAndSyncRevision(ctx context.Context, d *apps.Deployment, rsList []*apps.ReplicaSet, createIfNotExisted bool) (*apps.ReplicaSet, []*apps.ReplicaSet, error) {
	_, allOldRSs := deploymentutil.FindOldReplicaSets(d, rsList)

	// Get new replica set with the updated revision number
	newRS, err := r.getNewReplicaSet(ctx, rsList, allOldRSs, createIfNotExisted)
	if err != nil {
		return nil, nil, err
	}

	return newRS, allOldRSs, nil
}

func (r *Executor) getNewReplicaSet(ctx context.Context, rsList, oldRSs []*apps.ReplicaSet, createIfNotExisted bool) (*apps.ReplicaSet, error) {
	existingNewRS := deploymentutil.FindNewReplicaSet(r.de, rsList)

	// Calculate the max revision number among all old RSes
	maxOldRevision := deploymentutil.MaxRevision(oldRSs)
	// Calculate revision number for this new replica set
	newRevision := strconv.FormatInt(maxOldRevision+1, 10)

	// Latest replica set exists. We need to sync its annotations (includes copying all but
	// annotationsToSkip from the parent deployment, and update revision, desiredReplicas,
	// and maxReplicas) and also update the revision annotation in the deployment with the
	// latest revision.
	if existingNewRS != nil {
		rsCopy := existingNewRS.DeepCopy()

		// Set existing new replica set's annotation
		annotationsUpdated := r.setNewReplicaSetAnnotations(rsCopy, newRevision, true, maxRevHistoryLengthInChars)
		minReadySecondsNeedsUpdate := rsCopy.Spec.MinReadySeconds != r.de.Spec.MinReadySeconds
		if annotationsUpdated || minReadySecondsNeedsUpdate {
			// Update the copy with the new minReadySeconds
			if minReadySecondsNeedsUpdate {
				rsCopy.Spec.MinReadySeconds = r.de.Spec.MinReadySeconds
			}

			// Use MergeFrom with optimistic lock for patching, if ResourceVersion conflicts, API Server will return 409 error
			// Controller-runtime will automatically reschedule for reconciliation
			patch := client.MergeFromWithOptions(existingNewRS, client.MergeFromWithOptimisticLock{})
			err := r.client.Patch(ctx, rsCopy, patch)
			if err != nil {
				return nil, err
			}
			return rsCopy, nil
		}

		// Should use the revision in existingNewRS's annotation, since it set by before
		needsUpdate := deploymentutil.SetDeploymentRevision(r.de, rsCopy.Annotations[deploymentutil.RevisionAnnotation])
		// If no other Progressing condition has been recorded and we need to estimate the progress
		// of this deployment then it is likely that old users started caring about progress. In that
		// case we need to take into account the first time we noticed their new replica set.
		cond := deploymentutil.GetDeploymentCondition(r.de.Status, apps.DeploymentProgressing)
		if deploymentutil.HasProgressDeadline(r.de) && cond == nil {
			msg := fmt.Sprintf("Found new replica set %q", rsCopy.Name)
			condition := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionTrue, deploymentutil.FoundNewRSReason, msg)
			deploymentutil.SetDeploymentCondition(&r.de.Status, *condition)
			needsUpdate = true
		}

		if needsUpdate {
			var err error
			// todo 更新方式统一
			if err = r.client.Status().Update(ctx, r.de); err != nil {
				return nil, err
			}
		}
		return rsCopy, nil
	}

	if !createIfNotExisted {
		return nil, nil
	}

	// new ReplicaSet does not exist, create one.
	newRSTemplate := *r.de.Spec.Template.DeepCopy()
	podTemplateSpecHash := deploymentutil.ComputeHash(&newRSTemplate, r.de.Status.CollisionCount)
	newRSTemplate.Labels = labelsutil.CloneAndAddLabel(r.de.Spec.Template.Labels, apps.DefaultDeploymentUniqueLabelKey, podTemplateSpecHash)
	// Add podTemplateHash label to selector.
	newRSSelector := labelsutil.CloneSelectorAndAddLabel(r.de.Spec.Selector, apps.DefaultDeploymentUniqueLabelKey, podTemplateSpecHash)

	// Create new ReplicaSet
	newRS := apps.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			// Make the name deterministic, to ensure idempotence
			Name:            r.de.Name + "-" + podTemplateSpecHash,
			Namespace:       r.de.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(r.de, controllerKind)},
			Labels:          newRSTemplate.Labels,
		},
		Spec: apps.ReplicaSetSpec{
			Replicas:        new(int32),
			MinReadySeconds: r.de.Spec.MinReadySeconds,
			Selector:        newRSSelector,
			Template:        newRSTemplate,
		},
	}
	allRSs := append(oldRSs, &newRS)
	newReplicasCount, err := r.newRSNewReplicas(r.de, allRSs, &newRS, r.Br.Spec.Strategy.Steps[r.Br.Status.CurrentStepIndex])
	if err != nil {
		return nil, err
	}

	// We ensure that newReplicasLowerBound is greater than 0 unless deployment is 0,
	// this is because if we set new replicas as 0, the native deployment controller
	// will flight with ours.
	newReplicasLowerBound := r.newRSReplicasLowerBound(r.de)

	*(newRS.Spec.Replicas) = integer.Int32Max(newReplicasCount, newReplicasLowerBound)
	// Set new replica set's annotation
	r.setNewReplicaSetAnnotations(&newRS, newRevision, false, maxRevHistoryLengthInChars)
	// Create the new ReplicaSet. If it already exists, then we need to check for possible
	// hash collisions. If there is any other error, we need to report it in the status of
	// the Deployment.
	alreadyExists := false
	var createdRS *apps.ReplicaSet
	err = r.client.Create(ctx, &newRS)
	if err == nil {
		createdRS = &newRS
	}
	switch {
	// We may end up hitting this due to a slow cache or a fast resync of the Deployment.
	case errors.IsAlreadyExists(err):
		alreadyExists = true

		// Fetch a copy of the ReplicaSet.
		rs := &apps.ReplicaSet{}
		rsErr := r.client.Get(ctx, client.ObjectKey{Namespace: newRS.Namespace, Name: newRS.Name}, rs)
		if rsErr != nil {
			return nil, rsErr
		}

		// If the Deployment owns the ReplicaSet and the ReplicaSet's PodTemplateSpec is semantically
		// deep equal to the PodTemplateSpec of the Deployment, it's the Deployment's new ReplicaSet.
		// Otherwise, this is a hash collision and we need to increment the collisionCount field in
		// the status of the Deployment and requeue to try the creation in the next sync.
		controllerRef := metav1.GetControllerOf(rs)
		if controllerRef != nil && controllerRef.UID == r.de.UID && deploymentutil.EqualIgnoreHash(&r.de.Spec.Template, &rs.Spec.Template) {
			createdRS = rs
			err = nil
			break
		}

		// Matching ReplicaSet is not equal - increment the collisionCount in the DeploymentStatus
		// and requeue the Deployment.
		if r.de.Status.CollisionCount == nil {
			r.de.Status.CollisionCount = new(int32)
		}
		preCollisionCount := *r.de.Status.CollisionCount
		*r.de.Status.CollisionCount++
		// Update the collisionCount for the Deployment and let it requeue by returning the original
		// error.
		dErr := r.client.Status().Update(ctx, r.de)
		if dErr == nil {
			klog.V(2).Infof("Found a hash collision for deployment %q - bumping collisionCount (%d->%d) to resolve it", r.de.Name, preCollisionCount, *r.de.Status.CollisionCount)
		} else {
			klog.Errorf("Failed to update deployment collision count: %v", dErr)
		}
		return nil, err
	case errors.HasStatusCause(err, v1.NamespaceTerminatingCause):
		// if the namespace is terminating, all subsequent creates will fail and we can safely do nothing
		return nil, err
	case err != nil:
		msg := fmt.Sprintf("Failed to create new replica set %q: %v", newRS.Name, err)
		if deploymentutil.HasProgressDeadline(r.de) {
			cond := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionFalse, deploymentutil.FailedRSCreateReason, msg)
			deploymentutil.SetDeploymentCondition(&r.de.Status, *cond)
			// We don't really care about this error at this point, since we have a bigger issue to report.
			// TODO: Identify which errors are permanent and switch DeploymentIsFailed to take into account
			// these reasons as well. Related issue: https://github.com/kubernetes/kubernetes/issues/18568
			if updateErr := r.client.Status().Update(ctx, r.de); updateErr != nil {
				klog.Errorf("Failed to update deployment status after RS creation failure: %v", updateErr)
			}
		}
		r.eventRecorder.Eventf(r.de, v1.EventTypeWarning, deploymentutil.FailedRSCreateReason, msg)
		return nil, err
	}
	if !alreadyExists && newReplicasCount > 0 {
		r.eventRecorder.Eventf(r.de, v1.EventTypeNormal, "ScalingReplicaSet", "Scaled up replica set %s to %d", createdRS.Name, newReplicasCount)
	}

	needsUpdate := deploymentutil.SetDeploymentRevision(r.de, newRevision)
	if !alreadyExists && deploymentutil.HasProgressDeadline(r.de) {
		msg := fmt.Sprintf("Created new replica set %q", createdRS.Name)
		condition := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionTrue, deploymentutil.NewReplicaSetReason, msg)
		deploymentutil.SetDeploymentCondition(&r.de.Status, *condition)
		needsUpdate = true
	}
	if needsUpdate {
		if updateErr := r.client.Status().Update(ctx, r.de); updateErr != nil {
			klog.Errorf("Failed to update deployment status: %v", updateErr)
			err = updateErr
		}
	}
	return createdRS, err
}

func (r *Executor) getReplicaSetsForDeployment(ctx context.Context, d *apps.Deployment) ([]*apps.ReplicaSet, error) {
	deploymentSelector, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("deployment %s/%s has invalid label selector: %v", d.Namespace, d.Name, err)
	}

	// List all ReplicaSets using runtimeClient
	rsList := &apps.ReplicaSetList{}
	err = r.client.List(ctx, rsList, client.InNamespace(d.Namespace), client.MatchingLabelsSelector{Selector: deploymentSelector})
	if err != nil {
		return nil, fmt.Errorf("list %s/%s rs failed:%v", d.Namespace, d.Name, err)
	}

	// select rs owner by current deployment
	ownedRSs := make([]*apps.ReplicaSet, 0)
	for i := range rsList.Items {
		rs := &rsList.Items[i]
		if !rs.DeletionTimestamp.IsZero() {
			continue
		}

		if metav1.IsControlledBy(rs, d) {
			ownedRSs = append(ownedRSs, rs)
		}
	}
	return ownedRSs, nil
}

func UpdateObj(ctx context.Context, cli client.Client, obj client.Object, patch func(object client.Object)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := cli.Get(ctx, types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}, obj); err != nil {
			return err
		}
		patch(obj)
		return cli.Update(ctx, obj)
	})
}
func UpdateObjStatus(ctx context.Context, cli client.Client, obj client.Object, patch func(object client.Object)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := cli.Get(ctx, types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}, obj); err != nil {
			return err
		}
		patch(obj)
		return cli.Status().Update(ctx, obj)
	})
}

func (dc *Executor) maxSurge() int32 {
	maxSurge, _, _ := deploymentutil.ResolveFenceposts(dc.Br.Status.MaxSurge, dc.Br.Status.MaxUnavailable, *(dc.de.Spec.Replicas))
	return maxSurge
}

func (dc *Executor) setNewReplicaSetAnnotations(newRS *apps.ReplicaSet, newRevision string, exists bool, revHistoryLimitInChars int) bool {
	// First, copy deployment's annotations (except for apply and revision annotations)
	annotationChanged := deploymentutil.CopyDeploymentAnnotationsToReplicaSet(dc.de, newRS)
	// Then, update replica set's revision annotation
	if newRS.Annotations == nil {
		newRS.Annotations = make(map[string]string)
	}
	oldRevision, ok := newRS.Annotations[deploymentutil.RevisionAnnotation]
	// The newRS's revision should be the greatest among all RSes. Usually, its revision number is newRevision (the max revision number
	// of all old RSes + 1). However, it's possible that some of the old RSes are deleted after the newRS revision being updated, and
	// newRevision becomes smaller than newRS's revision. We should only update newRS revision when it's smaller than newRevision.

	oldRevisionInt, err := strconv.ParseInt(oldRevision, 10, 64)
	if err != nil {
		if oldRevision != "" {
			klog.Warningf("Updating replica set revision OldRevision not int %s", err)
			return false
		}
		// If the RS annotation is empty then initialize it to 0
		oldRevisionInt = 0
	}
	newRevisionInt, err := strconv.ParseInt(newRevision, 10, 64)
	if err != nil {
		klog.Warningf("Updating replica set revision NewRevision not int %s", err)
		return false
	}
	if oldRevisionInt < newRevisionInt {
		newRS.Annotations[deploymentutil.RevisionAnnotation] = newRevision
		annotationChanged = true
		klog.V(4).Infof("Updating replica set %q revision to %s", newRS.Name, newRevision)
	}
	// If a revision annotation already existed and this replica set was updated with a new revision
	// then that means we are rolling back to this replica set. We need to preserve the old revisions
	// for historical information.
	if ok && oldRevisionInt < newRevisionInt {
		revisionHistoryAnnotation := newRS.Annotations[deploymentutil.RevisionHistoryAnnotation]
		oldRevisions := strings.Split(revisionHistoryAnnotation, ",")
		if len(oldRevisions[0]) == 0 {
			newRS.Annotations[deploymentutil.RevisionHistoryAnnotation] = oldRevision
		} else {
			totalLen := len(revisionHistoryAnnotation) + len(oldRevision) + 1
			// index for the starting position in oldRevisions
			start := 0
			for totalLen > revHistoryLimitInChars && start < len(oldRevisions) {
				totalLen = totalLen - len(oldRevisions[start]) - 1
				start++
			}
			if totalLen <= revHistoryLimitInChars {
				oldRevisions = append(oldRevisions[start:], oldRevision)
				newRS.Annotations[deploymentutil.RevisionHistoryAnnotation] = strings.Join(oldRevisions, ",")
			} else {
				klog.Warningf("Not appending revision due to length limit of %v reached", revHistoryLimitInChars)
			}
		}
	}
	// If the new replica set is about to be created, we need to add replica annotations to it.
	if !exists && deploymentutil.SetReplicasAnnotations(newRS, *(dc.de.Spec.Replicas), *(dc.de.Spec.Replicas)+dc.maxSurge()) {
		annotationChanged = true
	}
	return annotationChanged
}

func (dc *Executor) newRSNewReplicas(deployment *apps.Deployment, allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet, currentStep v1alpha1.Step) (int32, error) {
	// Find the total number of pods
	currentPodCount := deploymentutil.GetReplicaCountForReplicaSets(allRSs)
	switch {
	case currentPodCount > *newRS.Spec.Replicas:
		// Do not scale down due to partition settings.
		scaleUpLimit := deploymentutil.NewRSReplicasLimit(currentStep.Replicas, deployment)
		if *newRS.Spec.Replicas >= scaleUpLimit {
			// Cannot scale up.
			return *(newRS.Spec.Replicas), nil
		}
		// Do not scale up due to exceeded current replicas.
		maxTotalPods := *(deployment.Spec.Replicas) + dc.maxSurge()
		if currentPodCount >= maxTotalPods {
			// Cannot scale up.
			return *(newRS.Spec.Replicas), nil
		}
		// Scale up.
		scaleUpCount := maxTotalPods - currentPodCount
		// Do not exceed the number of desired replicas.
		scaleUpCount = int32(integer.IntMin(int(scaleUpCount), int(*(deployment.Spec.Replicas)-*(newRS.Spec.Replicas))))
		// Do not exceed the number of partition replicas.
		return integer.Int32Min(*(newRS.Spec.Replicas)+scaleUpCount, scaleUpLimit), nil
	default:
		// If there is ONLY ONE active replica set, just be in line with deployment replicas.
		return *(deployment.Spec.Replicas), nil
	}
}

func (dc *Executor) newRSReplicasLowerBound(deployment *apps.Deployment) int32 {
	if dc.maxSurge() > 0 {
		return int32(0)
	}
	return integer.Int32Min(int32(1), *deployment.Spec.Replicas)
}
