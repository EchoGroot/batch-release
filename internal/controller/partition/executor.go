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
	"github.com/go-logr/logr"
	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	intstrutil "k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/integer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

const (
	maxRevHistoryLengthInChars = 2000
	DefaultRetryDuration       = 2 * time.Second
)

var controllerKind = apps.SchemeGroupVersion.WithKind("Deployment")

type Executor struct {
	Br            *v1alpha1.BatchRelease
	de            *apps.Deployment
	client        client.Client
	eventRecorder record.EventRecorder
	log           logr.Logger
}

func NewExecutor(br *v1alpha1.BatchRelease, de *apps.Deployment, client client.Client, eventRecorder record.EventRecorder, log logr.Logger) *Executor {
	return &Executor{
		Br:            br,
		de:            de,
		client:        client,
		eventRecorder: eventRecorder,
		log:           log,
	}
}

func (e *Executor) Do(ctx context.Context) (ctrl.Result, error) {
	e.cleanStatusReasonAndMessage()

	if e.handlePodTemplateUpdate() {
		return ctrl.Result{}, nil
	}

	switch e.Br.Status.Phase {
	default:
		e.Br.Status.Phase = v1alpha1.PhaseInitial
		fallthrough
	case v1alpha1.PhaseInitial:
		if success, err := e.init(ctx); err != nil || !success {
			return ctrl.Result{}, err
		}
		e.eventRecorder.Eventf(e.Br, v1.EventTypeNormal, "BatchReleaseStarted", "BatchRelease %s started for Deployment %s", e.Br.Name, e.de.Name)
		e.Br.Status.Phase = v1alpha1.PhaseRollingUpdate
		return ctrl.Result{}, nil
	case v1alpha1.PhaseRollingUpdate:
		if e.isPreRollback() {
			e.eventRecorder.Eventf(e.Br, v1.EventTypeNormal, "RollbackRequested", "Rollback requested during rolling update for BatchRelease %s", e.Br.Name)
			return e.preRollback(ctx, e.Br.Annotations[v1alpha1.CurrentStableReversionKey], e.Br.Annotations[v1alpha1.CurrentStableReversionRawKey])
		}
		return e.rollingUpdate(ctx)
	case v1alpha1.PhaseRollingBack:
		if e.isPreRollback() {
			return e.removeRollbackMark(ctx)
		}
		return e.rollingUpdate(ctx)
	case v1alpha1.PhaseFinalizing:
		return e.finalize(ctx)
	case v1alpha1.PhaseCompleted:
		if e.isPreRollback() {
			e.eventRecorder.Eventf(e.Br, v1.EventTypeNormal, "RollbackRequested", "Rollback requested after completion for BatchRelease %s", e.Br.Name)
			return e.preRollback(ctx, e.Br.Annotations[v1alpha1.LastStableReversionKey], e.Br.Annotations[v1alpha1.LastStableReversionRawKey])
		}
		return ctrl.Result{}, nil
	}
}

func (e *Executor) cleanStatusReasonAndMessage() {
	e.Br.Status.Reason, e.Br.Status.Message = "", ""
}

func (e *Executor) handlePodTemplateUpdate() (update bool) {
	podTemplateHash := deploymentutil.ComputeHash(e.Br.Spec.Template.DeepCopy(), nil)
	if len(e.Br.Status.ObservedUpdateReversion) > 0 && e.Br.Status.ObservedUpdateReversion != podTemplateHash && !e.isPreRollback() {
		e.eventRecorder.Eventf(e.Br, v1.EventTypeNormal, "BatchReleaseUpdated", "BatchRelease %s updated for Deployment %s", e.Br.Name, e.de.Name)
		e.Br.Status.ObservedUpdateReversion = podTemplateHash
		e.Br.Status.Phase = v1alpha1.PhaseInitial
		return true
	}
	return false
}

func (e *Executor) isPreRollback() bool {
	_, ok := e.Br.Annotations[v1alpha1.RollbackMark]
	return ok
}

func (e *Executor) init(ctx context.Context) (bool, error) {
	if e.Br.Status.MaxUnavailable == nil || e.Br.Status.MaxSurge == nil {
		if e.de.Spec.Strategy.RollingUpdate == nil {
			return false, fmt.Errorf("deployment %s/%s does not have RollingUpdate strategy", e.de.Namespace, e.de.Name)
		}
		e.Br.Status.MaxUnavailable = e.de.Spec.Strategy.RollingUpdate.MaxUnavailable
		e.Br.Status.MaxSurge = e.de.Spec.Strategy.RollingUpdate.MaxSurge
		return false, nil
	}

	if _, ok := e.Br.Annotations[v1alpha1.CurrentStableReversionKey]; !ok {
		return false, e.recordStableVersion(ctx, e.de.Spec.Template.DeepCopy())
	}

	e.Br.Status.CurrentStepIndex = 0
	e.Br.Status.CurrentStepState = ""
	e.Br.Status.UpdatedReadyReplicas = 0

	podTemplateHash := deploymentutil.ComputeHash(e.Br.Spec.Template.DeepCopy(), nil)
	e.Br.Status.ObservedUpdateReversion = podTemplateHash

	if err := UpdateObj(ctx, e.client, e.de.DeepCopy(), func(object client.Object) {
		d := object.(*apps.Deployment)
		d.Spec.Template = e.Br.Spec.Template
		d.Spec.Paused = true
		d.Spec.Strategy.Type = apps.RecreateDeploymentStrategyType
		d.Spec.Strategy.RollingUpdate = nil
		if d.Annotations == nil {
			d.Annotations = make(map[string]string)
		}
		d.Annotations[v1alpha1.BatchReleaseControlInfoAnno] = jsonutil.DumpJSON(metav1.NewControllerRef(e.Br, e.Br.GetObjectKind().GroupVersionKind()))
	}); err != nil {
		return false, err
	}
	e.log.V(1).Info("Successfully updated Deployment",
		"deployment", e.de.Name)

	return true, nil
}

func (e *Executor) preRollback(ctx context.Context, reversion, podTemplate string) (ctrl.Result, error) {
	e.log.V(2).Info("Rolling back")
	e.eventRecorder.Eventf(e.Br, v1.EventTypeNormal, "RollbackStarted", "Rollback started for BatchRelease %s", e.Br.Name)

	if reversion != deploymentutil.ComputeHash(e.Br.Spec.Template.DeepCopy(), nil) {
		var podTemplateObj = v1.PodTemplateSpec{}
		if err := yaml.Unmarshal([]byte(podTemplate), &podTemplateObj); err != nil {
			return ctrl.Result{}, err
		}
		err := UpdateObj(ctx, e.client, e.Br.DeepCopy(), func(object client.Object) {
			br := object.(*v1alpha1.BatchRelease)
			br.Spec.Strategy.Steps = []v1alpha1.Step{
				{Replicas: intstr.Parse("1")},
				{Replicas: intstr.Parse("100%")},
			}
			br.Spec.Template = podTemplateObj
		})
		return ctrl.Result{}, err
	}

	if success, err := e.init(ctx); err != nil || !success {
		return ctrl.Result{}, err
	}

	e.Br.Status.Phase = v1alpha1.PhaseRollingBack
	return ctrl.Result{}, nil
}

func (e *Executor) rollingUpdate(ctx context.Context) (ctrl.Result, error) {
	rsList, err := e.getReplicaSetsForDeployment(ctx, e.de)
	if err != nil {
		return ctrl.Result{}, err
	}

	scalingEvent, err := e.isScalingEvent(ctx, rsList)
	if err != nil {
		return ctrl.Result{}, err
	}

	if scalingEvent {
		if err := e.sync(ctx, rsList); err != nil {
			return ctrl.Result{}, err
		}
		e.Br.Status.CurrentStepState = v1alpha1.StepStateInitial
		return ctrl.Result{}, nil
	}

	return e.stepByStep(ctx, rsList)
}

func (e *Executor) stepByStep(ctx context.Context, rsList []*apps.ReplicaSet) (ctrl.Result, error) {
	switch e.Br.Status.CurrentStepState {
	default:
		e.Br.Status.CurrentStepState = v1alpha1.StepStateInitial
		fallthrough
	case v1alpha1.StepStateInitial:
		e.log.V(1).Info("Start release step",
			"step", e.Br.Status.CurrentStepIndex)
		e.Br.Status.CurrentStepState = v1alpha1.StepStateUpgrade
	case v1alpha1.StepStateUpgrade:
		newRS, oldRSs, err := e.getAllReplicaSetsAndSyncRevision(ctx, rsList, true)
		if err != nil {
			return ctrl.Result{}, err
		}
		allRSs := append(oldRSs, newRS)

		// Scale up, if we can.
		scaledUp, err := e.reconcileNewReplicaSet(ctx, allRSs, newRS)
		if err != nil {
			return ctrl.Result{}, err
		}
		if scaledUp {
			// Update DeploymentStatus
			return ctrl.Result{}, e.syncRolloutStatus(ctx, allRSs, newRS)
		}

		// Scale down, if we can.
		scaledDown, err := e.reconcileOldReplicaSets(ctx, allRSs, deploymentutil.FilterActiveReplicaSets(oldRSs), newRS, e.de)
		if err != nil {
			return ctrl.Result{}, err
		}
		if scaledDown {
			// Update DeploymentStatus
			return ctrl.Result{}, e.syncRolloutStatus(ctx, allRSs, newRS)
		}

		// Sync deployment status
		if err = e.syncRolloutStatus(ctx, allRSs, newRS); err != nil {
			return ctrl.Result{}, err
		}

		if err := e.IsBatchReady(); err != nil {
			e.log.V(2).Info("Release step not ready, requeue",
				"step", e.Br.Status.CurrentStepIndex,
				"reason", err.Error())
			return ctrl.Result{RequeueAfter: DefaultRetryDuration}, err
		}

		e.Br.Status.CurrentStepState = v1alpha1.StepStateBlocking
		return ctrl.Result{}, nil
	case v1alpha1.StepStateBlocking:
		if e.Br.Status.CurrentStepIndex == int32(len(e.Br.Spec.Strategy.Steps)-1) {
			e.Br.Status.CurrentStepState = v1alpha1.StepStateCompleted
			return ctrl.Result{}, nil
		}
		e.Br.Status.Reason, e.Br.Status.Message = v1alpha1.BatchReleaseReasonStepBlocking, v1alpha1.StepBlockingMessage
		e.eventRecorder.Eventf(e.Br, v1.EventTypeNormal, "BatchStepBlocking", "Batch step %d is blocking for BatchRelease %s", e.Br.Status.CurrentStepIndex, e.Br.Name)
		return ctrl.Result{}, nil
	case v1alpha1.StepStateCompleted:
		e.log.V(1).Info("Release step completed",
			"step", e.Br.Status.CurrentStepIndex)
		e.eventRecorder.Eventf(e.Br, v1.EventTypeNormal, "BatchStepCompleted", "Batch step %d completed for BatchRelease %s", e.Br.Status.CurrentStepIndex, e.Br.Name)
		if e.Br.Status.CurrentStepIndex == int32(len(e.Br.Spec.Strategy.Steps))-1 {
			e.Br.Status.Phase = v1alpha1.PhaseFinalizing
			return ctrl.Result{}, nil
		}

		e.Br.Status.CurrentStepIndex++
		e.Br.Status.CurrentStepState = v1alpha1.StepStateInitial
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, nil
}

func (e *Executor) removeRollbackMark(ctx context.Context) (ctrl.Result, error) {
	e.log.V(1).Info("Removing leftover RollbackMark in RollingBack phase")
	err := UpdateObj(ctx, e.client, e.Br, func(object client.Object) {
		br := object.(*v1alpha1.BatchRelease)
		delete(br.Annotations, v1alpha1.RollbackMark)
	})
	return ctrl.Result{}, err
}

func (e *Executor) finalize(ctx context.Context) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	err := UpdateObj(ctx, e.client, e.de.DeepCopy(), func(object client.Object) {
		newDe := object.(*apps.Deployment)
		newDe.Spec.Paused = false
		newDe.Spec.Strategy.Type = apps.RollingUpdateDeploymentStrategyType
		newDe.Spec.Strategy.RollingUpdate = &apps.RollingUpdateDeployment{MaxSurge: e.Br.Status.MaxSurge, MaxUnavailable: e.Br.Status.MaxUnavailable}
		delete(newDe.Annotations, v1alpha1.BatchReleaseControlInfoAnno)
	})
	if err != nil {
		log.Error(err, "Failed to update deployment",
			"namespace", e.de.Namespace,
			"name", e.de.Name)
		return ctrl.Result{RequeueAfter: DefaultRetryDuration}, err
	}

	if err := e.recordStableVersion(ctx, e.Br.Spec.Template.DeepCopy()); err != nil {
		return ctrl.Result{}, err
	}

	e.eventRecorder.Eventf(e.Br, v1.EventTypeNormal, "BatchReleaseCompleted", "BatchRelease %s completed for Deployment %s", e.Br.Name, e.de.Name)
	e.Br.Status.Phase = v1alpha1.PhaseCompleted
	return ctrl.Result{}, nil
}

func (e *Executor) isScalingEvent(ctx context.Context, rsList []*apps.ReplicaSet) (bool, error) {
	newRS, oldRSs, err := e.getAllReplicaSetsAndSyncRevision(ctx, rsList, false)
	if err != nil {
		return false, err
	}
	allRSs := append(oldRSs, newRS)
	for _, rs := range deploymentutil.FilterActiveReplicaSets(allRSs) {
		desired, ok := deploymentutil.GetReplicasAnnotation(rs)
		if !ok {
			continue
		}
		if desired != *(e.de.Spec.Replicas) {
			return true, nil
		}
	}
	return false, nil
}

func (e *Executor) IsBatchReady() error {
	currentBatch := e.Br.Status.CurrentStepIndex
	desiredPartition := e.Br.Spec.Strategy.Steps[currentBatch].Replicas
	DesiredUpdatedReplicas := deploymentutil.NewRSReplicasLimit(desiredPartition, e.de)
	if e.de.Status.UpdatedReplicas < DesiredUpdatedReplicas {
		return fmt.Errorf("current batch not ready: updated replicas not satisfied, UpdatedReplicas %d < DesiredUpdatedReplicas %d", e.de.Status.UpdatedReplicas, DesiredUpdatedReplicas)
	}

	unavailableToleration := allowedUnavailable(e.Br.Status.MaxUnavailable, e.de.Status.UpdatedReplicas)
	if unavailableToleration+e.Br.Status.UpdatedReadyReplicas < DesiredUpdatedReplicas {
		return fmt.Errorf("current batch not ready: updated ready replicas not satisfied, allowedUnavailable + UpdatedReadyReplicas %d < DesiredUpdatedReplicas %d", unavailableToleration+e.Br.Status.UpdatedReadyReplicas, DesiredUpdatedReplicas)
	}

	if DesiredUpdatedReplicas > 0 && e.Br.Status.UpdatedReadyReplicas == 0 {
		return fmt.Errorf("current batch not ready: no updated ready replicas, DesiredUpdatedReplicas %d > 0 and UpdatedReadyReplicas %d = 0", DesiredUpdatedReplicas, e.Br.Status.UpdatedReadyReplicas)
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

func (e *Executor) sync(ctx context.Context, rsList []*apps.ReplicaSet) error {
	newRS, oldRSs, err := e.getAllReplicaSetsAndSyncRevision(ctx, rsList, false)
	if err != nil {
		return err
	}
	if err := e.scale(ctx, newRS, oldRSs); err != nil {
		// If we get an error while trying to scale, the deployment will be requeued
		// so we can abort this resync
		return err
	}

	allRSs := append(oldRSs, newRS)
	return e.syncDeploymentStatus(ctx, allRSs, newRS)
}

func (e *Executor) syncDeploymentStatus(ctx context.Context, allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet) error {
	newStatus := e.calculateStatus(allRSs, newRS)

	// Calculate extra status annotation
	// extraStatusAnno, err := e.updateDeploymentExtraStatus(ctx, newRS, d)
	// if err != nil {
	// 	return nil // no need to retry
	// }

	// Update both status and annotation
	return e.patchDeploymentStatusAndAnnotation(ctx, e.de, newStatus)
}

func (e *Executor) scale(ctx context.Context, newRS *apps.ReplicaSet, oldRSs []*apps.ReplicaSet) error {
	// If there is only one active replica set then we should scale that up to the full count of the
	// deployment. If there is no active replica set, then we should scale up the newest replica set.
	if activeOrLatest := deploymentutil.FindActiveOrLatest(newRS, oldRSs); activeOrLatest != nil {
		if *(activeOrLatest.Spec.Replicas) == *(e.de.Spec.Replicas) {
			return nil
		}
		_, _, err := e.scaleReplicaSetAndRecordEvent(ctx, activeOrLatest, *(e.de.Spec.Replicas), e.de)
		return err
	}

	// If the new replica set is saturated, old replica sets should be fully scaled down.
	// This case handles replica set adoption during a saturated new replica set.
	if deploymentutil.IsSaturated(e.de, newRS) {
		for _, old := range deploymentutil.FilterActiveReplicaSets(oldRSs) {
			if _, _, err := e.scaleReplicaSetAndRecordEvent(ctx, old, 0, e.de); err != nil {
				return err
			}
		}
		return nil
	}

	// There are old replica sets with pods and the new replica set is not saturated.
	// We need to proportionally scale all replica sets (new and old) in case of a
	// rolling deployment.
	if deploymentutil.IsRollingUpdate(e.de) {
		allRSs := deploymentutil.FilterActiveReplicaSets(append(oldRSs, newRS))
		allRSsReplicas := deploymentutil.GetReplicaCountForReplicaSets(allRSs)

		allowedSize := int32(0)
		if *(e.de.Spec.Replicas) > 0 {
			allowedSize = *(e.de.Spec.Replicas)
		}

		// Number of additional replicas that can be either added or removed from the total
		// replicas count. These replicas should be distributed proportionally to the active
		// replica sets.
		deploymentReplicasToAdd := allowedSize - allRSsReplicas

		// Scale down the unhealthy replicas in old replica sets firstly to avoid some bad cases.
		// For example:
		//       _______________________________________________________________________________________
		//       | ReplicaSet    |       oldRS-1      |   oldRS-2            |          newRS           |
		//       | --------------| -------------------|----------------------|--------------------------|
		//       | Replicas      |  4 healthy Pods    |  2 unhealthy Pods    |    4 unhealthy Pods      |
		//       ---------------------------------------------------------------------------------------
		// If we want to scale down these replica sets from 10 to 6, we expect to scale down the oldRS-2
		// from 2 to 0 firstly, then scale down oldRS-1 1 Pod and newRS 1 Pod based on proportion.
		//
		// We do not scale down the newRS unhealthy Pods with higher priority, because these new revision
		// Pods may be just created, not the one with the crash or other problems.
		var err error
		var cleanupCount int32
		if deploymentReplicasToAdd < 0 {
			oldRSs, cleanupCount, err = e.cleanupUnhealthyReplicas(ctx, oldRSs, e.de, -deploymentReplicasToAdd)
			if err != nil {
				return err
			}
			e.log.V(4).Info("Cleaned up unhealthy replicas from old RSes during scaling",
				"count", cleanupCount)
			deploymentReplicasToAdd += cleanupCount
			allRSs = deploymentutil.FilterActiveReplicaSets(append(oldRSs, newRS))
		}

		// The additional replicas should be distributed proportionally amongst the active
		// replica sets from the larger to the smaller in size replica set. Scaling direction
		// drives what happens in case we are trying to scale replica sets of the same size.
		// In such a case when scaling up, we should scale up newer replica sets first, and
		// when scaling down, we should scale down older replica sets first.
		var scalingOperation string
		switch {
		case deploymentReplicasToAdd > 0:
			sort.Sort(deploymentutil.ReplicaSetsBySizeNewer(allRSs))
			scalingOperation = "up"

		case deploymentReplicasToAdd < 0:
			sort.Sort(deploymentutil.ReplicaSetsBySizeOlder(allRSs))
			scalingOperation = "down"
		}

		// Iterate over all active replica sets and estimate proportions for each of them.
		// The absolute value of deploymentReplicasAdded should never exceed the absolute
		// value of deploymentReplicasToAdd.
		deploymentReplicasAdded := int32(0)
		nameToSize := make(map[string]int32)
		for i := range allRSs {
			rs := allRSs[i]

			// Estimate proportions if we have replicas to add, otherwise simply populate
			// nameToSize with the current sizes for each replica set.
			if deploymentReplicasToAdd != 0 {
				proportion := e.GetProportion(rs, deploymentReplicasToAdd, deploymentReplicasAdded)

				nameToSize[rs.Name] = *(rs.Spec.Replicas) + proportion
				deploymentReplicasAdded += proportion
			} else {
				nameToSize[rs.Name] = *(rs.Spec.Replicas)
			}
		}

		// Update all replica sets
		for i := range allRSs {
			rs := allRSs[i]

			// Add/remove any leftovers to the largest replica set.
			if i == 0 && deploymentReplicasToAdd != 0 {
				leftover := deploymentReplicasToAdd - deploymentReplicasAdded
				nameToSize[rs.Name] = nameToSize[rs.Name] + leftover
				if nameToSize[rs.Name] < 0 {
					nameToSize[rs.Name] = 0
				}
			}

			// TODO: Use transactions when we have them.
			if _, _, err := e.scaleReplicaSet(ctx, rs, nameToSize[rs.Name], e.de, scalingOperation); err != nil {
				// Return as soon as we fail, the deployment is requeued
				return err
			}
		}
	}
	return nil
}

func (e *Executor) GetProportion(rs *apps.ReplicaSet, deploymentReplicasToAdd, deploymentReplicasAdded int32) int32 {
	if rs == nil || *(rs.Spec.Replicas) == 0 || deploymentReplicasToAdd == 0 || deploymentReplicasToAdd == deploymentReplicasAdded {
		return int32(0)
	}

	rsFraction := e.getReplicaSetFraction(*rs)
	allowed := deploymentReplicasToAdd - deploymentReplicasAdded

	if deploymentReplicasToAdd > 0 {
		// Use the minimum between the replica set fraction and the maximum allowed replicas
		// when scaling up. This way we ensure we will not scale up more than the allowed
		// replicas we can add.
		return integer.Int32Min(rsFraction, allowed)
	}
	// Use the maximum between the replica set fraction and the maximum allowed replicas
	// when scaling down. This way we ensure we will not scale down more than the allowed
	// replicas we can remove.
	return integer.Int32Max(rsFraction, allowed)
}

func (e *Executor) getReplicaSetFraction(rs apps.ReplicaSet) int32 {
	// If we are scaling down to zero then the fraction of this replica set is its whole size (negative)
	if *(e.de.Spec.Replicas) == int32(0) {
		return -*(rs.Spec.Replicas)
	}

	deploymentReplicas := *(e.de.Spec.Replicas) + e.maxSurge()
	annotatedReplicas, ok := deploymentutil.GetMaxReplicasAnnotation(&rs)
	if !ok {
		// If we cannot find the annotation then fallback to the current deployment size. Note that this
		// will not be an accurate proportion estimation in case other replica sets have different values
		// which means that the deployment was scaled at some point but we at least will stay in limits
		// due to the min-max comparisons in getProportion.
		annotatedReplicas = e.de.Status.Replicas
	}

	// We should never proportionally scale up from zero which means rs.spec.replicas and annotatedReplicas
	// will never be zero here.
	newRSsize := (float64(*(rs.Spec.Replicas) * deploymentReplicas)) / float64(annotatedReplicas)
	return integer.RoundToInt32(newRSsize) - *(rs.Spec.Replicas)
}

func (e *Executor) reconcileOldReplicaSets(ctx context.Context, allRSs []*apps.ReplicaSet, oldRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet, deployment *apps.Deployment) (bool, error) {
	oldPodsCount := deploymentutil.GetReplicaCountForReplicaSets(oldRSs)
	if oldPodsCount == 0 {
		// Can't scale down further
		return false, nil
	}

	allPodsCount := deploymentutil.GetReplicaCountForReplicaSets(allRSs)
	e.log.V(4).Info("New replica set has available pods",
		"namespace", newRS.Namespace,
		"name", newRS.Name,
		"availableReplicas", newRS.Status.AvailableReplicas)
	maxUnavailable := e.maxUnavailable()

	// Old RSes should obey the limitation of partition.
	ScaleDownOldLimit := ScaleDownLimitForOld(oldRSs, newRS, deployment, e.Br.Spec.Strategy.Steps[e.Br.Status.CurrentStepIndex].Replicas)
	if ScaleDownOldLimit <= 0 {
		// Old replica sets do not satisfied as partition expectation, scale up.
		return e.scaleUpOldReplicaSets(ctx, oldRSs, -ScaleDownOldLimit, deployment)
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
	oldRSs, cleanupCount, err := e.cleanupUnhealthyReplicas(ctx, oldRSs, deployment, maxScaledDown)
	if err != nil {
		return false, nil
	}
	e.log.V(4).Info("Cleaned up unhealthy replicas from old RSes",
		"count", cleanupCount)

	// Scale down old replica sets, need check maxUnavailable to ensure we can scale down
	allRSs = append(oldRSs, newRS)
	scaledDownCount, err := e.scaleDownOldReplicaSetsForRollingUpdate(ctx, allRSs, oldRSs, deployment)
	if err != nil {
		return false, nil
	}
	e.log.V(4).Info("Scaled down old RSes of deployment",
		"deployment", deployment.Name,
		"count", scaledDownCount)

	totalScaledDown := cleanupCount + scaledDownCount
	return totalScaledDown > 0, nil
}

func (e *Executor) maxUnavailable() int32 {
	if *(e.de.Spec.Replicas) == 0 {
		return int32(0)
	}
	// Error caught by validation
	_, maxUnavailable, _ := deploymentutil.ResolveFenceposts(e.Br.Status.MaxSurge, e.Br.Status.MaxUnavailable, *(e.de.Spec.Replicas))
	if maxUnavailable > *e.de.Spec.Replicas {
		return *e.de.Spec.Replicas
	}
	return maxUnavailable
}

func (e *Executor) scaleUpOldReplicaSets(ctx context.Context, oldRSs []*apps.ReplicaSet, scaledUpCount int32, deployment *apps.Deployment) (bool, error) {
	if scaledUpCount <= 0 || len(oldRSs) == 0 {
		return false, nil
	}
	// Scale up the biggest one or older.
	sort.Sort(deploymentutil.ReplicaSetsBySizeOlder(oldRSs))
	newScale := (*oldRSs[0].Spec.Replicas) + scaledUpCount
	scaled, _, err := e.scaleReplicaSetAndRecordEvent(ctx, oldRSs[0], newScale, deployment)
	return scaled, err
}

func (e *Executor) scaleDownOldReplicaSetsForRollingUpdate(ctx context.Context, allRSs []*apps.ReplicaSet, oldRSs []*apps.ReplicaSet, deployment *apps.Deployment) (int32, error) {
	maxUnavailable := e.maxUnavailable()

	// Check if we can scale down.
	minAvailable := *(deployment.Spec.Replicas) - maxUnavailable
	// Find the number of available pods.
	availablePodCount := deploymentutil.GetAvailableReplicaCountForReplicaSets(allRSs)
	if availablePodCount <= minAvailable {
		// Cannot scale down.
		return 0, nil
	}
	e.log.V(4).Info("Found available pods in deployment, scaling down old RSes",
		"availablePods", availablePodCount,
		"deployment", deployment.Name)

	// We expected scaled down the middle revision firstly.
	sort.Sort(deploymentutil.ReplicaSetsBySmallerRevision(oldRSs))

	totalScaledDown := int32(0)
	totalScaleDownCount := availablePodCount - minAvailable
	newRS := deploymentutil.FindNewReplicaSet(deployment, allRSs)
	// Old RSes should obey the limitation of partition.
	ScaleDownOldLimit := ScaleDownLimitForOld(oldRSs, newRS, deployment, e.Br.Spec.Strategy.Steps[e.Br.Status.CurrentStepIndex].Replicas)
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
		_, _, err := e.scaleReplicaSetAndRecordEvent(ctx, targetRS, newReplicasCount, deployment)
		if err != nil {
			return totalScaledDown, err
		}

		totalScaledDown += scaleDownCount
	}

	return totalScaledDown, nil
}

func (e *Executor) cleanupUnhealthyReplicas(ctx context.Context, oldRSs []*apps.ReplicaSet, deployment *apps.Deployment, maxCleanupCount int32) ([]*apps.ReplicaSet, int32, error) {
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
		e.log.V(4).Info("Found available pods in old RS",
			"namespace", targetRS.Namespace,
			"name", targetRS.Name,
			"availableReplicas", targetRS.Status.AvailableReplicas)
		if *(targetRS.Spec.Replicas) == targetRS.Status.AvailableReplicas {
			// no unhealthy replicas found, no scaling required.
			continue
		}

		scaledDownCount := int32(integer.IntMin(int(maxCleanupCount-totalScaledDown), int(*(targetRS.Spec.Replicas)-targetRS.Status.AvailableReplicas)))
		newReplicasCount := *(targetRS.Spec.Replicas) - scaledDownCount
		if newReplicasCount > *(targetRS.Spec.Replicas) {
			return nil, 0, fmt.Errorf("when cleaning up unhealthy replicas, got invalid request to scale down %s/%s %d -> %d", targetRS.Namespace, targetRS.Name, *(targetRS.Spec.Replicas), newReplicasCount)
		}
		_, updatedOldRS, err := e.scaleReplicaSetAndRecordEvent(ctx, targetRS, newReplicasCount, deployment)
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

	return scaleDownOldLimit
}

func (e *Executor) syncRolloutStatus(ctx context.Context, allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet) error {
	newStatus := e.calculateStatus(allRSs, newRS)

	// If there is no progressDeadlineSeconds set, remove any Progressing condition.
	if !deploymentutil.HasProgressDeadline(e.de) {
		deploymentutil.RemoveDeploymentCondition(&newStatus, apps.DeploymentProgressing)
	}

	// If there is only one replica set that is active then that means we are not running
	// a new rollout and this is a resync where we don't need to estimate any progress.
	// In such a case, we should simply not estimate any progress for this deployment.
	currentCond := deploymentutil.GetDeploymentCondition(e.de.Status, apps.DeploymentProgressing)
	isCompleteDeployment := newStatus.Replicas == newStatus.UpdatedReplicas && currentCond != nil && currentCond.Reason == deploymentutil.NewRSAvailableReason
	// Check for progress only if there is a progress deadline set and the latest rollout
	// hasn't completed yet.
	if deploymentutil.HasProgressDeadline(e.de) && !isCompleteDeployment {
		switch {
		case deploymentutil.DeploymentComplete(e.de, &newStatus):
			// Update the deployment conditions with a message for the new replica set that
			// was successfully deployed. If the condition already exists, we ignore this update.
			msg := fmt.Sprintf("Deployment %q has successfully progressed.", e.de.Name)
			if newRS != nil {
				msg = fmt.Sprintf("ReplicaSet %q has successfully progressed.", newRS.Name)
			}
			condition := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionTrue, deploymentutil.NewRSAvailableReason, msg)
			deploymentutil.SetDeploymentCondition(&newStatus, *condition)

		case deploymentutil.DeploymentProgressing(e.de, &newStatus):
			// If there is any progress made, continue by not checking if the deployment failed. This
			// behavior emulates the rolling updater progressDeadline check.
			msg := fmt.Sprintf("Deployment %q is progressing.", e.de.Name)
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

		case deploymentutil.DeploymentTimedOut(e.de, &newStatus):
			// Update the deployment with a timeout condition. If the condition already exists,
			// we ignore this update.
			msg := fmt.Sprintf("Deployment %q has timed out progressing.", e.de.Name)
			if newRS != nil {
				msg = fmt.Sprintf("ReplicaSet %q has timed out progressing.", newRS.Name)
			}
			condition := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionFalse, deploymentutil.TimedOutReason, msg)
			deploymentutil.SetDeploymentCondition(&newStatus, *condition)
		}
	}

	// Move failure conditions of all replica sets in deployment conditions. For now,
	// only one failure condition is returned from getReplicaFailures.
	if replicaFailureCond := e.getReplicaFailures(allRSs, newRS); len(replicaFailureCond) > 0 {
		// There will be only one ReplicaFailure condition on the replica set.
		deploymentutil.SetDeploymentCondition(&newStatus, replicaFailureCond[0])
	} else {
		deploymentutil.RemoveDeploymentCondition(&newStatus, apps.DeploymentReplicaFailure)
	}

	// Calculate extra status annotation
	// extraStatusAnno, err := e.updateDeploymentExtraStatus(ctx, newRS, d)
	// if err != nil {
	// 	return nil // no need to retry
	// }

	updatedReadyReplicas := int32(0)
	if newRS != nil {
		updatedReadyReplicas = newRS.Status.ReadyReplicas
	}
	e.Br.Status.UpdatedReadyReplicas = updatedReadyReplicas

	// Update both status and annotation
	err := e.patchDeploymentStatusAndAnnotation(ctx, e.de, newStatus)
	if err != nil {
		return err
	}

	// Requeue the deployment if required.
	e.requeueStuckDeployment(e.de, newStatus)
	return nil
}

func (e *Executor) requeueStuckDeployment(d *apps.Deployment, newStatus apps.DeploymentStatus) time.Duration {
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
		e.log.V(4).Info("Queueing up deployment for a progress check now",
			"deployment", d.Name)
		// e.enqueueRateLimited(d)  requeue
		return time.Duration(0)
	}
	e.log.V(4).Info("Queueing up deployment for a progress check",
		"deployment", d.Name,
		"afterSeconds", int(after.Seconds()))
	// Add a second to avoid milliseconds skew in AddAfter.
	// See https://github.com/kubernetes/kubernetes/issues/39785#issuecomment-279959133 for more info.
	// e.enqueueAfter(d, after+time.Second) requeue
	return after
}

var nowFn = func() time.Time { return time.Now() }

func (e *Executor) patchDeploymentStatusAndAnnotation(ctx context.Context, d *apps.Deployment, newStatus apps.DeploymentStatus) error {
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
	err := e.client.Patch(ctx, deploymentCopy, patch)
	if err != nil {
		log := ctrl.LoggerFrom(ctx)
		log.Error(err, "Failed to patch deployment status and annotation",
			"deployment", d.Name,
			"namespace", d.Namespace)
		return err
	}

	return nil
}

func (e *Executor) getReplicaFailures(allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet) []apps.DeploymentCondition {
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

func (e *Executor) calculateStatus(allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet) apps.DeploymentStatus {
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
		ObservedGeneration:  e.de.Generation,
		Replicas:            deploymentutil.GetActualReplicaCountForReplicaSets(allRSs),
		UpdatedReplicas:     deploymentutil.GetActualReplicaCountForReplicaSets([]*apps.ReplicaSet{newRS}),
		ReadyReplicas:       deploymentutil.GetReadyReplicaCountForReplicaSets(allRSs),
		AvailableReplicas:   availableReplicas,
		UnavailableReplicas: unavailableReplicas,
		CollisionCount:      e.de.Status.CollisionCount,
	}

	// Copy conditions one by one so we won't mutate the original object.
	conditions := e.de.Status.Conditions
	for i := range conditions {
		status.Conditions = append(status.Conditions, conditions[i])
	}

	if availableReplicas >= *(e.de.Spec.Replicas)-e.maxUnavailable() {
		minAvailability := deploymentutil.NewDeploymentCondition(apps.DeploymentAvailable, v1.ConditionTrue, deploymentutil.MinimumReplicasAvailable, "Deployment has minimum availability.")
		deploymentutil.SetDeploymentCondition(&status, *minAvailability)
	} else {
		noMinAvailability := deploymentutil.NewDeploymentCondition(apps.DeploymentAvailable, v1.ConditionFalse, deploymentutil.MinimumReplicasUnavailable, "Deployment does not have minimum availability.")
		deploymentutil.SetDeploymentCondition(&status, *noMinAvailability)
	}

	return status
}
func (e *Executor) reconcileNewReplicaSet(ctx context.Context, allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet) (bool, error) {
	if *(newRS.Spec.Replicas) == *(e.de.Spec.Replicas) {
		// Scaling not required.
		return false, nil
	}
	if *(newRS.Spec.Replicas) > *(e.de.Spec.Replicas) {
		// Scale down.
		scaled, _, err := e.scaleReplicaSetAndRecordEvent(ctx, newRS, *(e.de.Spec.Replicas), e.de)
		return scaled, err
	}
	newReplicasCount, err := e.newRSNewReplicas(e.de, allRSs, newRS, e.Br.Spec.Strategy.Steps[e.Br.Status.CurrentStepIndex])
	if err != nil {
		return false, err
	}
	scaled, _, err := e.scaleReplicaSetAndRecordEvent(ctx, newRS, newReplicasCount, e.de)
	return scaled, err
}

func (e *Executor) scaleReplicaSetAndRecordEvent(ctx context.Context, rs *apps.ReplicaSet, newScale int32, deployment *apps.Deployment) (bool, *apps.ReplicaSet, error) {
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
	scaled, newRS, err := e.scaleReplicaSet(ctx, rs, newScale, deployment, scalingOperation)
	return scaled, newRS, err
}

func (e *Executor) scaleReplicaSet(ctx context.Context, rs *apps.ReplicaSet, newScale int32, deployment *apps.Deployment, scalingOperation string) (bool, *apps.ReplicaSet, error) {

	sizeNeedsUpdate := *(rs.Spec.Replicas) != newScale

	annotationsNeedUpdate := deploymentutil.ReplicasAnnotationsNeedUpdate(rs, *(deployment.Spec.Replicas), *(deployment.Spec.Replicas)+e.maxSurge())

	scaled := false
	var err error
	if sizeNeedsUpdate || annotationsNeedUpdate {
		oldScale := *(rs.Spec.Replicas)

		// Use existing state directly for patching, let API Server handle conflicts
		rsCopy := rs.DeepCopy()
		*(rsCopy.Spec.Replicas) = newScale
		deploymentutil.SetReplicasAnnotations(rsCopy, *(deployment.Spec.Replicas), *(deployment.Spec.Replicas)+e.maxSurge())

		// Use MergeFrom with optimistic lock for patching, if ResourceVersion conflicts, API Server will return 409 error
		// Controller-runtime will automatically reschedule for reconciliation
		patch := client.MergeFromWithOptions(rs, client.MergeFromWithOptimisticLock{})
		err = e.client.Patch(ctx, rsCopy, patch)
		if err != nil {
			return scaled, rs, err
		}

		rs = rsCopy
		if sizeNeedsUpdate {
			scaled = true
			e.eventRecorder.Eventf(deployment, v1.EventTypeNormal, "ScalingReplicaSet", "Scaled %s replica set %s to %d from %d", scalingOperation, rs.Name, newScale, oldScale)
		}
	}
	return scaled, rs, err
}

func (e *Executor) getAllReplicaSetsAndSyncRevision(ctx context.Context, rsList []*apps.ReplicaSet, createIfNotExisted bool) (*apps.ReplicaSet, []*apps.ReplicaSet, error) {
	_, allOldRSs := deploymentutil.FindOldReplicaSets(e.de, rsList)

	// Get new replica set with the updated revision number
	newRS, err := e.getNewReplicaSet(ctx, rsList, allOldRSs, createIfNotExisted)
	if err != nil {
		return nil, nil, err
	}

	return newRS, allOldRSs, nil
}

func (e *Executor) getNewReplicaSet(ctx context.Context, rsList, oldRSs []*apps.ReplicaSet, createIfNotExisted bool) (*apps.ReplicaSet, error) {
	existingNewRS := deploymentutil.FindNewReplicaSet(e.de, rsList)

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
		annotationsUpdated := e.setNewReplicaSetAnnotations(rsCopy, newRevision, true, maxRevHistoryLengthInChars)
		minReadySecondsNeedsUpdate := rsCopy.Spec.MinReadySeconds != e.de.Spec.MinReadySeconds
		if annotationsUpdated || minReadySecondsNeedsUpdate {
			// Update the copy with the new minReadySeconds
			if minReadySecondsNeedsUpdate {
				rsCopy.Spec.MinReadySeconds = e.de.Spec.MinReadySeconds
			}

			// Use MergeFrom with optimistic lock for patching, if ResourceVersion conflicts, API Server will return 409 error
			// Controller-runtime will automatically reschedule for reconciliation
			patch := client.MergeFromWithOptions(existingNewRS, client.MergeFromWithOptimisticLock{})
			err := e.client.Patch(ctx, rsCopy, patch)
			if err != nil {
				return nil, err
			}
			return rsCopy, nil
		}

		// Should use the revision in existingNewRS's annotation, since it set by before
		needsUpdate := deploymentutil.SetDeploymentRevision(e.de, rsCopy.Annotations[deploymentutil.RevisionAnnotation])
		// If no other Progressing condition has been recorded and we need to estimate the progress
		// of this deployment then it is likely that old users started caring about progress. In that
		// case we need to take into account the first time we noticed their new replica set.
		cond := deploymentutil.GetDeploymentCondition(e.de.Status, apps.DeploymentProgressing)
		if deploymentutil.HasProgressDeadline(e.de) && cond == nil {
			msg := fmt.Sprintf("Found new replica set %q", rsCopy.Name)
			condition := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionTrue, deploymentutil.FoundNewRSReason, msg)
			deploymentutil.SetDeploymentCondition(&e.de.Status, *condition)
			needsUpdate = true
		}

		if needsUpdate {
			var err error
			// todo 更新方式统一
			if err = e.client.Status().Update(ctx, e.de); err != nil {
				return nil, err
			}
		}
		return rsCopy, nil
	}

	if !createIfNotExisted {
		return nil, nil
	}

	// new ReplicaSet does not exist, create one.
	newRSTemplate := *e.de.Spec.Template.DeepCopy()
	podTemplateSpecHash := deploymentutil.ComputeHash(&newRSTemplate, e.de.Status.CollisionCount)
	newRSTemplate.Labels = labelsutil.CloneAndAddLabel(e.de.Spec.Template.Labels, apps.DefaultDeploymentUniqueLabelKey, podTemplateSpecHash)
	// Add podTemplateHash label to selector.
	newRSSelector := labelsutil.CloneSelectorAndAddLabel(e.de.Spec.Selector, apps.DefaultDeploymentUniqueLabelKey, podTemplateSpecHash)

	// Create new ReplicaSet
	newRS := apps.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			// Make the name deterministic, to ensure idempotence
			Name:            e.de.Name + "-" + podTemplateSpecHash,
			Namespace:       e.de.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(e.de, controllerKind)},
			Labels:          newRSTemplate.Labels,
		},
		Spec: apps.ReplicaSetSpec{
			Replicas:        new(int32),
			MinReadySeconds: e.de.Spec.MinReadySeconds,
			Selector:        newRSSelector,
			Template:        newRSTemplate,
		},
	}
	allRSs := append(oldRSs, &newRS)
	newReplicasCount, err := e.newRSNewReplicas(e.de, allRSs, &newRS, e.Br.Spec.Strategy.Steps[e.Br.Status.CurrentStepIndex])
	if err != nil {
		return nil, err
	}

	// We ensure that newReplicasLowerBound is greater than 0 unless deployment is 0,
	// this is because if we set new replicas as 0, the native deployment controller
	// will flight with ours.
	newReplicasLowerBound := e.newRSReplicasLowerBound(e.de)

	*(newRS.Spec.Replicas) = integer.Int32Max(newReplicasCount, newReplicasLowerBound)
	// Set new replica set's annotation
	e.setNewReplicaSetAnnotations(&newRS, newRevision, false, maxRevHistoryLengthInChars)
	// Create the new ReplicaSet. If it already exists, then we need to check for possible
	// hash collisions. If there is any other error, we need to report it in the status of
	// the Deployment.
	alreadyExists := false
	var createdRS *apps.ReplicaSet
	err = e.client.Create(ctx, &newRS)
	if err == nil {
		createdRS = &newRS
	}
	switch {
	// We may end up hitting this due to a slow cache or a fast resync of the Deployment.
	case errors.IsAlreadyExists(err):
		alreadyExists = true

		// Fetch a copy of the ReplicaSet.
		rs := &apps.ReplicaSet{}
		rsErr := e.client.Get(ctx, client.ObjectKey{Namespace: newRS.Namespace, Name: newRS.Name}, rs)
		if rsErr != nil {
			return nil, rsErr
		}

		// If the Deployment owns the ReplicaSet and the ReplicaSet's PodTemplateSpec is semantically
		// deep equal to the PodTemplateSpec of the Deployment, it's the Deployment's new ReplicaSet.
		// Otherwise, this is a hash collision and we need to increment the collisionCount field in
		// the status of the Deployment and requeue to try the creation in the next sync.
		controllerRef := metav1.GetControllerOf(rs)
		if controllerRef != nil && controllerRef.UID == e.de.UID && deploymentutil.EqualIgnoreHash(&e.de.Spec.Template, &rs.Spec.Template) {
			createdRS = rs
			err = nil
			break
		}

		// Matching ReplicaSet is not equal - increment the collisionCount in the DeploymentStatus
		// and requeue the Deployment.
		if e.de.Status.CollisionCount == nil {
			e.de.Status.CollisionCount = new(int32)
		}
		preCollisionCount := *e.de.Status.CollisionCount
		*e.de.Status.CollisionCount++
		// Update the collisionCount for the Deployment and let it requeue by returning the original
		// error.
		dErr := e.client.Status().Update(ctx, e.de)
		if dErr == nil {
			e.log.V(2).Info("Found a hash collision for deployment, bumping collisionCount to resolve it",
				"deployment", e.de.Name,
				"oldCount", preCollisionCount,
				"newCount", *e.de.Status.CollisionCount)
		} else {
			log := ctrl.LoggerFrom(ctx)
			log.Error(dErr, "Failed to update deployment collision count",
				"deployment", e.de.Name)
		}
		return nil, err
	case errors.HasStatusCause(err, v1.NamespaceTerminatingCause):
		// if the namespace is terminating, all subsequent creates will fail and we can safely do nothing
		return nil, err
	case err != nil:
		msg := fmt.Sprintf("Failed to create new replica set %q: %v", newRS.Name, err)
		if deploymentutil.HasProgressDeadline(e.de) {
			cond := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionFalse, deploymentutil.FailedRSCreateReason, msg)
			deploymentutil.SetDeploymentCondition(&e.de.Status, *cond)
			// We don't really care about this error at this point, since we have a bigger issue to report.
			// TODO: Identify which errors are permanent and switch DeploymentIsFailed to take into account
			// these reasons as well. Related issue: https://github.com/kubernetes/kubernetes/issues/18568
			if updateErr := e.client.Status().Update(ctx, e.de); updateErr != nil {
				log := ctrl.LoggerFrom(ctx)
				log.Error(updateErr, "Failed to update deployment status after RS creation failure",
					"deployment", e.de.Name)
			}
		}
		e.eventRecorder.Eventf(e.de, v1.EventTypeWarning, deploymentutil.FailedRSCreateReason, msg)
		return nil, err
	}
	if !alreadyExists && newReplicasCount > 0 {
		e.eventRecorder.Eventf(e.de, v1.EventTypeNormal, "ScalingReplicaSet", "Scaled up replica set %s to %d", createdRS.Name, newReplicasCount)
	}

	needsUpdate := deploymentutil.SetDeploymentRevision(e.de, newRevision)
	if !alreadyExists && deploymentutil.HasProgressDeadline(e.de) {
		msg := fmt.Sprintf("Created new replica set %q", createdRS.Name)
		condition := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionTrue, deploymentutil.NewReplicaSetReason, msg)
		deploymentutil.SetDeploymentCondition(&e.de.Status, *condition)
		needsUpdate = true
	}
	if needsUpdate {
		if updateErr := e.client.Status().Update(ctx, e.de); updateErr != nil {
			log := ctrl.LoggerFrom(ctx)
			log.Error(updateErr, "Failed to update deployment status",
				"deployment", e.de.Name)
			err = updateErr
		}
	}
	return createdRS, err
}

func (e *Executor) getReplicaSetsForDeployment(ctx context.Context, d *apps.Deployment) ([]*apps.ReplicaSet, error) {
	deploymentSelector, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("deployment %s/%s has invalid label selector: %v", d.Namespace, d.Name, err)
	}

	// List all ReplicaSets using runtimeClient
	rsList := &apps.ReplicaSetList{}
	err = e.client.List(ctx, rsList, client.InNamespace(d.Namespace), client.MatchingLabelsSelector{Selector: deploymentSelector})
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

func (e *Executor) maxSurge() int32 {
	maxSurge, _, _ := deploymentutil.ResolveFenceposts(e.Br.Status.MaxSurge, e.Br.Status.MaxUnavailable, *(e.de.Spec.Replicas))
	return maxSurge
}

func (e *Executor) setNewReplicaSetAnnotations(newRS *apps.ReplicaSet, newRevision string, exists bool, revHistoryLimitInChars int) bool {
	// First, copy deployment's annotations (except for apply and revision annotations)
	annotationChanged := deploymentutil.CopyDeploymentAnnotationsToReplicaSet(e.de, newRS)
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
			e.log.V(1).Info("Updating replica set revision OldRevision not int",
				"replicaSet", newRS.Name,
				"error", err.Error())
			return false
		}
		// If the RS annotation is empty then initialize it to 0
		oldRevisionInt = 0
	}
	newRevisionInt, err := strconv.ParseInt(newRevision, 10, 64)
	if err != nil {
		e.log.V(1).Info("Updating replica set revision NewRevision not int",
			"replicaSet", newRS.Name,
			"error", err.Error())
		return false
	}
	if oldRevisionInt < newRevisionInt {
		newRS.Annotations[deploymentutil.RevisionAnnotation] = newRevision
		annotationChanged = true
		e.log.V(4).Info("Updating replica set revision",
			"replicaSet", newRS.Name,
			"newRevision", newRevision)
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
				e.log.V(1).Info("Not appending revision due to length limit reached",
					"replicaSet", newRS.Name,
					"limitChars", revHistoryLimitInChars)
			}
		}
	}
	// If the new replica set is about to be created, we need to add replica annotations to it.
	if !exists && deploymentutil.SetReplicasAnnotations(newRS, *(e.de.Spec.Replicas), *(e.de.Spec.Replicas)+e.maxSurge()) {
		annotationChanged = true
	}
	return annotationChanged
}

func (e *Executor) newRSNewReplicas(deployment *apps.Deployment, allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet, currentStep v1alpha1.Step) (int32, error) {
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
		maxTotalPods := *(deployment.Spec.Replicas) + e.maxSurge()
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

func (e *Executor) newRSReplicasLowerBound(deployment *apps.Deployment) int32 {
	if e.maxSurge() > 0 {
		return int32(0)
	}
	return integer.Int32Min(int32(1), *deployment.Spec.Replicas)
}

func (e *Executor) recordStableVersion(ctx context.Context, podTemplate *v1.PodTemplateSpec) error {
	csv, csvr := e.Br.Annotations[v1alpha1.CurrentStableReversionKey], e.Br.Annotations[v1alpha1.CurrentStableReversionRawKey]

	podTemplateHash := deploymentutil.ComputeHash(podTemplate, nil)
	if csv == podTemplateHash {
		return nil
	}

	templateYaml, err := yaml.Marshal(podTemplate)
	if err != nil {
		e.log.Error(err, "Failed to serialize template to yaml")
		return nil
	}
	err = UpdateObj(ctx, e.client, e.Br.DeepCopy(), func(object client.Object) {
		br := object.(*v1alpha1.BatchRelease)
		if br.Annotations == nil {
			br.Annotations = make(map[string]string)
		}
		br.Annotations[v1alpha1.CurrentStableReversionKey] = podTemplateHash
		br.Annotations[v1alpha1.CurrentStableReversionRawKey] = string(templateYaml)

		if len(csv) == 0 {
			// 初次部署
			br.Annotations[v1alpha1.LastStableReversionKey] = podTemplateHash
			br.Annotations[v1alpha1.LastStableReversionRawKey] = string(templateYaml)
		} else {
			br.Annotations[v1alpha1.LastStableReversionKey] = csv
			br.Annotations[v1alpha1.LastStableReversionRawKey] = csvr
		}
	})
	if err != nil {
		return err
	}

	e.log.V(4).Info("Record current stable reversion", "currentStableReversion", podTemplateHash)
	if len(csv) != 0 {
		e.log.V(4).Info("Record last stable reversion", "lastStableReversion", csv)
	}
	return nil
}

func (e *Executor) DesiredPartition() intstrutil.IntOrString {
	return e.Br.Spec.Strategy.Steps[e.Br.Status.CurrentStepIndex].Replicas
}
