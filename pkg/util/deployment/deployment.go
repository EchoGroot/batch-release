package deploymentutil

import (
	"encoding/binary"
	"fmt"
	"github.com/EchoGroot/batch-release/api/v1alpha1"
	"github.com/davecgh/go-spew/spew"
	"hash"
	"hash/fnv"
	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	intstrutil "k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/klog/v2"
	"k8s.io/utils/integer"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	RevisionAnnotation        = "deployment.kubernetes.io/revision"
	RevisionHistoryAnnotation = "deployment.kubernetes.io/revision-history"

	ReplicasAnnotation    = "deployment.kubernetes.io/desired-replicas"
	MaxReplicasAnnotation = "deployment.kubernetes.io/max-replicas"
	FoundNewRSReason      = "FoundNewReplicaSet"
	alphanums             = "bcdfghjklmnpqrstvwxz2456789"

	FailedRSCreateReason       = "ReplicaSetCreateError"
	NewReplicaSetReason        = "NewReplicaSetCreated"
	MinimumReplicasAvailable   = "MinimumReplicasAvailable"
	MinimumReplicasUnavailable = "MinimumReplicasUnavailable"
	NewRSAvailableReason       = "NewReplicaSetAvailable"

	ReplicaSetUpdatedReason = "ReplicaSetUpdated"
	TimedOutReason          = "ProgressDeadlineExceeded"
)

func FindOldReplicaSets(deployment *apps.Deployment, rsList []*apps.ReplicaSet) ([]*apps.ReplicaSet, []*apps.ReplicaSet) {
	var requiredRSs []*apps.ReplicaSet
	var allRSs []*apps.ReplicaSet
	newRS := FindNewReplicaSet(deployment, rsList)
	for _, rs := range rsList {
		// Filter out new replica set
		if newRS != nil && rs.UID == newRS.UID {
			continue
		}
		allRSs = append(allRSs, rs)
		if *(rs.Spec.Replicas) != 0 {
			requiredRSs = append(requiredRSs, rs)
		}
	}
	return requiredRSs, allRSs
}

func FindNewReplicaSet(deployment *apps.Deployment, rsList []*apps.ReplicaSet) *apps.ReplicaSet {
	sort.Sort(ReplicaSetsByCreationTimestamp(rsList))
	for i := range rsList {
		if EqualIgnoreHash(&rsList[i].Spec.Template, &deployment.Spec.Template) {
			// In rare cases, such as after cluster upgrades, Deployment may end up with
			// having more than one new ReplicaSets that have the same template as its template,
			// see https://github.com/kubernetes/kubernetes/issues/40415
			// We deterministically choose the oldest new ReplicaSet.
			return rsList[i]
		}
	}
	// new ReplicaSet does not exist.
	return nil
}

// ReplicaSetsByCreationTimestamp sorts a list of ReplicaSet by creation timestamp, using their names as a tie breaker.
type ReplicaSetsByCreationTimestamp []*apps.ReplicaSet

func (o ReplicaSetsByCreationTimestamp) Len() int      { return len(o) }
func (o ReplicaSetsByCreationTimestamp) Swap(i, j int) { o[i], o[j] = o[j], o[i] }
func (o ReplicaSetsByCreationTimestamp) Less(i, j int) bool {
	if o[i].CreationTimestamp.Equal(&o[j].CreationTimestamp) {
		return o[i].Name < o[j].Name
	}
	return o[i].CreationTimestamp.Before(&o[j].CreationTimestamp)
}

func EqualIgnoreHash(template1, template2 *v1.PodTemplateSpec) bool {
	t1Copy := template1.DeepCopy()
	t2Copy := template2.DeepCopy()
	// Remove hash labels from template.Labels before comparing
	delete(t1Copy.Labels, apps.DefaultDeploymentUniqueLabelKey)
	delete(t2Copy.Labels, apps.DefaultDeploymentUniqueLabelKey)
	return equality.Semantic.DeepEqual(t1Copy, t2Copy)
}

func MaxRevision(allRSs []*apps.ReplicaSet) int64 {
	max := int64(0)
	for _, rs := range allRSs {
		if v, err := Revision(rs); err != nil {
			// Skip the replica sets when it failed to parse their revision information
			klog.V(4).Infof("Error: %v. Couldn't parse revision for replica set %#v, deployment controller will skip it when reconciling revisions.", err, rs)
		} else if v > max {
			max = v
		}
	}
	return max
}

// Revision returns the revision number of the input object.
func Revision(obj runtime.Object) (int64, error) {
	acc, err := meta.Accessor(obj)
	if err != nil {
		return 0, err
	}
	v, ok := acc.GetAnnotations()[RevisionAnnotation]
	if !ok {
		return 0, nil
	}
	return strconv.ParseInt(v, 10, 64)
}

func SetNewReplicaSetAnnotations(deployment *apps.Deployment, newRS *apps.ReplicaSet, newRevision string, exists bool, revHistoryLimitInChars int) bool {
	// First, copy deployment's annotations (except for apply and revision annotations)
	annotationChanged := copyDeploymentAnnotationsToReplicaSet(deployment, newRS)
	// Then, update replica set's revision annotation
	if newRS.Annotations == nil {
		newRS.Annotations = make(map[string]string)
	}
	oldRevision, ok := newRS.Annotations[RevisionAnnotation]
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
		newRS.Annotations[RevisionAnnotation] = newRevision
		annotationChanged = true
		klog.V(4).Infof("Updating replica set %q revision to %s", newRS.Name, newRevision)
	}
	// If a revision annotation already existed and this replica set was updated with a new revision
	// then that means we are rolling back to this replica set. We need to preserve the old revisions
	// for historical information.
	if ok && oldRevisionInt < newRevisionInt {
		revisionHistoryAnnotation := newRS.Annotations[RevisionHistoryAnnotation]
		oldRevisions := strings.Split(revisionHistoryAnnotation, ",")
		if len(oldRevisions[0]) == 0 {
			newRS.Annotations[RevisionHistoryAnnotation] = oldRevision
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
				newRS.Annotations[RevisionHistoryAnnotation] = strings.Join(oldRevisions, ",")
			} else {
				klog.Warningf("Not appending revision due to length limit of %v reached", revHistoryLimitInChars)
			}
		}
	}
	// If the new replica set is about to be created, we need to add replica annotations to it.
	if !exists && SetReplicasAnnotations(newRS, *(deployment.Spec.Replicas), *(deployment.Spec.Replicas)+MaxSurge(deployment)) {
		annotationChanged = true
	}
	return annotationChanged
}

func MaxSurge(deployment *apps.Deployment) int32 {
	if deployment.Spec.Strategy.RollingUpdate == nil {
		return int32(0)
	}
	// Error caught by validation
	maxSurge, _, _ := ResolveFenceposts(deployment.Spec.Strategy.RollingUpdate.MaxSurge, deployment.Spec.Strategy.RollingUpdate.MaxUnavailable, *(deployment.Spec.Replicas))
	return maxSurge
}

func ResolveFenceposts(maxSurge, maxUnavailable *intstrutil.IntOrString, desired int32) (int32, int32, error) {
	surge, err := intstrutil.GetScaledValueFromIntOrPercent(intstrutil.ValueOrDefault(maxSurge, intstrutil.FromInt(0)), int(desired), true)
	if err != nil {
		return 0, 0, err
	}
	unavailable, err := intstrutil.GetScaledValueFromIntOrPercent(intstrutil.ValueOrDefault(maxUnavailable, intstrutil.FromInt(0)), int(desired), false)
	if err != nil {
		return 0, 0, err
	}

	if surge == 0 && unavailable == 0 {
		// Validation should never allow the user to explicitly use zero values for both maxSurge
		// maxUnavailable. Due to rounding down maxUnavailable though, it may resolve to zero.
		// If both fenceposts resolve to zero, then we should set maxUnavailable to 1 on the
		// theory that surge might not work due to quota.
		unavailable = 1
	}

	return int32(surge), int32(unavailable), nil
}
func SetReplicasAnnotations(rs *apps.ReplicaSet, desiredReplicas, maxReplicas int32) bool {
	updated := false
	if rs.Annotations == nil {
		rs.Annotations = make(map[string]string)
	}
	desiredString := fmt.Sprintf("%d", desiredReplicas)
	if hasString := rs.Annotations[ReplicasAnnotation]; hasString != desiredString {
		rs.Annotations[ReplicasAnnotation] = desiredString
		updated = true
	}
	maxString := fmt.Sprintf("%d", maxReplicas)
	if hasString := rs.Annotations[MaxReplicasAnnotation]; hasString != maxString {
		rs.Annotations[MaxReplicasAnnotation] = maxString
		updated = true
	}
	return updated
}
func copyDeploymentAnnotationsToReplicaSet(deployment *apps.Deployment, rs *apps.ReplicaSet) bool {
	rsAnnotationsChanged := false
	if rs.Annotations == nil {
		rs.Annotations = make(map[string]string)
	}
	for k, v := range deployment.Annotations {
		// newRS revision is updated automatically in getNewReplicaSet, and the deployment's revision number is then updated
		// by copying its newRS revision number. We should not copy deployment's revision to its newRS, since the update of
		// deployment revision number may fail (revision becomes stale) and the revision number in newRS is more reliable.
		if _, exist := rs.Annotations[k]; skipCopyAnnotation(k) || (exist && rs.Annotations[k] == v) {
			continue
		}
		rs.Annotations[k] = v
		rsAnnotationsChanged = true
	}
	return rsAnnotationsChanged
}

func skipCopyAnnotation(key string) bool {
	return annotationsToSkip[key]
}

var annotationsToSkip = map[string]bool{
	v1.LastAppliedConfigAnnotation: true,
	RevisionAnnotation:             true,
	RevisionHistoryAnnotation:      true,
	ReplicasAnnotation:             true,
	MaxReplicasAnnotation:          true,
	apps.DeprecatedRollbackTo:      true,
}

func SetDeploymentRevision(deployment *apps.Deployment, revision string) bool {
	updated := false

	if deployment.Annotations == nil {
		deployment.Annotations = make(map[string]string)
	}
	if deployment.Annotations[RevisionAnnotation] != revision {
		deployment.Annotations[RevisionAnnotation] = revision
		updated = true
	}

	return updated
}

func GetDeploymentCondition(status apps.DeploymentStatus, condType apps.DeploymentConditionType) *apps.DeploymentCondition {
	for i := range status.Conditions {
		c := status.Conditions[i]
		if c.Type == condType {
			return &c
		}
	}
	return nil
}

func HasProgressDeadline(d *apps.Deployment) bool {
	return d.Spec.ProgressDeadlineSeconds != nil && *d.Spec.ProgressDeadlineSeconds != math.MaxInt32
}
func NewDeploymentCondition(condType apps.DeploymentConditionType, status v1.ConditionStatus, reason, message string) *apps.DeploymentCondition {
	return &apps.DeploymentCondition{
		Type:               condType,
		Status:             status,
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
}
func SetDeploymentCondition(status *apps.DeploymentStatus, condition apps.DeploymentCondition) {
	currentCond := GetDeploymentCondition(*status, condition.Type)
	if currentCond != nil && currentCond.Status == condition.Status && currentCond.Reason == condition.Reason {
		return
	}
	// Do not update lastTransitionTime if the status of the condition doesn't change.
	if currentCond != nil && currentCond.Status == condition.Status {
		condition.LastTransitionTime = currentCond.LastTransitionTime
	}
	newConditions := filterOutCondition(status.Conditions, condition.Type)
	status.Conditions = append(newConditions, condition)
}

func filterOutCondition(conditions []apps.DeploymentCondition, condType apps.DeploymentConditionType) []apps.DeploymentCondition {
	var newConditions []apps.DeploymentCondition
	for _, c := range conditions {
		if c.Type == condType {
			continue
		}
		newConditions = append(newConditions, c)
	}
	return newConditions
}

func ComputeHash(template *v1.PodTemplateSpec, collisionCount *int32) string {
	podTemplateSpecHasher := fnv.New32a()
	DeepHashObject(podTemplateSpecHasher, *template)

	// Add collisionCount in the hash if it exists.
	if collisionCount != nil {
		collisionCountBytes := make([]byte, 8)
		binary.LittleEndian.PutUint32(collisionCountBytes, uint32(*collisionCount))
		podTemplateSpecHasher.Write(collisionCountBytes)
	}

	return SafeEncodeString(fmt.Sprint(podTemplateSpecHasher.Sum32()))
}

func DeepHashObject(hasher hash.Hash, objectToWrite interface{}) {
	hasher.Reset()
	printer := spew.ConfigState{
		Indent:         " ",
		SortKeys:       true,
		DisableMethods: true,
		SpewKeys:       true,
	}
	printer.Fprintf(hasher, "%#v", objectToWrite)
}

func SafeEncodeString(s string) string {
	r := make([]byte, len(s))
	for i, b := range []rune(s) {
		r[i] = alphanums[int(b)%len(alphanums)]
	}
	return string(r)
}

func NewRSNewReplicas(deployment *apps.Deployment, allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet, currentStep v1alpha1.Step) (int32, error) {
	// Find the total number of pods
	currentPodCount := GetReplicaCountForReplicaSets(allRSs)
	switch {
	case currentPodCount > *newRS.Spec.Replicas:
		// Do not scale down due to partition settings.
		scaleUpLimit := NewRSReplicasLimit(currentStep.Replicas, deployment)
		if *newRS.Spec.Replicas >= scaleUpLimit {
			// Cannot scale up.
			return *(newRS.Spec.Replicas), nil
		}
		// Do not scale up due to exceeded current replicas.
		maxTotalPods := *(deployment.Spec.Replicas) + MaxSurge(deployment)
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

func GetReplicaCountForReplicaSets(replicaSets []*apps.ReplicaSet) int32 {
	totalReplicas := int32(0)
	for _, rs := range replicaSets {
		if rs != nil {
			totalReplicas += *(rs.Spec.Replicas)
		}
	}
	return totalReplicas
}

func NewRSReplicasLimit(partition intstrutil.IntOrString, deployment *apps.Deployment) int32 {
	replicas := int(*deployment.Spec.Replicas)
	replicaLimit, _ := intstrutil.GetScaledValueFromIntOrPercent(&partition, replicas, true)
	replicaLimit = integer.IntMax(integer.IntMin(replicaLimit, replicas), 0)
	if replicas > 1 && partition.Type == intstrutil.String && partition.String() != "100%" {
		replicaLimit = integer.IntMin(replicaLimit, replicas-1)
	}
	return int32(replicaLimit)
}

func NewRSReplicasLowerBound(deployment *apps.Deployment) int32 {
	if MaxSurge(deployment) > 0 {
		return int32(0)
	}
	return integer.Int32Min(int32(1), *deployment.Spec.Replicas)
}

func ReplicasAnnotationsNeedUpdate(rs *apps.ReplicaSet, desiredReplicas, maxReplicas int32) bool {
	if rs.Annotations == nil {
		return true
	}
	desiredString := fmt.Sprintf("%d", desiredReplicas)
	if hasString := rs.Annotations[ReplicasAnnotation]; hasString != desiredString {
		return true
	}
	maxString := fmt.Sprintf("%d", maxReplicas)
	if hasString := rs.Annotations[MaxReplicasAnnotation]; hasString != maxString {
		return true
	}
	return false
}

func MaxUnavailable(deployment *apps.Deployment) int32 {
	if deployment.Spec.Strategy.RollingUpdate == nil || *(deployment.Spec.Replicas) == 0 {
		return int32(0)
	}
	// Error caught by validation
	_, maxUnavailable, _ := ResolveFenceposts(deployment.Spec.Strategy.RollingUpdate.MaxSurge, deployment.Spec.Strategy.RollingUpdate.MaxUnavailable, *(deployment.Spec.Replicas))
	if maxUnavailable > *deployment.Spec.Replicas {
		return *deployment.Spec.Replicas
	}
	return maxUnavailable
}

func GetAvailableReplicaCountForReplicaSets(replicaSets []*apps.ReplicaSet) int32 {
	totalAvailableReplicas := int32(0)
	for _, rs := range replicaSets {
		if rs != nil {
			totalAvailableReplicas += rs.Status.AvailableReplicas
		}
	}
	return totalAvailableReplicas
}

func GetActualReplicaCountForReplicaSets(replicaSets []*apps.ReplicaSet) int32 {
	totalActualReplicas := int32(0)
	for _, rs := range replicaSets {
		if rs != nil {
			totalActualReplicas += rs.Status.Replicas
		}
	}
	return totalActualReplicas
}

func GetReadyReplicaCountForReplicaSets(replicaSets []*apps.ReplicaSet) int32 {
	totalReadyReplicas := int32(0)
	for _, rs := range replicaSets {
		if rs != nil {
			totalReadyReplicas += rs.Status.ReadyReplicas
		}
	}
	return totalReadyReplicas
}

func RemoveDeploymentCondition(status *apps.DeploymentStatus, condType apps.DeploymentConditionType) {
	status.Conditions = filterOutCondition(status.Conditions, condType)
}

func DeploymentComplete(deployment *apps.Deployment, newStatus *apps.DeploymentStatus) bool {
	return newStatus.UpdatedReplicas == *(deployment.Spec.Replicas) &&
		newStatus.Replicas == *(deployment.Spec.Replicas) &&
		newStatus.AvailableReplicas == *(deployment.Spec.Replicas) &&
		newStatus.ObservedGeneration >= deployment.Generation
}

func DeploymentProgressing(deployment *apps.Deployment, newStatus *apps.DeploymentStatus) bool {
	oldStatus := deployment.Status

	// Old replicas that need to be scaled down
	oldStatusOldReplicas := oldStatus.Replicas - oldStatus.UpdatedReplicas
	newStatusOldReplicas := newStatus.Replicas - newStatus.UpdatedReplicas

	return (newStatus.UpdatedReplicas > oldStatus.UpdatedReplicas) ||
		(newStatusOldReplicas < oldStatusOldReplicas) ||
		newStatus.ReadyReplicas > deployment.Status.ReadyReplicas ||
		newStatus.AvailableReplicas > deployment.Status.AvailableReplicas
}

func DeploymentTimedOut(deployment *apps.Deployment, newStatus *apps.DeploymentStatus) bool {
	if !HasProgressDeadline(deployment) {
		return false
	}

	// Look for the Progressing condition. If it doesn't exist, we have no base to estimate progress.
	// If it's already set with a TimedOutReason reason, we have already timed out, no need to check
	// again.
	condition := GetDeploymentCondition(*newStatus, apps.DeploymentProgressing)
	if condition == nil {
		return false
	}
	// If the previous condition has been a successful rollout then we shouldn't try to
	// estimate any progress. Scenario:
	//
	// * progressDeadlineSeconds is smaller than the difference between now and the time
	//   the last rollout finished in the past.
	// * the creation of a new ReplicaSet triggers a resync of the Deployment prior to the
	//   cached copy of the Deployment getting updated with the status.condition that indicates
	//   the creation of the new ReplicaSet.
	//
	// The Deployment will be resynced and eventually its Progressing condition will catch
	// up with the state of the world.
	if condition.Reason == NewRSAvailableReason {
		return false
	}
	if condition.Reason == TimedOutReason {
		return true
	}

	// Look at the difference in seconds between now and the last time we reported any
	// progress or tried to create a replica set, or resumed a paused deployment and
	// compare against progressDeadlineSeconds.
	from := condition.LastUpdateTime
	now := nowFn()
	delta := time.Duration(*deployment.Spec.ProgressDeadlineSeconds) * time.Second
	timedOut := from.Add(delta).Before(now)

	klog.V(4).Infof("Deployment %q timed out (%t) [last progress check: %v - now: %v]", deployment.Name, timedOut, from, now)
	return timedOut
}

var nowFn = func() time.Time { return time.Now() }

func ReplicaSetToDeploymentCondition(cond apps.ReplicaSetCondition) apps.DeploymentCondition {
	return apps.DeploymentCondition{
		Type:               apps.DeploymentConditionType(cond.Type),
		Status:             cond.Status,
		LastTransitionTime: cond.LastTransitionTime,
		LastUpdateTime:     cond.LastTransitionTime,
		Reason:             cond.Reason,
		Message:            cond.Message,
	}
}

// ReplicaSetsBySmallerRevision sorts a list of ReplicaSet by revision in desc, using their creation timestamp or name as a tie breaker.
// By using the creation timestamp, this sorts from old to new replica sets.
type ReplicaSetsBySmallerRevision []*apps.ReplicaSet

func (o ReplicaSetsBySmallerRevision) Len() int      { return len(o) }
func (o ReplicaSetsBySmallerRevision) Swap(i, j int) { o[i], o[j] = o[j], o[i] }
func (o ReplicaSetsBySmallerRevision) Less(i, j int) bool {
	revision1, err1 := Revision(o[i])
	revision2, err2 := Revision(o[j])
	if err1 != nil || err2 != nil || revision1 == revision2 {
		return ReplicaSetsByCreationTimestamp(o).Less(i, j)
	}
	return revision1 > revision2
}

// FilterActiveReplicaSets returns replica sets that have (or at least ought to have) pods.
func FilterActiveReplicaSets(replicaSets []*apps.ReplicaSet) []*apps.ReplicaSet {
	activeFilter := func(rs *apps.ReplicaSet) bool {
		return rs != nil && *(rs.Spec.Replicas) > 0
	}
	return FilterReplicaSets(replicaSets, activeFilter)
}

type filterRS func(rs *apps.ReplicaSet) bool

// FilterReplicaSets returns replica sets that are filtered by filterFn (all returned ones should match filterFn).
func FilterReplicaSets(RSes []*apps.ReplicaSet, filterFn filterRS) []*apps.ReplicaSet {
	var filtered []*apps.ReplicaSet
	for i := range RSes {
		if filterFn(RSes[i]) {
			filtered = append(filtered, RSes[i])
		}
	}
	return filtered
}

type ReplicaSetsBySizeOlder []*apps.ReplicaSet

func (o ReplicaSetsBySizeOlder) Len() int      { return len(o) }
func (o ReplicaSetsBySizeOlder) Swap(i, j int) { o[i], o[j] = o[j], o[i] }
func (o ReplicaSetsBySizeOlder) Less(i, j int) bool {
	if *(o[i].Spec.Replicas) == *(o[j].Spec.Replicas) {
		return ReplicaSetsByCreationTimestamp(o).Less(i, j)
	}
	return *(o[i].Spec.Replicas) > *(o[j].Spec.Replicas)
}

func DeploymentRolloutSatisfied(deployment *apps.Deployment, partition intstrutil.IntOrString) error {
	if deployment.Status.ObservedGeneration < deployment.Generation {
		return fmt.Errorf("deployment %v observed generation %d less than generation %d",
			klog.KObj(deployment), deployment.Status.ObservedGeneration, deployment.Generation)
	}
	if deployment.Status.Replicas != *(deployment.Spec.Replicas) {
		return fmt.Errorf("deployment %v status replicas %d not equals to replicas %d",
			klog.KObj(deployment), deployment.Status.Replicas, *deployment.Spec.Replicas)
	}
	newRSReplicasLimit := NewRSReplicasLimit(partition, deployment)
	if deployment.Status.UpdatedReplicas < newRSReplicasLimit {
		return fmt.Errorf("deployment %v updated replicas %d less than partition %d",
			klog.KObj(deployment), deployment.Status.UpdatedReplicas, newRSReplicasLimit)
	}
	return nil
}
