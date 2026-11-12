package scheduler

import (
	"encoding/json"
	"sort"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/schedulerstate"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/initialsizeclass"
	scheduler_invocation "github.com/buildbarn/bb-remote-execution/pkg/scheduler/invocation"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// restoredLearner is a minimal initialsizeclass.Learner attached to
// in-flight tasks restored from a handover snapshot. It skips ISCC
// feedback writes on success while preserving fallback retry onto the
// largest size class if a restored task fails on a smaller size class.
type restoredLearner struct {
	onSmallerSizeClass bool
	expectedDuration   time.Duration
	timeout            time.Duration
}

func (*restoredLearner) Succeeded(duration time.Duration, sizeClasses []uint32) (int, time.Duration, time.Duration, initialsizeclass.Learner) {
	return 0, 0, 0, nil
}

func (l *restoredLearner) Failed(timedOut bool) (time.Duration, time.Duration, initialsizeclass.Learner) {
	if l.onSmallerSizeClass {
		return l.expectedDuration, l.timeout, &restoredLearner{
			onSmallerSizeClass: false,
		}
	}
	return 0, 0, nil
}

func (*restoredLearner) Abandoned() {}

func (l *restoredLearner) GetFallbackExecutionParameters() (time.Duration, time.Duration, bool) {
	return l.expectedDuration, l.timeout, l.onSmallerSizeClass
}

// Snapshot serializes the point-in-time state of InMemoryBuildQueue
// into a protobuf Snapshot message suitable for transferring state to a
// successor scheduler instance during a coordinated handover.
func (bq *InMemoryBuildQueue) Snapshot() (*schedulerstate.Snapshot, error) {
	bq.enter(bq.clock.Now())
	defer bq.leave()

	// Group distinct tasks by their current sizeClassQueue.
	tasksBySCQ := map[*sizeClassQueue][]*task{}
	seenTasks := map[*task]struct{}{}
	for _, o := range bq.operationsNameMap {
		t := o.task
		if _, seen := seenTasks[t]; seen {
			continue
		}
		seenTasks[t] = struct{}{}
		scq := t.getCurrentSizeClassQueue()
		tasksBySCQ[scq] = append(tasksBySCQ[scq], t)
	}

	pqs := append(platformQueueList(nil), bq.platformQueues...)
	sort.Sort(pqs)

	snapPQs := make([]*schedulerstate.PlatformQueueSnapshot, 0, len(pqs))
	for _, pq := range pqs {
		pqName := pq.platformKey.GetPlatformQueueName()
		snapSCQs := make([]*schedulerstate.SizeClassQueueSnapshot, 0, len(pq.sizeClassQueues))
		for _, scq := range pq.sizeClassQueues {
			tasks := tasksBySCQ[scq]
			// Sort tasks deterministically by their lowest operation name.
			sort.Slice(tasks, func(i, j int) bool {
				return lowestOperationName(tasks[i]) < lowestOperationName(tasks[j])
			})

			snapTasks := make([]*schedulerstate.TaskSnapshot, 0, len(tasks))
			for _, t := range tasks {
				ops := make([]*operation, 0, len(t.operations))
				for _, o := range t.operations {
					ops = append(ops, o)
				}
				sort.Slice(ops, func(i, j int) bool {
					return ops[i].name < ops[j].name
				})

				snapOps := make([]*schedulerstate.OperationSnapshot, 0, len(ops))
				for _, o := range ops {
					invIDs := make([]*anypb.Any, 0, len(o.invocation.invocationKeys))
					for _, k := range o.invocation.invocationKeys {
						invIDs = append(invIDs, k.GetID())
					}
					snapOps = append(snapOps, &schedulerstate.OperationSnapshot{
						Name:                   o.name,
						Priority:               o.priority,
						InvocationIds:          invIDs,
						MayExistWithoutWaiters: o.mayExistWithoutWaiters,
					})
				}

				desiredStateCopy := proto.Clone(&t.desiredState).(*remoteworker.DesiredState_Executing)
				stage := t.getStage()
				var executingWorkerID map[string]string
				var executingWorkerSizeClass *uint32
				if stage == remoteexecution.ExecutionStage_EXECUTING && t.currentWorker != nil {
					executingWorkerID = t.currentWorker.workerKey.getWorkerID()
					executingWorkerSizeClass = proto.Uint32(t.currentWorker.synchronizationSizeClass)
				}

				snapTask := &schedulerstate.TaskSnapshot{
					InstanceName:             t.actionDigest.GetInstanceName().String(),
					DesiredState:             desiredStateCopy,
					TargetId:                 t.targetID,
					CurrentStageStartTime:    timestamppb.New(t.currentStageStartTime),
					RetryCount:               int32(t.retryCount),
					ExpectedDuration:         durationpb.New(t.expectedDuration),
					MayExistWithoutWaiters:   t.mayExistWithoutWaiters,
					Stage:                    stage,
					ExecutingWorkerId:        executingWorkerID,
					ExecutingWorkerSizeClass: executingWorkerSizeClass,
					ExecuteResponse:          t.executeResponse,
					Operations:               snapOps,
				}
				if learner, ok := t.initialSizeClassLearner.(initialsizeclass.FallbackExecutionParametersProvider); ok {
					expectedDuration, timeout, retry := learner.GetFallbackExecutionParameters()
					snapTask.RetryOnLargestSizeClass = proto.Bool(retry)
					if retry {
						snapTask.FallbackExpectedDuration = durationpb.New(expectedDuration)
						snapTask.FallbackExecutionTimeout = durationpb.New(timeout)
					}
				}
				snapTasks = append(snapTasks, snapTask)
			}

			drainKeys := make([]string, 0, len(scq.drains))
			for k := range scq.drains {
				drainKeys = append(drainKeys, k)
			}
			sort.Strings(drainKeys)
			snapDrains := make([]*buildqueuestate.DrainState, 0, len(drainKeys))
			for _, k := range drainKeys {
				snapDrains = append(snapDrains, scq.drains[k])
			}

			snapSCQs = append(snapSCQs, &schedulerstate.SizeClassQueueSnapshot{
				SizeClass:    scq.sizeClass,
				MayBeRemoved: scq.mayBeRemoved,
				Tasks:        snapTasks,
				Drains:       snapDrains,
			})
		}

		snapPQs = append(snapPQs, &schedulerstate.PlatformQueueSnapshot{
			InstanceNamePrefix: pqName.InstanceNamePrefix,
			Platform:           pqName.Platform,
			SizeClassQueues:    snapSCQs,
		})
	}

	return &schedulerstate.Snapshot{
		PlatformQueues:                      snapPQs,
		PlatformQueueAbsenceHardFailureTime: timestamppb.New(bq.platformQueueAbsenceHardFailureTime),
	}, nil
}

func lowestOperationName(t *task) string {
	lowest := ""
	for _, o := range t.operations {
		if lowest == "" || o.name < lowest {
			lowest = o.name
		}
	}
	return lowest
}

// RestoreState populates InMemoryBuildQueue from a handover Snapshot.
// Predeclared platform queues already registered on bq retain their
// configured parameters, while dynamically registered queues and tasks
// from the snapshot are merged in.
func (bq *InMemoryBuildQueue) RestoreState(snap *schedulerstate.Snapshot) error {
	if snap == nil {
		return nil
	}

	bq.enter(bq.clock.Now())
	defer bq.leave()

	if snap.PlatformQueueAbsenceHardFailureTime != nil {
		bq.platformQueueAbsenceHardFailureTime = snap.PlatformQueueAbsenceHardFailureTime.AsTime()
	}

	adoptionTimeout := bq.configuration.WorkerAdoptionTimeout
	if adoptionTimeout <= 0 {
		adoptionTimeout = 15 * time.Second
	}
	recoveredGracePeriod := bq.configuration.RecoveredOperationWithNoWaitersTimeout
	if recoveredGracePeriod <= 0 {
		recoveredGracePeriod = bq.configuration.OperationWithNoWaitersTimeout
	}

	for _, snapPQ := range snap.PlatformQueues {
		platformKey, err := newPlatformKeyFromName(&buildqueuestate.PlatformQueueName{
			InstanceNamePrefix: snapPQ.InstanceNamePrefix,
			Platform:           snapPQ.Platform,
		})
		if err != nil {
			return util.StatusWrap(err, "Failed to restore platform queue key")
		}

		var pq *platformQueue
		if pqIndex := bq.platformQueuesTrie.GetExact(platformKey); pqIndex >= 0 {
			pq = bq.platformQueues[pqIndex]
		} else {
			pq = bq.addPlatformQueue(platformKey, nil, 0, 0)
		}

		for _, snapSCQ := range snapPQ.SizeClassQueues {
			scKey := sizeClassKey{
				platformKey: platformKey,
				sizeClass:   snapSCQ.SizeClass,
			}
			scq, ok := bq.sizeClassQueues[scKey]
			if !ok {
				// Queued work in a removed size class must move to the new
				// largest queue, instead of recreating a queue without workers.
				if len(pq.sizeClassQueues) > 0 && !pq.sizeClassQueues[len(pq.sizeClassQueues)-1].mayBeRemoved {
					scq = pq.sizeClassQueues[len(pq.sizeClassQueues)-1]
				}
				if scq == nil {
					scq = pq.addSizeClassQueue(bq, snapSCQ.SizeClass, true)
				}
			}

			// Drains belong to a specific size class. Moving work out of a
			// removed class must not drain its replacement workers as well.
			if scq.sizeClass == snapSCQ.SizeClass {
				for _, drain := range snapSCQ.Drains {
					drainKeyBytes, err := json.Marshal(drain.WorkerIdPattern)
					if err != nil {
						return util.StatusWrap(err, "Failed to marshal drain worker ID pattern")
					}
					scq.drains[string(drainKeyBytes)] = drain
				}
			}

			onSmallerSizeClass := len(pq.sizeClassQueues) > 0 && scq != pq.sizeClassQueues[len(pq.sizeClassQueues)-1]

			for _, snapTask := range snapSCQ.Tasks {
				instanceName, err := digest.NewInstanceName(snapTask.InstanceName)
				if err != nil {
					return util.StatusWrapf(err, "Invalid instance name %#v in task snapshot", snapTask.InstanceName)
				}
				digestFunction, err := instanceName.GetDigestFunction(
					snapTask.DesiredState.GetDigestFunction(),
					len(snapTask.DesiredState.GetActionDigest().GetHash()),
				)
				if err != nil {
					return util.StatusWrap(err, "Invalid digest function in task snapshot")
				}
				actionDigest, err := digestFunction.NewDigestFromProto(snapTask.DesiredState.GetActionDigest())
				if err != nil {
					return util.StatusWrap(err, "Invalid action digest in task snapshot")
				}

				t := &task{
					operations:             make(map[*invocation]*operation, len(snapTask.Operations)),
					actionDigest:           actionDigest,
					targetID:               snapTask.TargetId,
					currentStageStartTime:  snapTask.CurrentStageStartTime.AsTime(),
					retryCount:             int(snapTask.RetryCount),
					expectedDuration:       snapTask.ExpectedDuration.AsDuration(),
					mayExistWithoutWaiters: snapTask.MayExistWithoutWaiters,
				}
				if snapTask.DesiredState != nil {
					proto.Merge(&t.desiredState, snapTask.DesiredState)
				}

				var actionTimeout time.Duration
				if timeoutProto := t.desiredState.Action.GetTimeout(); timeoutProto != nil {
					actionTimeout = timeoutProto.AsDuration()
				}
				learner := &restoredLearner{
					onSmallerSizeClass: onSmallerSizeClass,
					expectedDuration:   t.expectedDuration,
					timeout:            actionTimeout,
				}
				if snapTask.RetryOnLargestSizeClass != nil {
					learner.onSmallerSizeClass = snapTask.GetRetryOnLargestSizeClass()
					learner.expectedDuration = snapTask.FallbackExpectedDuration.AsDuration()
					learner.timeout = snapTask.FallbackExecutionTimeout.AsDuration()
				}
				if snapTask.Stage == remoteexecution.ExecutionStage_QUEUED && scq.sizeClass != snapSCQ.SizeClass && learner.onSmallerSizeClass {
					// This task is already moving to the largest class. Give
					// it the original timeout before its first execution there.
					t.expectedDuration = learner.expectedDuration
					t.desiredState.Action.Timeout = durationpb.New(learner.timeout)
					learner.onSmallerSizeClass = false
				}

				for _, snapOp := range snapTask.Operations {
					invKeys := make([]scheduler_invocation.Key, 0, len(snapOp.InvocationIds))
					for _, id := range snapOp.InvocationIds {
						invKey, err := scheduler_invocation.NewKey(id)
						if err != nil {
							return util.StatusWrap(err, "Invalid invocation ID in operation snapshot")
						}
						invKeys = append(invKeys, invKey)
					}
					inv := scq.getOrCreateInvocation(bq, invKeys)
					o := &operation{
						name:                   snapOp.Name,
						task:                   t,
						priority:               snapOp.Priority,
						invocation:             inv,
						queueIndex:             -1,
						mayExistWithoutWaiters: snapOp.MayExistWithoutWaiters,
					}
					t.operations[inv] = o
					bq.operationsNameMap[o.name] = o
					if !o.mayExistWithoutWaiters {
						oToRemove := o
						bq.cleanupQueue.add(&oToRemove.cleanupKey, bq.now.Add(recoveredGracePeriod), func() {
							oToRemove.remove(bq)
						})
					}
					if snapTask.Stage == remoteexecution.ExecutionStage_COMPLETED {
						// Completed operations retain detached invocation references.
						// Remove each newly created path before creating the next:
						// several operations may share an otherwise empty ancestor.
						for curr := inv; curr != nil && curr.removeIfEmpty(); curr = curr.parent {
						}
					}
				}

				switch snapTask.Stage {
				case remoteexecution.ExecutionStage_QUEUED:
					t.stageChangeWakeup = make(chan struct{})
					t.initialSizeClassLearner = learner
					if !t.desiredState.Action.GetDoNotCache() {
						bq.inFlightDeduplicationMap[actionDigest] = t
					}
					for _, o := range t.operations {
						o.enqueue()
					}

				case remoteexecution.ExecutionStage_EXECUTING:
					t.stageChangeWakeup = make(chan struct{})
					t.initialSizeClassLearner = learner
					if !t.desiredState.Action.GetDoNotCache() {
						bq.inFlightDeduplicationMap[actionDigest] = t
					}
					wKey := newWorkerKey(snapTask.ExecutingWorkerId)
					workerSizeClass := snapSCQ.SizeClass
					if snapTask.ExecutingWorkerSizeClass != nil {
						workerSizeClass = snapTask.GetExecutingWorkerSizeClass()
					}
					if _, exists := scq.workers[wKey]; exists {
						return status.Errorf(codes.FailedPrecondition, "Cannot restore worker %s from multiple size classes into size class %d", wKey, scq.sizeClass)
					}
					w := &worker{
						workerKey:                wKey,
						synchronizationSizeClass: workerSizeClass,
						listIndex:                -1,
						stickinessStartingTimes:  make([]time.Time, len(pq.workerInvocationStickinessLimits)),
					}
					scq.workers[wKey] = w
					scq.workersCreatedTotal.Inc()
					w.currentTask = t
					w.unadopted = true
					t.currentWorker = w
					if workerSizeClass != scq.sizeClass {
						bq.restoredWorkerQueues[workerRegistrationKey{
							sizeClassKey{platformKey: platformKey, sizeClass: workerSizeClass}, wKey,
						}] = scq
					}
					for inv := range t.operations {
						inv.incrementExecutingWorkersCount(bq, w)
					}
					removalTime := bq.now.Add(adoptionTimeout)
					scqForWorker := scq
					bq.cleanupQueue.add(&w.cleanupKey, removalTime, func() {
						scqForWorker.removeStaleWorker(bq, wKey, removalTime)
					})

				case remoteexecution.ExecutionStage_COMPLETED:
					t.executeResponse = snapTask.ExecuteResponse
					t.desiredState.Action = nil
					t.stageChangeWakeup = nil
					t.initialSizeClassLearner = nil
				}
			}
		}
	}

	// Ensure cleanup keys on removable size class queues are consistent
	// with whether any workers were restored into them.
	for _, pq := range bq.platformQueues {
		for _, scq := range pq.sizeClassQueues {
			if len(scq.workers) > 0 {
				if scq.cleanupKey.isActive() {
					bq.cleanupQueue.remove(scq.cleanupKey)
				}
			} else if scq.mayBeRemoved && !scq.cleanupKey.isActive() {
				scqToRemove := scq
				bq.cleanupQueue.add(&scqToRemove.cleanupKey, bq.now.Add(bq.configuration.PlatformQueueWithNoWorkersTimeout), func() {
					scqToRemove.remove(bq)
				})
			}
		}
	}

	return nil
}
