package scheduler_test

import (
	"context"
	"io"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/schedulerstate"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/platform"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.uber.org/mock/gomock"
)

func newBuildQueueForRestoreTest(t *testing.T, now *time.Time, sizeClasses []uint32) *scheduler.InMemoryBuildQueue {
	t.Helper()
	ctrl := gomock.NewController(t)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().DoAndReturn(func() time.Time { return *now }).AnyTimes()
	bq := scheduler.NewInMemoryBuildQueue(nil, clock, nil, &buildQueueConfigurationForTesting, nil,
		allowAllAuthorizer, allowAllAuthorizer, allowAllAuthorizer, allowAllAuthorizer)
	require.NoError(t, bq.RegisterPredeclaredPlatformQueue(
		util.Must(digest.NewInstanceName("main")), platformForTesting, nil, 0, 0, sizeClasses))
	return bq
}

func snapshotTaskForRestoreTest(stage remoteexecution.ExecutionStage_Value) *schedulerstate.TaskSnapshot {
	task := &schedulerstate.TaskSnapshot{
		InstanceName: "main",
		DesiredState: &remoteworker.DesiredState_Executing{
			ActionDigest:   &remoteexecution.Digest{Hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SizeBytes: 123},
			DigestFunction: remoteexecution.DigestFunction_SHA1,
			Action:         &remoteexecution.Action{Timeout: durationpb.New(15 * time.Second)},
		},
		Stage:                 stage,
		Operations:            []*schedulerstate.OperationSnapshot{{Name: "operation"}},
		CurrentStageStartTime: timestamppb.New(time.Unix(1000, 0)),
		ExpectedDuration:      durationpb.New(15 * time.Second),
	}
	if stage == remoteexecution.ExecutionStage_EXECUTING {
		task.ExecutingWorkerId = map[string]string{"id": "old-worker"}
	}
	if stage == remoteexecution.ExecutionStage_COMPLETED {
		task.DesiredState.Action = nil
		task.ExecuteResponse = &remoteexecution.ExecuteResponse{Result: &remoteexecution.ActionResult{}}
	}
	return task
}

func snapshotForRestoreTest(sizeClass uint32, tasks ...*schedulerstate.TaskSnapshot) *schedulerstate.Snapshot {
	return &schedulerstate.Snapshot{PlatformQueues: []*schedulerstate.PlatformQueueSnapshot{{
		InstanceNamePrefix: "main", Platform: platformForTesting,
		SizeClassQueues: []*schedulerstate.SizeClassQueueSnapshot{{SizeClass: sizeClass, Tasks: tasks}},
	}}}
}

func TestInMemoryBuildQueueRestoreCompletedSharedInvocationAncestors(t *testing.T) {
	for _, paths := range [][][]string{{{"shared", "a"}, {"shared", "b"}}, {{"shared"}, {"shared", "child"}}} {
		now := time.Unix(1000, 0)
		bq := newBuildQueueForRestoreTest(t, &now, []uint32{0})
		task := snapshotTaskForRestoreTest(remoteexecution.ExecutionStage_COMPLETED)
		task.Operations = nil
		for index, path := range paths {
			op := &schedulerstate.OperationSnapshot{Name: []string{"first", "second"}[index]}
			for _, id := range path {
				op.InvocationIds = append(op.InvocationIds, util.Must(anypb.New(&remoteexecution.RequestMetadata{ToolInvocationId: id})))
			}
			task.Operations = append(task.Operations, op)
		}
		require.NoError(t, bq.RestoreState(snapshotForRestoreTest(0, task)))
		for _, name := range []string{"first", "second"} {
			op, err := bq.GetOperation(context.Background(), &buildqueuestate.GetOperationRequest{OperationName: name})
			require.NoError(t, err)
			require.NotNil(t, op.Operation.GetCompleted())
		}
		roundTrip, err := bq.Snapshot()
		require.NoError(t, err)
		require.Len(t, roundTrip.PlatformQueues[0].SizeClassQueues[0].Tasks[0].Operations, 2)
		now = now.Add(time.Minute + time.Second)
		bq.ForceCleanup()
		_, err = bq.GetOperation(context.Background(), &buildqueuestate.GetOperationRequest{OperationName: "first"})
		require.Equal(t, codes.NotFound, status.Code(err))
	}
}

func TestInMemoryBuildQueueRestoreRemovedQueuedSizeClass(t *testing.T) {
	now := time.Unix(1000, 0)
	bq := newBuildQueueForRestoreTest(t, &now, []uint32{1, 4})
	task := snapshotTaskForRestoreTest(remoteexecution.ExecutionStage_QUEUED)
	snapshot := snapshotForRestoreTest(2, task)
	// A wildcard drain on the removed class must not be transplanted onto
	// the replacement queue, where it would prevent the restored work running.
	snapshot.PlatformQueues[0].SizeClassQueues[0].Drains = []*buildqueuestate.DrainState{{}}
	require.NoError(t, bq.RestoreState(snapshot))
	op, err := bq.GetOperation(context.Background(), &buildqueuestate.GetOperationRequest{OperationName: "operation"})
	require.NoError(t, err)
	require.Equal(t, uint32(4), op.Operation.InvocationName.SizeClassQueueName.SizeClass)
	queues, err := bq.ListPlatformQueues(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Len(t, queues.PlatformQueues[0].SizeClassQueues, 2)
	require.Zero(t, queues.PlatformQueues[0].SizeClassQueues[1].DrainsCount)
	response, err := bq.Synchronize(context.Background(), &remoteworker.SynchronizeRequest{
		InstanceNamePrefix: "main", Platform: platformForTesting, SizeClass: 4,
		WorkerId:     map[string]string{"id": "new-worker"},
		CurrentState: &remoteworker.CurrentState{WorkerState: &remoteworker.CurrentState_Idle{Idle: &emptypb.Empty{}}},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, task.DesiredState.ActionDigest, response.DesiredState.GetExecuting().ActionDigest)
}

func TestInMemoryBuildQueueRestoreRemovedExecutingSizeClass(t *testing.T) {
	now := time.Unix(1000, 0)
	bq := newBuildQueueForRestoreTest(t, &now, []uint32{1, 2})
	task := snapshotTaskForRestoreTest(remoteexecution.ExecutionStage_EXECUTING)
	queuedTask := snapshotTaskForRestoreTest(remoteexecution.ExecutionStage_QUEUED)
	queuedTask.Operations[0].Name = "queued"
	queuedTask.DesiredState.ActionDigest.Hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	require.NoError(t, bq.RestoreState(snapshotForRestoreTest(4, task, queuedTask)))
	request := &remoteworker.SynchronizeRequest{
		InstanceNamePrefix: "main", Platform: platformForTesting, SizeClass: 4, WorkerId: task.ExecutingWorkerId,
		CurrentState: &remoteworker.CurrentState{WorkerState: &remoteworker.CurrentState_Executing_{Executing: &remoteworker.CurrentState_Executing{
			ActionDigest:   task.DesiredState.ActionDigest,
			ExecutionState: &remoteworker.CurrentState_Executing_Running{Running: &emptypb.Empty{}},
		}}},
	}
	response, err := bq.Synchronize(context.Background(), request)
	require.NoError(t, err)
	require.Nil(t, response.DesiredState)
	// Another handover must retain the physical worker's original size class.
	snapshot, err := bq.Snapshot()
	require.NoError(t, err)
	nextBQ := newBuildQueueForRestoreTest(t, &now, []uint32{1, 2})
	require.NoError(t, nextBQ.RestoreState(snapshot))
	response, err = nextBQ.Synchronize(context.Background(), request)
	require.NoError(t, err)
	require.Nil(t, response.DesiredState)
	request.CurrentState.GetExecuting().ExecutionState = &remoteworker.CurrentState_Executing_Completed{
		Completed: &remoteexecution.ExecuteResponse{Result: &remoteexecution.ActionResult{}},
	}
	response, err = nextBQ.Synchronize(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, response.DesiredState.GetIdle())
	op, err := nextBQ.GetOperation(context.Background(), &buildqueuestate.GetOperationRequest{OperationName: "operation"})
	require.NoError(t, err)
	require.NotNil(t, op.Operation.GetCompleted())
	queued, err := nextBQ.GetOperation(context.Background(), &buildqueuestate.GetOperationRequest{OperationName: "queued"})
	require.NoError(t, err)
	require.NotNil(t, queued.Operation.GetQueued())
}

func TestInMemoryBuildQueueRestoreRemovedWorkerRequeuesTask(t *testing.T) {
	for _, mode := range []string{"idle", "mismatched", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Unix(1000, 0)
			bq := newBuildQueueForRestoreTest(t, &now, []uint32{1, 2})
			task := snapshotTaskForRestoreTest(remoteexecution.ExecutionStage_EXECUTING)
			task.RetryOnLargestSizeClass = proto.Bool(true)
			task.FallbackExecutionTimeout = durationpb.New(30 * time.Minute)
			task.FallbackExpectedDuration = durationpb.New(2 * time.Minute)
			require.NoError(t, bq.RestoreState(snapshotForRestoreTest(4, task)))
			if mode == "timeout" {
				now = now.Add(16 * time.Second)
				bq.ForceCleanup()
			} else {
				currentState := &remoteworker.CurrentState{WorkerState: &remoteworker.CurrentState_Idle{Idle: &emptypb.Empty{}}}
				if mode == "mismatched" {
					currentState.WorkerState = &remoteworker.CurrentState_Executing_{Executing: &remoteworker.CurrentState_Executing{
						ActionDigest:   &remoteexecution.Digest{Hash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", SizeBytes: 123},
						ExecutionState: &remoteworker.CurrentState_Executing_Running{Running: &emptypb.Empty{}},
					}}
				}
				response, err := bq.Synchronize(context.Background(), &remoteworker.SynchronizeRequest{
					InstanceNamePrefix: "main", Platform: platformForTesting, SizeClass: 4,
					WorkerId: task.ExecutingWorkerId, CurrentState: currentState,
				})
				require.NoError(t, err)
				require.NotNil(t, response.DesiredState.GetIdle())
			}
			response, err := bq.Synchronize(context.Background(), &remoteworker.SynchronizeRequest{
				InstanceNamePrefix: "main", Platform: platformForTesting, SizeClass: 2,
				WorkerId:     map[string]string{"id": "new-worker"},
				CurrentState: &remoteworker.CurrentState{WorkerState: &remoteworker.CurrentState_Idle{Idle: &emptypb.Empty{}}},
			})
			require.NoError(t, err)
			testutil.RequireEqualProto(t, task.DesiredState.ActionDigest, response.DesiredState.GetExecuting().ActionDigest)
			require.Equal(t, 30*time.Minute, response.DesiredState.GetExecuting().Action.Timeout.AsDuration())
		})
	}
}

func TestInMemoryBuildQueueRestoreRejectsCollidingWorkerIDs(t *testing.T) {
	now := time.Unix(1000, 0)
	bq := newBuildQueueForRestoreTest(t, &now, []uint32{1, 2})
	first := snapshotTaskForRestoreTest(remoteexecution.ExecutionStage_EXECUTING)
	second := snapshotTaskForRestoreTest(remoteexecution.ExecutionStage_EXECUTING)
	second.Operations[0].Name = "other"
	second.DesiredState.ActionDigest.Hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	snapshot := snapshotForRestoreTest(2, first)
	snapshot.PlatformQueues[0].SizeClassQueues = append(snapshot.PlatformQueues[0].SizeClassQueues,
		&schedulerstate.SizeClassQueueSnapshot{SizeClass: 4, Tasks: []*schedulerstate.TaskSnapshot{second}})
	require.Equal(t, codes.FailedPrecondition, status.Code(bq.RestoreState(snapshot)))
}

func TestInMemoryBuildQueueRestoredFallbackTimeout(t *testing.T) {
	for _, oldSizeClass := range []uint32{1, 2} {
		now := time.Unix(1000, 0)
		bq := newBuildQueueForRestoreTest(t, &now, []uint32{1, 4})
		task := snapshotTaskForRestoreTest(remoteexecution.ExecutionStage_QUEUED)
		task.RetryOnLargestSizeClass = proto.Bool(true)
		task.FallbackExecutionTimeout = durationpb.New(30 * time.Minute)
		task.FallbackExpectedDuration = durationpb.New(2 * time.Minute)
		require.NoError(t, bq.RestoreState(snapshotForRestoreTest(oldSizeClass, task)))
		// Verify the retry parameters survive another snapshot/restore, too.
		snapshot, err := bq.Snapshot()
		require.NoError(t, err)
		bq = newBuildQueueForRestoreTest(t, &now, []uint32{1, 4})
		require.NoError(t, bq.RestoreState(snapshot))
		request := &remoteworker.SynchronizeRequest{
			InstanceNamePrefix: "main", Platform: platformForTesting, SizeClass: 1,
			WorkerId:     map[string]string{"id": "small-worker"},
			CurrentState: &remoteworker.CurrentState{WorkerState: &remoteworker.CurrentState_Idle{Idle: &emptypb.Empty{}}},
		}
		if oldSizeClass == 1 {
			response, err := bq.Synchronize(context.Background(), request)
			require.NoError(t, err)
			require.Equal(t, 15*time.Second, response.DesiredState.GetExecuting().Action.Timeout.AsDuration())
			request.PreferBeingIdle = true
			request.CurrentState.WorkerState = &remoteworker.CurrentState_Executing_{Executing: &remoteworker.CurrentState_Executing{
				ActionDigest:   task.DesiredState.ActionDigest,
				ExecutionState: &remoteworker.CurrentState_Executing_Completed{Completed: &remoteexecution.ExecuteResponse{Status: status.New(codes.DeadlineExceeded, "exploratory timeout").Proto()}},
			}}
			_, err = bq.Synchronize(context.Background(), request)
			require.NoError(t, err)
		}
		response, err := bq.Synchronize(context.Background(), &remoteworker.SynchronizeRequest{
			InstanceNamePrefix: "main", Platform: platformForTesting, SizeClass: 4,
			WorkerId:     map[string]string{"id": "large-worker"},
			CurrentState: &remoteworker.CurrentState{WorkerState: &remoteworker.CurrentState_Idle{Idle: &emptypb.Empty{}}},
		})
		require.NoError(t, err)
		require.Equal(t, 30*time.Minute, response.DesiredState.GetExecuting().Action.Timeout.AsDuration())
	}
}

func TestInMemoryBuildQueueSnapshotRestoreAndAdopt(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	actionReader := mock.NewMockReader[*remoteexecution.Action](ctrl)
	clockO := mock.NewMockClock(ctrl)
	clockO.EXPECT().Now().Return(time.Unix(1000, 0))
	uuidGeneratorO := mock.NewMockUUIDGenerator(ctrl)
	actionRouterO := mock.NewMockActionRouter(ctrl)

	configO := buildQueueConfigurationForTesting
	configO.WorkerAdoptionTimeout = 15 * time.Second
	configO.RecoveredOperationWithNoWaitersTimeout = 30 * time.Second

	bqO := scheduler.NewInMemoryBuildQueue(
		actionReader,
		clockO,
		uuidGeneratorO.Call,
		&configO,
		actionRouterO,
		allowAllAuthorizer,
		allowAllAuthorizer,
		allowAllAuthorizer,
		allowAllAuthorizer,
	)
	execClientO := getExecutionClient(t, bqO)

	clockO.EXPECT().Now().Return(time.Unix(1000, 0))
	require.NoError(t, bqO.RegisterPredeclaredPlatformQueue(
		util.Must(digest.NewInstanceName("main")),
		platformForTesting,
		nil,
		0,
		0,
		[]uint32{0},
	))

	// Add a drain to verify drain round-tripping.
	clockO.EXPECT().Now().Return(time.Unix(1001, 0))
	_, err := bqO.AddDrain(ctx, &buildqueuestate.AddOrRemoveDrainRequest{
		SizeClassQueueName: &buildqueuestate.SizeClassQueueName{
			PlatformQueueName: &buildqueuestate.PlatformQueueName{
				InstanceNamePrefix: "main",
				Platform:           platformForTesting,
			},
			SizeClass: 0,
		},
		WorkerIdPattern: map[string]string{"hostname": "drained-worker"},
	})
	require.NoError(t, err)

	// Enqueue two actions on bqO: action1 (will become EXECUTING on worker-1) and action2 (will stay QUEUED).
	action1 := &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "1111111111111111111111111111111111111111",
			SizeBytes: 100,
		},
	}
	digest1 := &remoteexecution.Digest{
		Hash:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SizeBytes: 123,
	}
	actionReader.EXPECT().Read(
		gomock.Any(),
		digest.MustNewDigest("main", remoteexecution.DigestFunction_SHA1, digest1.Hash, digest1.SizeBytes),
	).Return(action1, nil)
	selector1 := mock.NewMockSelector(ctrl)
	learner1 := mock.NewMockLearner(ctrl)
	actionRouterO.EXPECT().RouteAction(gomock.Any(), gomock.Any(), testutil.EqProto(t, action1), nil).Return(
		action1,
		platform.MustNewKey("main", platformForTesting),
		nil,
		selector1,
		nil,
	)
	selector1.EXPECT().Select([]uint32{0}).Return(0, 10*time.Minute, 20*time.Minute, learner1)
	clockO.EXPECT().Now().Return(time.Unix(1002, 0))
	timer1 := mock.NewMockTimer(ctrl)
	clockO.EXPECT().NewTimer(time.Minute).Return(timer1, nil)
	uuidGeneratorO.EXPECT().Call().Return(uuid.Parse("11111111-1111-1111-1111-111111111111"))

	ctx1, cancel1 := context.WithCancel(ctx)
	stream1, err := execClientO.Execute(ctx1, &remoteexecution.ExecuteRequest{
		InstanceName: "main",
		ActionDigest: digest1,
	})
	require.NoError(t, err)
	op1Queued, err := stream1.Recv()
	require.NoError(t, err)
	require.Equal(t, "11111111-1111-1111-1111-111111111111", op1Queued.Name)

	// Assign action1 to worker-1 so it is EXECUTING.
	clockO.EXPECT().Now().Return(time.Unix(1003, 0))
	timer1.EXPECT().Stop().Return(true)
	clockO.EXPECT().Now().Return(time.Unix(1003, 0))
	timer1Exec := mock.NewMockTimer(ctrl)
	clockO.EXPECT().NewTimer(time.Minute).Return(timer1Exec, nil)

	syncResp, err := bqO.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId:           map[string]string{"hostname": "worker-1"},
		InstanceNamePrefix: "main",
		Platform:           platformForTesting,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, syncResp.DesiredState.GetExecuting())
	_, err = stream1.Recv()
	require.NoError(t, err)

	// Enqueue action2 (stays QUEUED).
	action2 := &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "2222222222222222222222222222222222222222",
			SizeBytes: 200,
		},
	}
	digest2 := &remoteexecution.Digest{
		Hash:      "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		SizeBytes: 234,
	}
	actionReader.EXPECT().Read(
		gomock.Any(),
		digest.MustNewDigest("main", remoteexecution.DigestFunction_SHA1, digest2.Hash, digest2.SizeBytes),
	).Return(action2, nil)
	selector2 := mock.NewMockSelector(ctrl)
	learner2 := mock.NewMockLearner(ctrl)
	actionRouterO.EXPECT().RouteAction(gomock.Any(), gomock.Any(), testutil.EqProto(t, action2), nil).Return(
		action2,
		platform.MustNewKey("main", platformForTesting),
		nil,
		selector2,
		nil,
	)
	selector2.EXPECT().Select([]uint32{0}).Return(0, 5*time.Minute, 15*time.Minute, learner2)
	clockO.EXPECT().Now().Return(time.Unix(1004, 0))
	timer2 := mock.NewMockTimer(ctrl)
	clockO.EXPECT().NewTimer(time.Minute).Return(timer2, nil)
	uuidGeneratorO.EXPECT().Call().Return(uuid.Parse("22222222-2222-2222-2222-222222222222"))

	ctx2, cancel2 := context.WithCancel(ctx)
	stream2, err := execClientO.Execute(ctx2, &remoteexecution.ExecuteRequest{
		InstanceName: "main",
		ActionDigest: digest2,
	})
	require.NoError(t, err)
	op2Queued, err := stream2.Recv()
	require.NoError(t, err)
	require.Equal(t, "22222222-2222-2222-2222-222222222222", op2Queued.Name)

	// Interrupt client streams on bqO (simulating LeaderGate freeze before Snapshot).
	timer1Exec.EXPECT().Stop().Return(true)
	clockO.EXPECT().Now().Return(time.Unix(1005, 0))
	cancel1()
	_, err = stream1.Recv()
	require.Error(t, err)

	timer2.EXPECT().Stop().Return(true)
	clockO.EXPECT().Now().Return(time.Unix(1005, 0))
	cancel2()
	_, err = stream2.Recv()
	require.Error(t, err)

	// Take Snapshot on bqO at t=1005.
	clockO.EXPECT().Now().Return(time.Unix(1005, 0))
	snap, err := bqO.Snapshot()
	require.NoError(t, err)
	require.Len(t, snap.PlatformQueues, 1)
	require.Len(t, snap.PlatformQueues[0].SizeClassQueues[0].Tasks, 2)

	// Create candidate scheduler bqN and restore the snapshot at t=1005.
	clockN := mock.NewMockClock(ctrl)
	clockN.EXPECT().Now().Return(time.Unix(1005, 0))
	uuidGeneratorN := mock.NewMockUUIDGenerator(ctrl)
	actionRouterN := mock.NewMockActionRouter(ctrl)
	bqN := scheduler.NewInMemoryBuildQueue(
		actionReader,
		clockN,
		uuidGeneratorN.Call,
		&configO,
		actionRouterN,
		allowAllAuthorizer,
		allowAllAuthorizer,
		allowAllAuthorizer,
		allowAllAuthorizer,
	)
	execClientN := getExecutionClient(t, bqN)

	clockN.EXPECT().Now().Return(time.Unix(1005, 0))
	require.NoError(t, bqN.RegisterPredeclaredPlatformQueue(
		util.Must(digest.NewInstanceName("main")),
		platformForTesting,
		nil,
		0,
		0,
		[]uint32{0},
	))
	clockN.EXPECT().Now().Return(time.Unix(1005, 0))
	require.NoError(t, bqN.RestoreState(snap))

	// Verify ListDrains, ListQueuedOperations, and ListWorkers match between bqO and bqN.
	clockO.EXPECT().Now().Return(time.Unix(1005, 0))
	drainsO, err := bqO.ListDrains(ctx, &buildqueuestate.ListDrainsRequest{
		SizeClassQueueName: &buildqueuestate.SizeClassQueueName{
			PlatformQueueName: &buildqueuestate.PlatformQueueName{
				InstanceNamePrefix: "main",
				Platform:           platformForTesting,
			},
		},
	})
	require.NoError(t, err)
	clockN.EXPECT().Now().Return(time.Unix(1005, 0))
	drainsN, err := bqN.ListDrains(ctx, &buildqueuestate.ListDrainsRequest{
		SizeClassQueueName: &buildqueuestate.SizeClassQueueName{
			PlatformQueueName: &buildqueuestate.PlatformQueueName{
				InstanceNamePrefix: "main",
				Platform:           platformForTesting,
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, drainsO, drainsN)

	// Re-attach to operation 1 on bqN via WaitExecution (simulating O's stream relay).
	clockN.EXPECT().Now().Return(time.Unix(1006, 0)).Times(2)
	timerN1 := mock.NewMockTimer(ctrl)
	clockN.EXPECT().NewTimer(time.Minute).Return(timerN1, nil)
	waitStream1, err := execClientN.WaitExecution(ctx, &remoteexecution.WaitExecutionRequest{
		Name: "11111111-1111-1111-1111-111111111111",
	})
	require.NoError(t, err)
	op1OnN, err := waitStream1.Recv()
	require.NoError(t, err)
	require.False(t, op1OnN.Done)

	// Worker-1 synchronizes with bqN reporting Executing on digest1, adopting the task,
	// and then reports Completed!
	clockN.EXPECT().Now().Return(time.Unix(1007, 0))
	syncRespN, err := bqN.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId:           map[string]string{"hostname": "worker-1"},
		InstanceNamePrefix: "main",
		Platform:           platformForTesting,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: digest1,
					ExecutionState: &remoteworker.CurrentState_Executing_Running{
						Running: &emptypb.Empty{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: &timestamppb.Timestamp{Seconds: 1017},
	}, syncRespN)

	// Now worker-1 reports Completed for digest1; bqN completes op1 and immediately assigns queued action2 to worker-1!
	clockN.EXPECT().Now().Return(time.Unix(1008, 0)).Times(3)
	timerN1.EXPECT().Stop().Return(true)
	syncRespN2, err := bqN.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId:           map[string]string{"hostname": "worker-1"},
		InstanceNamePrefix: "main",
		Platform:           platformForTesting,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: digest1,
					ExecutionState: &remoteworker.CurrentState_Executing_Completed{
						Completed: &remoteexecution.ExecuteResponse{
							Result: &remoteexecution.ActionResult{
								ExitCode: 0,
								ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
									VirtualExecutionDuration: durationpb.New(5 * time.Second),
								},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, syncRespN2.DesiredState.GetExecuting())
	testutil.RequireEqualProto(t, digest2, syncRespN2.DesiredState.GetExecuting().ActionDigest)

	op1Final, err := waitStream1.Recv()
	require.NoError(t, err)
	require.True(t, op1Final.Done)
	_, err = waitStream1.Recv()
	require.Equal(t, io.EOF, err)
}

func TestInMemoryBuildQueueAdoptionTimeoutReenqueuesTask(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	actionReader := mock.NewMockReader[*remoteexecution.Action](ctrl)
	clockO := mock.NewMockClock(ctrl)
	clockO.EXPECT().Now().Return(time.Unix(1000, 0))
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	actionRouter := mock.NewMockActionRouter(ctrl)

	config := buildQueueConfigurationForTesting
	config.WorkerAdoptionTimeout = 15 * time.Second
	config.RecoveredOperationWithNoWaitersTimeout = 60 * time.Second

	bqO := scheduler.NewInMemoryBuildQueue(
		actionReader,
		clockO,
		uuidGenerator.Call,
		&config,
		actionRouter,
		allowAllAuthorizer,
		allowAllAuthorizer,
		allowAllAuthorizer,
		allowAllAuthorizer,
	)
	execClientO := getExecutionClient(t, bqO)

	clockO.EXPECT().Now().Return(time.Unix(1000, 0))
	require.NoError(t, bqO.RegisterPredeclaredPlatformQueue(
		util.Must(digest.NewInstanceName("main")),
		platformForTesting,
		nil,
		0,
		0,
		[]uint32{0},
	))

	action1 := &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "1111111111111111111111111111111111111111",
			SizeBytes: 100,
		},
	}
	digest1 := &remoteexecution.Digest{
		Hash:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SizeBytes: 123,
	}
	actionReader.EXPECT().Read(
		gomock.Any(),
		digest.MustNewDigest("main", remoteexecution.DigestFunction_SHA1, digest1.Hash, digest1.SizeBytes),
	).Return(action1, nil)
	selector1 := mock.NewMockSelector(ctrl)
	learner1 := mock.NewMockLearner(ctrl)
	actionRouter.EXPECT().RouteAction(gomock.Any(), gomock.Any(), testutil.EqProto(t, action1), nil).Return(
		action1,
		platform.MustNewKey("main", platformForTesting),
		nil,
		selector1,
		nil,
	)
	selector1.EXPECT().Select([]uint32{0}).Return(0, 10*time.Minute, 20*time.Minute, learner1)
	clockO.EXPECT().Now().Return(time.Unix(1001, 0))
	timer1 := mock.NewMockTimer(ctrl)
	clockO.EXPECT().NewTimer(time.Minute).Return(timer1, nil)
	uuidGenerator.EXPECT().Call().Return(uuid.Parse("11111111-1111-1111-1111-111111111111"))

	ctx1, cancel1 := context.WithCancel(ctx)
	stream1, err := execClientO.Execute(ctx1, &remoteexecution.ExecuteRequest{
		InstanceName: "main",
		ActionDigest: digest1,
	})
	require.NoError(t, err)
	_, err = stream1.Recv()
	require.NoError(t, err)

	// Start task on worker-dead.
	clockO.EXPECT().Now().Return(time.Unix(1002, 0))
	timer1.EXPECT().Stop().Return(true)
	clockO.EXPECT().Now().Return(time.Unix(1002, 0))
	timer1Exec := mock.NewMockTimer(ctrl)
	clockO.EXPECT().NewTimer(time.Minute).Return(timer1Exec, nil)

	_, err = bqO.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId:           map[string]string{"hostname": "worker-dead"},
		InstanceNamePrefix: "main",
		Platform:           platformForTesting,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	})
	require.NoError(t, err)
	_, err = stream1.Recv()
	require.NoError(t, err)

	timer1Exec.EXPECT().Stop().Return(true)
	clockO.EXPECT().Now().Return(time.Unix(1003, 0))
	cancel1()
	_, err = stream1.Recv()
	require.Error(t, err)

	clockO.EXPECT().Now().Return(time.Unix(1003, 0))
	snap, err := bqO.Snapshot()
	require.NoError(t, err)

	// Restore onto bqN at t=1003.
	clockN := mock.NewMockClock(ctrl)
	clockN.EXPECT().Now().Return(time.Unix(1003, 0))
	bqN := scheduler.NewInMemoryBuildQueue(
		actionReader,
		clockN,
		uuidGenerator.Call,
		&config,
		actionRouter,
		allowAllAuthorizer,
		allowAllAuthorizer,
		allowAllAuthorizer,
		allowAllAuthorizer,
	)
	clockN.EXPECT().Now().Return(time.Unix(1003, 0))
	require.NoError(t, bqN.RegisterPredeclaredPlatformQueue(
		util.Must(digest.NewInstanceName("main")),
		platformForTesting,
		nil,
		0,
		0,
		[]uint32{0},
	))
	clockN.EXPECT().Now().Return(time.Unix(1003, 0))
	require.NoError(t, bqN.RestoreState(snap))

	// Advance clock past WorkerAdoptionTimeout (1003 + 15 = 1018).
	// Instead of failing the operation with codes.Unavailable, bqN must re-enqueue the task!
	clockN.EXPECT().Now().Return(time.Unix(1019, 0))
	opResp, err := bqN.GetOperation(ctx, &buildqueuestate.GetOperationRequest{
		OperationName: "11111111-1111-1111-1111-111111111111",
	})
	require.NoError(t, err)
	require.NotNil(t, opResp.Operation.GetQueued())
}
