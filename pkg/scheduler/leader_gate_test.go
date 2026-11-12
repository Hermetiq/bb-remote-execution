package scheduler_test

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/schedulerhandover"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/platform"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.uber.org/mock/gomock"
)

func startGateServer(t *testing.T, gate *scheduler.LeaderGate) (string, func(string) (grpc.ClientConnInterface, error)) {
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	remoteexecution.RegisterExecutionServer(srv, gate)
	remoteworker.RegisterOperationQueueServer(srv, gate)
	buildqueuestate.RegisterBuildQueueStateServer(srv, gate)
	schedulerhandover.RegisterHandoverServer(srv, scheduler.NewHandoverServer(gate))
	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(func() {
		srv.Stop()
	})
	dialer := func(string) (grpc.ClientConnInterface, error) {
		return grpc.Dial(
			"bufnet",
			grpc.WithDialer(func(string, time.Duration) (net.Conn, error) {
				return lis.Dial()
			}),
			grpc.WithInsecure(),
		)
	}
	return "bufnet", dialer
}

func TestLeaderGateZeroDowntimeHandoverAndRelay(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	actionReader := mock.NewMockReader[*remoteexecution.Action](ctrl)
	actionRouter := mock.NewMockActionRouter(ctrl)

	config := buildQueueConfigurationForTesting
	config.WorkerAdoptionTimeout = 15 * time.Second
	config.RecoveredOperationWithNoWaitersTimeout = 30 * time.Second

	bqO := scheduler.NewInMemoryBuildQueue(
		actionReader,
		clock.SystemClock,
		uuid.NewRandom,
		&config,
		actionRouter,
		allowAllAuthorizer,
		allowAllAuthorizer,
		allowAllAuthorizer,
		allowAllAuthorizer,
	)
	require.NoError(t, bqO.RegisterPredeclaredPlatformQueue(
		util.Must(digest.NewInstanceName("main")),
		platformForTesting,
		nil,
		0,
		0,
		[]uint32{0},
	))

	bqN := scheduler.NewInMemoryBuildQueue(
		actionReader,
		clock.SystemClock,
		uuid.NewRandom,
		&config,
		actionRouter,
		allowAllAuthorizer,
		allowAllAuthorizer,
		allowAllAuthorizer,
		allowAllAuthorizer,
	)
	require.NoError(t, bqN.RegisterPredeclaredPlatformQueue(
		util.Must(digest.NewInstanceName("main")),
		platformForTesting,
		nil,
		0,
		0,
		[]uint32{0},
	))

	var dialersMu sync.Mutex
	dialers := map[string]func(string) (grpc.ClientConnInterface, error){}
	multiDialer := func(endpoint string) (grpc.ClientConnInterface, error) {
		dialersMu.Lock()
		d := dialers[endpoint]
		dialersMu.Unlock()
		return d(endpoint)
	}

	endpointsO := &schedulerhandover.LeaderEndpoints{
		LeaderId:         "scheduler-o",
		ClientEndpoint:   "scheduler-o",
		WorkerEndpoint:   "scheduler-o",
		StateEndpoint:    "scheduler-o",
		HandoverEndpoint: "scheduler-o",
	}
	endpointsN := &schedulerhandover.LeaderEndpoints{
		LeaderId:         "scheduler-n",
		ClientEndpoint:   "scheduler-n",
		WorkerEndpoint:   "scheduler-n",
		StateEndpoint:    "scheduler-n",
		HandoverEndpoint: "scheduler-n",
	}

	gateO := scheduler.NewLeaderGate(bqO, multiDialer, scheduler.GateRoleLeader, endpointsO)
	gateN := scheduler.NewLeaderGate(bqN, multiDialer, scheduler.GateRoleFollower, endpointsO)

	_, dO := startGateServer(t, gateO)
	_, dN := startGateServer(t, gateN)
	dialersMu.Lock()
	dialers["scheduler-o"] = dO
	dialers["scheduler-n"] = dN
	dialersMu.Unlock()

	connO, err := multiDialer("scheduler-o")
	require.NoError(t, err)
	execClientO := remoteexecution.NewExecutionClient(connO)
	workerClientO := remoteworker.NewOperationQueueClient(connO)

	action := &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "1111111111111111111111111111111111111111",
			SizeBytes: 100,
		},
	}
	actionDigest := &remoteexecution.Digest{
		Hash:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SizeBytes: 123,
	}
	actionReader.EXPECT().Read(
		gomock.Any(),
		digest.MustNewDigest("main", remoteexecution.DigestFunction_SHA1, actionDigest.Hash, actionDigest.SizeBytes),
	).Return(action, nil)
	selector := mock.NewMockSelector(ctrl)
	learner := mock.NewMockLearner(ctrl)
	actionRouter.EXPECT().RouteAction(gomock.Any(), gomock.Any(), testutil.EqProto(t, action), nil).Return(
		action,
		platform.MustNewKey("main", platformForTesting),
		nil,
		selector,
		nil,
	)
	selector.EXPECT().Select([]uint32{0}).Return(0, 10*time.Minute, 20*time.Minute, learner)

	// Client opens an Execute stream against O (and holds it open across handover).
	stream, err := execClientO.Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "main",
		ActionDigest: actionDigest,
	})
	require.NoError(t, err)

	opQueued, err := stream.Recv()
	require.NoError(t, err)
	require.False(t, opQueued.Done)

	// Worker synchronizes with O and starts executing the action.
	syncResp, err := workerClientO.Synchronize(ctx, &remoteworker.SynchronizeRequest{
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

	opExecuting, err := stream.Recv()
	require.NoError(t, err)
	require.False(t, opExecuting.Done)
	require.Equal(t, opQueued.Name, opExecuting.Name)

	// Execute blue/green handover from O to N while the client's Execute stream is open on O!
	require.NoError(t, scheduler.ExecuteTakeOver(
		ctx,
		gateN,
		multiDialer,
		nil,
		endpointsN,
		endpointsO,
		1,
	))
	require.Equal(t, scheduler.GateRoleRetired, gateO.Role())
	require.Equal(t, scheduler.GateRoleLeader, gateN.Role())

	// Worker sends its completion update to O (which is now RETIRED and forwards to N!).
	_, err = workerClientO.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId:           map[string]string{"hostname": "worker-1"},
		InstanceNamePrefix: "main",
		Platform:           platformForTesting,
		PreferBeingIdle:    true,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: actionDigest,
					ExecutionState: &remoteworker.CurrentState_Executing_Completed{
						Completed: &remoteexecution.ExecuteResponse{
							Result: &remoteexecution.ActionResult{
								ExitCode: 42,
								ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
									VirtualExecutionDuration: durationpb.New(3 * time.Second),
								},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Client's original Execute stream on O receives the final COMPLETED operation relayed from N!
	var finalOp *remoteexecution.ExecuteResponse
	for {
		op, err := stream.Recv()
		require.NoError(t, err)
		if op.Done {
			var resp remoteexecution.ExecuteResponse
			require.NoError(t, op.GetResponse().UnmarshalTo(&resp))
			finalOp = &resp
			break
		}
	}
	require.NotNil(t, finalOp)
	require.Equal(t, int32(42), finalOp.GetResult().GetExitCode())

	_, err = stream.Recv()
	require.Equal(t, io.EOF, err)
}

func TestLeaderGateAbortHandoverResumesLocally(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	actionReader := mock.NewMockReader[*remoteexecution.Action](ctrl)
	actionRouter := mock.NewMockActionRouter(ctrl)

	config := buildQueueConfigurationForTesting
	bqO := scheduler.NewInMemoryBuildQueue(
		actionReader,
		clock.SystemClock,
		uuid.NewRandom,
		&config,
		actionRouter,
		allowAllAuthorizer,
		allowAllAuthorizer,
		allowAllAuthorizer,
		allowAllAuthorizer,
	)
	require.NoError(t, bqO.RegisterPredeclaredPlatformQueue(
		util.Must(digest.NewInstanceName("main")),
		platformForTesting,
		nil,
		0,
		0,
		[]uint32{0},
	))

	endpointsO := &schedulerhandover.LeaderEndpoints{
		LeaderId:       "scheduler-o",
		ClientEndpoint: "scheduler-o",
	}
	gateO := scheduler.NewLeaderGate(bqO, nil, scheduler.GateRoleLeader, endpointsO)
	_, dialO := startGateServer(t, gateO)
	connO, err := dialO("scheduler-o")
	require.NoError(t, err)
	execClientO := remoteexecution.NewExecutionClient(connO)
	workerClientO := remoteworker.NewOperationQueueClient(connO)

	action := &remoteexecution.Action{
		CommandDigest: &remoteexecution.Digest{
			Hash:      "1111111111111111111111111111111111111111",
			SizeBytes: 100,
		},
	}
	actionDigest := &remoteexecution.Digest{
		Hash:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SizeBytes: 123,
	}
	actionReader.EXPECT().Read(
		gomock.Any(),
		digest.MustNewDigest("main", remoteexecution.DigestFunction_SHA1, actionDigest.Hash, actionDigest.SizeBytes),
	).Return(action, nil)
	selector := mock.NewMockSelector(ctrl)
	learner := mock.NewMockLearner(ctrl)
	actionRouter.EXPECT().RouteAction(gomock.Any(), gomock.Any(), testutil.EqProto(t, action), nil).Return(
		action,
		platform.MustNewKey("main", platformForTesting),
		nil,
		selector,
		nil,
	)
	selector.EXPECT().Select([]uint32{0}).Return(0, 10*time.Minute, 20*time.Minute, learner)
	learner.EXPECT().Succeeded(gomock.Any(), []uint32{0}).Return(0, time.Duration(0), time.Duration(0), nil)

	stream, err := execClientO.Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "main",
		ActionDigest: actionDigest,
	})
	require.NoError(t, err)
	opQueued, err := stream.Recv()
	require.NoError(t, err)
	require.False(t, opQueued.Done)

	// BeginHandover interrupts the local Execute call, then AbortHandover resumes it locally via WaitExecution!
	require.NoError(t, gateO.BeginHandover())
	gateO.AbortHandover()
	require.Equal(t, scheduler.GateRoleLeader, gateO.Role())

	// Worker executes and completes the task locally on O.
	_, err = workerClientO.Synchronize(ctx, &remoteworker.SynchronizeRequest{
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

	_, err = workerClientO.Synchronize(ctx, &remoteworker.SynchronizeRequest{
		WorkerId:           map[string]string{"hostname": "worker-1"},
		InstanceNamePrefix: "main",
		Platform:           platformForTesting,
		PreferBeingIdle:    true,
		CurrentState: &remoteworker.CurrentState{
			WorkerState: &remoteworker.CurrentState_Executing_{
				Executing: &remoteworker.CurrentState_Executing{
					ActionDigest: actionDigest,
					ExecutionState: &remoteworker.CurrentState_Executing_Completed{
						Completed: &remoteexecution.ExecuteResponse{
							Result: &remoteexecution.ActionResult{
								ExitCode: 0,
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	for {
		op, err := stream.Recv()
		require.NoError(t, err)
		if op.Done {
			break
		}
	}
	_, err = stream.Recv()
	require.Equal(t, io.EOF, err)
}
