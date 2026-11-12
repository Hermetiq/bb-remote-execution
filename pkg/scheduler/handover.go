package scheduler

import (
	"bytes"
	"context"
	"log"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/coordinator"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/schedulerhandover"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/schedulerstate"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const snapshotChunkSizeBytes = 1024 * 1024

// HandoverServer implements schedulerhandover.HandoverServer for the
// active leader scheduler instance.
type HandoverServer struct {
	schedulerhandover.UnimplementedHandoverServer
	gate              *LeaderGate
	coordinatorClient coordinator.CoordinatorClient
}

// NewHandoverServer creates a new HandoverServer backed by gate.
func NewHandoverServer(gate *LeaderGate, coordinatorClients ...coordinator.CoordinatorClient) *HandoverServer {
	s := &HandoverServer{gate: gate}
	if len(coordinatorClients) > 0 {
		s.coordinatorClient = coordinatorClients[0]
	}
	return s
}

// Ping verifies that the underlying InMemoryBuildQueue lock can be
// acquired and released promptly.
func (s *HandoverServer) Ping(ctx context.Context, req *schedulerhandover.PingRequest) (*schedulerhandover.PingResponse, error) {
	s.gate.Ping()
	return &schedulerhandover.PingResponse{}, nil
}

// TakeOver freezes the active leader's build queue, streams a snapshot
// to the candidate, transfers leadership after restoration is acknowledged,
// and transitions the old leader to GateRoleRetired.
func (s *HandoverServer) TakeOver(stream schedulerhandover.Handover_TakeOverServer) error {
	beginMsg, err := stream.Recv()
	if err != nil {
		return util.StatusWrap(err, "Failed to receive BeginRequest")
	}
	candidate := beginMsg.GetBegin().GetCandidate()
	if candidate.GetLeaderId() == "" || candidate.GetClientEndpoint() == "" || candidate.GetWorkerEndpoint() == "" || candidate.GetStateEndpoint() == "" || candidate.GetHandoverEndpoint() == "" {
		return status.Error(codes.InvalidArgument, "Candidate endpoints must be provided in BeginRequest")
	}

	if err := s.gate.BeginHandoverContext(stream.Context()); err != nil {
		return err
	}
	mayAbort, transferred := true, false
	defer func() {
		if transferred {
			s.gate.CommitHandover(candidate)
		} else if mayAbort {
			s.gate.AbortHandover()
		}
	}()

	snap, err := s.gate.BuildQueue().Snapshot()
	if err != nil {
		return util.StatusWrap(err, "Failed to snapshot build queue state")
	}
	snapBytes, err := proto.Marshal(snap)
	if err != nil {
		return util.StatusWrap(err, "Failed to marshal build queue snapshot")
	}

	var tasksCount, opsCount uint32
	for _, pq := range snap.GetPlatformQueues() {
		for _, scq := range pq.GetSizeClassQueues() {
			tasksCount += uint32(len(scq.GetTasks()))
			for _, t := range scq.GetTasks() {
				opsCount += uint32(len(t.GetOperations()))
			}
		}
	}

	if len(snapBytes) == 0 {
		if err := stream.Send(&schedulerhandover.HandoverLeaderToClient{
			Message: &schedulerhandover.HandoverLeaderToClient_SnapshotChunk_{
				SnapshotChunk: &schedulerhandover.HandoverLeaderToClient_SnapshotChunk{
					IsLast:          true,
					TasksCount:      tasksCount,
					OperationsCount: opsCount,
				},
			},
		}); err != nil {
			return util.StatusWrap(err, "Failed to send empty snapshot chunk")
		}
	} else {
		for offset := 0; offset < len(snapBytes); offset += snapshotChunkSizeBytes {
			end := offset + snapshotChunkSizeBytes
			isLast := false
			if end >= len(snapBytes) {
				end = len(snapBytes)
				isLast = true
			}
			if err := stream.Send(&schedulerhandover.HandoverLeaderToClient{
				Message: &schedulerhandover.HandoverLeaderToClient_SnapshotChunk_{
					SnapshotChunk: &schedulerhandover.HandoverLeaderToClient_SnapshotChunk{
						Data:            snapBytes[offset:end],
						IsLast:          isLast,
						TasksCount:      tasksCount,
						OperationsCount: opsCount,
					},
				},
			}); err != nil {
				return util.StatusWrap(err, "Failed to send snapshot chunk")
			}
		}
	}

	ackMsg, err := stream.Recv()
	if err != nil {
		return util.StatusWrap(err, "Failed to receive RestoredAck from candidate")
	}
	ack := ackMsg.GetRestoredAck()
	if ack == nil || !ack.GetSuccess() {
		return status.Errorf(codes.Aborted, "Candidate failed to restore snapshot: %s", ack.GetError())
	}

	// Only the old leader may change the Lease. Once a write has been
	// attempted, transport failure is not evidence that it did not commit.
	mayAbort = false
	epoch := beginMsg.GetBegin().GetExpectedEpoch() + 1
	if s.coordinatorClient != nil {
		transferCtx, cancel := context.WithTimeout(context.WithoutCancel(stream.Context()), 10*time.Second)
		result, transferErr := s.coordinatorClient.TransferLeadership(transferCtx, &coordinator.TransferLeadershipRequest{
			Target: candidate, ExpectedEpoch: beginMsg.GetBegin().GetExpectedEpoch(),
		})
		cancel()
		if transferErr != nil {
			resolveCtx, resolveCancel := context.WithTimeout(context.WithoutCancel(stream.Context()), 5*time.Second)
			current, resolveErr := s.coordinatorClient.GetLeadership(resolveCtx, &coordinator.GetLeadershipRequest{FencePendingTransfer: true})
			resolveCancel()
			if resolveErr == nil && current.GetLeader().GetLeaderId() == candidate.GetLeaderId() && current.GetLeaseEpoch() > beginMsg.GetBegin().GetExpectedEpoch() {
				epoch = current.GetLeaseEpoch()
			} else if resolveErr == nil && current.GetRole() == coordinator.Role_LEADER && current.GetLeaseEpoch() == beginMsg.GetBegin().GetExpectedEpoch() {
				mayAbort = true
				return util.StatusWrap(transferErr, "Leadership transfer did not commit")
			} else {
				err := util.StatusWrap(transferErr, "Cannot safely resolve leadership transfer; fencing scheduler")
				s.gate.Fence(err)
				return err
			}
		} else {
			epoch = result.GetNewEpoch()
		}
	}
	transferred = true
	if err := stream.Send(&schedulerhandover.HandoverLeaderToClient{
		Message: &schedulerhandover.HandoverLeaderToClient_LeadershipTransferred_{
			LeadershipTransferred: &schedulerhandover.HandoverLeaderToClient_LeadershipTransferred{Epoch: epoch},
		},
	}); err != nil {
		return util.StatusWrap(err, "Failed to send leadership transfer result")
	}
	// Keep callers parked until the candidate has enabled its restored queue.
	// A lost acknowledgement still retires O: the Lease already belongs to N.
	ackMsg, err = stream.Recv()
	if err != nil {
		return util.StatusWrap(err, "Failed to receive activation acknowledgement")
	}
	if !ackMsg.GetCommit().GetCommitted() {
		return status.Error(codes.Aborted, "Candidate did not acknowledge activation")
	}

	return nil
}

// ExecuteTakeOver connects from candidate N to current leader O,
// pulls and restores the build queue snapshot, waits for O to transfer
// leadership, and transitions gate to GateRoleLeader. coordinatorClient
// resolves the authoritative outcome if O's final response is lost.
func ExecuteTakeOver(
	ctx context.Context,
	gate *LeaderGate,
	dialer EndpointDialer,
	coordinatorClient coordinator.CoordinatorClient,
	selfEndpoints *schedulerhandover.LeaderEndpoints,
	leaderEndpoints *schedulerhandover.LeaderEndpoints,
	leaseEpoch uint64,
) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if leaderEndpoints == nil || leaderEndpoints.GetHandoverEndpoint() == "" {
		return status.Error(codes.InvalidArgument, "Current leader handover endpoint is empty")
	}
	conn, err := dialer(leaderEndpoints.GetHandoverEndpoint())
	if err != nil {
		return util.StatusWrapf(err, "Failed to dial leader handover endpoint %#v", leaderEndpoints.GetHandoverEndpoint())
	}
	handoverClient := schedulerhandover.NewHandoverClient(conn)

	stream, err := handoverClient.TakeOver(ctx)
	if err != nil {
		return util.StatusWrap(err, "Failed to start TakeOver stream")
	}
	defer func() {
		_ = stream.CloseSend()
	}()

	if err := stream.Send(&schedulerhandover.HandoverClientToLeader{
		Message: &schedulerhandover.HandoverClientToLeader_Begin{
			Begin: &schedulerhandover.HandoverClientToLeader_BeginRequest{
				Candidate:     selfEndpoints,
				ExpectedEpoch: leaseEpoch,
			},
		},
	}); err != nil {
		return util.StatusWrap(err, "Failed to send BeginRequest")
	}

	var buf bytes.Buffer
	for {
		msg, err := stream.Recv()
		if err != nil {
			return util.StatusWrap(err, "Failed to receive SnapshotChunk")
		}
		chunk := msg.GetSnapshotChunk()
		if chunk == nil {
			return status.Error(codes.Internal, "Unexpected message from leader during snapshot stream")
		}
		buf.Write(chunk.GetData())
		if chunk.GetIsLast() {
			break
		}
	}

	var snap schedulerstate.Snapshot
	if err := proto.Unmarshal(buf.Bytes(), &snap); err != nil {
		_ = stream.Send(&schedulerhandover.HandoverClientToLeader{
			Message: &schedulerhandover.HandoverClientToLeader_RestoredAck_{
				RestoredAck: &schedulerhandover.HandoverClientToLeader_RestoredAck{
					Success: false,
					Error:   err.Error(),
				},
			},
		})
		return util.StatusWrap(err, "Failed to unmarshal snapshot")
	}

	if err := gate.BuildQueue().RestoreState(&snap); err != nil {
		_ = stream.Send(&schedulerhandover.HandoverClientToLeader{
			Message: &schedulerhandover.HandoverClientToLeader_RestoredAck_{
				RestoredAck: &schedulerhandover.HandoverClientToLeader_RestoredAck{
					Success: false,
					Error:   err.Error(),
				},
			},
		})
		return util.StatusWrap(err, "Failed to restore snapshot state")
	}

	if err := stream.Send(&schedulerhandover.HandoverClientToLeader{
		Message: &schedulerhandover.HandoverClientToLeader_RestoredAck_{
			RestoredAck: &schedulerhandover.HandoverClientToLeader_RestoredAck{
				Success: true,
			},
		},
	}); err != nil {
		return util.StatusWrap(err, "Failed to send RestoredAck")
	}

	msg, recvErr := stream.Recv()
	if recvErr != nil || msg.GetLeadershipTransferred() == nil || msg.GetLeadershipTransferred().GetEpoch() <= leaseEpoch {
		// The old leader may have committed but lost its response. Only a fresh
		// authoritative read can permit serving the restored snapshot.
		if coordinatorClient == nil {
			return status.Errorf(codes.Unavailable, "Failed to confirm leadership transfer: %v", recvErr)
		}
		resolveCtx, resolveCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		current, resolveErr := coordinatorClient.GetLeadership(resolveCtx, &coordinator.GetLeadershipRequest{})
		resolveCancel()
		if resolveErr != nil || current.GetLeader().GetLeaderId() != selfEndpoints.GetLeaderId() || current.GetLeaseEpoch() <= leaseEpoch {
			return status.Errorf(codes.Unavailable, "Leadership transfer was not confirmed: stream=%v, coordinator=%v", recvErr, resolveErr)
		}
	}
	gate.SetLeader(selfEndpoints)
	// Failure to acknowledge cannot undo a committed Lease transfer. O will
	// retire on EOF, and its Watch independently observes the new holder.
	_ = stream.Send(&schedulerhandover.HandoverClientToLeader{
		Message: &schedulerhandover.HandoverClientToLeader_Commit{
			Commit: &schedulerhandover.HandoverClientToLeader_CommitNotification{Committed: true},
		},
	})
	_ = stream.CloseSend()
	_, _ = stream.Recv()

	return nil
}

// RunCoordinatorLoop watches role updates from coordinatorClient, drives
// pull and push handovers on gate, invokes onReady once the initial role
// is established, and drains open relays upon ctx cancellation (SIGTERM).
func RunCoordinatorLoop(
	ctx context.Context,
	gate *LeaderGate,
	dialer EndpointDialer,
	coordinatorClient coordinator.CoordinatorClient,
	selfEndpoints *schedulerhandover.LeaderEndpoints,
	relayDrainPeriod time.Duration,
	onReady func(),
) error {
	// Keep the role watch alive through SIGTERM handover and relay draining.
	// The coordinator is a dependency and remains running until we return.
	watchCtx, watchCancel := context.WithCancel(context.WithoutCancel(ctx))
	defer watchCancel()
	stream, err := coordinatorClient.Watch(watchCtx, &coordinator.WatchRequest{Self: selfEndpoints})
	if err != nil {
		gate.Fence(err)
		return util.StatusWrap(err, "Failed to start coordinator Watch")
	}
	type update struct {
		response *coordinator.WatchResponse
		err      error
	}
	updates := make(chan update)
	go func() {
		for {
			response, err := stream.Recv()
			select {
			case updates <- update{response, err}:
			case <-watchCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	var ready, hasLed, takingOver, shuttingDown bool
	var takeOverEpoch uint64
	var takeoverResult <-chan error
	var shutdownResult <-chan error
	shutdownSignal := ctx.Done()
	startDrain := func() {
		done := make(chan error, 1)
		shutdownResult = done
		go func() { done <- drainScheduler(gate, coordinatorClient, selfEndpoints, relayDrainPeriod) }()
	}
	markReady := func() {
		if !ready && !shuttingDown {
			ready = true
			if onReady != nil {
				onReady()
			}
		}
	}
	fail := func(err error) error { gate.Fence(err); return err }
	for {
		select {
		case <-gate.failure:
			return gate.failureError()
		case <-shutdownSignal:
			shutdownSignal = nil
			shuttingDown = true
			if !takingOver {
				startDrain()
			}
		case err := <-shutdownResult:
			return err
		case err := <-takeoverResult:
			takeoverResult = nil
			takingOver = false
			if err != nil {
				// RestoreState mutates the candidate queue. Restart after a failed
				// attempt instead of ever restoring a second snapshot over it.
				return fail(util.StatusWrap(err, "Scheduler takeover failed"))
			}
			hasLed = true
			markReady()
			if shuttingDown {
				startDrain()
			}
		case next := <-updates:
			if next.err != nil {
				return fail(util.StatusWrap(next.err, "Coordinator Watch lost; fencing scheduler"))
			}
			resp := next.response
			switch resp.GetRole() {
			case coordinator.Role_LEADER:
				if takingOver || gate.Role() == GateRoleHandingOver {
					continue
				}
				if hasLed && gate.Role() != GateRoleLeader {
					return fail(status.Error(codes.FailedPrecondition, "Cannot regain leadership with stale queue state"))
				}
				gate.SetLeader(selfEndpoints)
				hasLed = true
				markReady()
			case coordinator.Role_FOLLOWER:
				if resp.GetLeader().GetClientEndpoint() == "" {
					return fail(status.Error(codes.Unavailable, "Coordinator fenced this scheduler"))
				}
				if takingOver {
					if resp.GetLeaseEpoch() > takeOverEpoch && resp.GetLeader().GetLeaderId() != selfEndpoints.GetLeaderId() {
						return fail(status.Error(codes.Aborted, "Leadership changed during takeover"))
					}
					continue
				}
				// The handover server releases parked calls after N acknowledges
				// activation. A Watch update must not release them prematurely.
				if gate.Role() != GateRoleHandingOver && gate.Role() != GateRoleRetired {
					gate.SetFollower(resp.GetLeader())
					markReady()
				}
			case coordinator.Role_TAKE_OVER:
				if takingOver || shuttingDown {
					continue
				}
				if hasLed {
					return fail(status.Error(codes.FailedPrecondition, "Cannot restore into a former leader's queue"))
				}
				takingOver = true
				takeOverEpoch = resp.GetLeaseEpoch()
				done := make(chan error, 1)
				takeoverResult = done
				go func() {
					done <- ExecuteTakeOver(watchCtx, gate, dialer, coordinatorClient, selfEndpoints, resp.GetLeader(), resp.GetLeaseEpoch())
				}()
			default:
				return fail(status.Error(codes.Unavailable, "Coordinator returned an unknown role"))
			}
		}
	}
}

func drainScheduler(gate *LeaderGate, client coordinator.CoordinatorClient, self *schedulerhandover.LeaderEndpoints, period time.Duration) error {
	if period <= 0 {
		period = 29 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), period)
	defer cancel()
	// A single-replica deployment may need time for its replacement to
	// appear. Keep requesting a peer while leadership is still ours.
	for gate.Role() == GateRoleLeader || gate.Role() == GateRoleHandingOver {
		if gate.Role() == GateRoleLeader {
			requestCtx, requestCancel := context.WithTimeout(ctx, 5*time.Second)
			_, err := client.RequestHandover(requestCtx, &coordinator.RequestHandoverRequest{SelfId: self.GetLeaderId()})
			requestCancel()
			if err != nil && ctx.Err() == nil {
				log.Printf("Waiting for shutdown handover peer: %v", err)
			}
		}
		select {
		case <-gate.failure:
			return gate.failureError()
		case <-ctx.Done():
			return nil
		case <-time.After(100 * time.Millisecond):
		}
	}
	if err := gate.WaitForRelays(ctx); err != nil {
		log.Printf("Relay drain period expired: %v", err)
	}
	return nil
}
