package scheduler

import (
	"context"
	"io"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/schedulerhandover"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
)

// GateRole represents the current leadership state of a LeaderGate.
type GateRole int

const (
	// GateRoleFollower forwards all incoming RPCs to the current leader.
	GateRoleFollower GateRole = iota
	// GateRoleLeader serves all incoming RPCs locally against InMemoryBuildQueue.
	GateRoleLeader
	// GateRoleHandingOver freezes local execution, interrupts in-flight local calls,
	// and parks callers while a point-in-time snapshot is transferred to a successor.
	GateRoleHandingOver
	// GateRoleRetired forwards new RPCs and relays interrupted Execute/WaitExecution
	// streams via WaitExecution(operationName) to the successor leader.
	GateRoleRetired
	// GateRoleFenced rejects calls until the process exits. A scheduler that
	// lost its coordinator must never revive its stale in-memory state.
	GateRoleFenced
)

// EndpointDialer dials a peer scheduler gRPC endpoint address.
type EndpointDialer func(endpoint string) (grpc.ClientConnInterface, error)

// LeaderGate wraps an InMemoryBuildQueue to provide role-based request
// gating, transparent follower/retired RPC forwarding, and zero-downtime
// Execute/WaitExecution stream relaying across blue/green handovers.
type LeaderGate struct {
	capabilities.Provider

	bq     *InMemoryBuildQueue
	dialer EndpointDialer

	mu                   sync.Mutex
	cond                 *sync.Cond
	role                 GateRole
	leaderEndpoints      *schedulerhandover.LeaderEndpoints
	conns                map[string]grpc.ClientConnInterface
	inFlightLocalCalls   int
	activeLocalCancels   map[*context.CancelFunc]struct{}
	activeRelays         int
	activeExecutionCalls int
	failure              chan struct{}
	failureErr           error
}

var (
	_ builder.BuildQueue                    = (*LeaderGate)(nil)
	_ remoteworker.OperationQueueServer     = (*LeaderGate)(nil)
	_ buildqueuestate.BuildQueueStateServer = (*LeaderGate)(nil)
)

// NewLeaderGate creates a new LeaderGate wrapping bq, starting in the
// initialRole state.
func NewLeaderGate(bq *InMemoryBuildQueue, dialer EndpointDialer, initialRole GateRole, initialLeader *schedulerhandover.LeaderEndpoints) *LeaderGate {
	g := &LeaderGate{
		Provider:           bq,
		bq:                 bq,
		dialer:             dialer,
		role:               initialRole,
		leaderEndpoints:    initialLeader,
		conns:              map[string]grpc.ClientConnInterface{},
		activeLocalCancels: map[*context.CancelFunc]struct{}{},
		failure:            make(chan struct{}),
	}
	g.cond = sync.NewCond(&g.mu)
	return g
}

// BuildQueue returns the underlying InMemoryBuildQueue.
func (g *LeaderGate) BuildQueue() *InMemoryBuildQueue {
	return g.bq
}

// RunMaintenance serializes background mutations with role transitions.
// Frozen snapshots and retired queues must not be changed by reloads or
// periodic cleanup callbacks.
func (g *LeaderGate) RunMaintenance(fn func()) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.role != GateRoleLeader {
		return false
	}
	fn()
	return true
}

// Ping checks lock responsiveness without running cleanup callbacks on
// frozen state. Cleanup is driven by leader RPCs and RunMaintenance.
func (g *LeaderGate) Ping() {
	g.bq.lock.Lock()
	g.bq.lock.Unlock()
}

// Role returns the gate's current GateRole.
func (g *LeaderGate) Role() GateRole {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.role
}

// SetLeader transitions the gate into GateRoleLeader.
func (g *LeaderGate) SetLeader(selfEndpoints *schedulerhandover.LeaderEndpoints) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.role == GateRoleFenced {
		return
	}
	g.role = GateRoleLeader
	g.leaderEndpoints = selfEndpoints
	g.cond.Broadcast()
}

// SetFollower transitions the gate into GateRoleFollower pointing at
// leaderEndpoints. If the gate was previously LEADER, any in-flight
// local calls are interrupted so they forward to the new leader.
func (g *LeaderGate) SetFollower(leaderEndpoints *schedulerhandover.LeaderEndpoints) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.role == GateRoleFenced {
		return
	}
	g.role = GateRoleFollower
	g.leaderEndpoints = leaderEndpoints
	for cancelPtr := range g.activeLocalCancels {
		(*cancelPtr)()
	}
	g.cond.Broadcast()
}

// Fence permanently prevents this process from serving local state.
func (g *LeaderGate) Fence(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.role == GateRoleFenced {
		return
	}
	g.role = GateRoleFenced
	g.leaderEndpoints = nil
	g.failureErr = err
	close(g.failure)
	for cancelPtr := range g.activeLocalCancels {
		(*cancelPtr)()
	}
	g.cond.Broadcast()
}

func (g *LeaderGate) failureError() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.failureErr
}

// BeginHandover transitions the gate from GateRoleLeader to
// GateRoleHandingOver, interrupts all in-flight local calls, and blocks
// until every in-flight local call has exited InMemoryBuildQueue.
func (g *LeaderGate) BeginHandover() error {
	return g.BeginHandoverContext(context.Background())
}

// BeginHandoverContext bounds quiescence by the takeover's lifetime.
func (g *LeaderGate) BeginHandoverContext(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if ctx.Err() != nil {
		return util.StatusFromContext(ctx)
	}
	if g.role != GateRoleLeader {
		return status.Error(codes.FailedPrecondition, "Scheduler is not the active leader")
	}
	g.role = GateRoleHandingOver
	for cancelPtr := range g.activeLocalCancels {
		(*cancelPtr)()
	}
	stop := context.AfterFunc(ctx, func() {
		g.mu.Lock()
		g.cond.Broadcast()
		g.mu.Unlock()
	})
	defer stop()
	for g.inFlightLocalCalls > 0 {
		if ctx.Err() != nil {
			if g.role == GateRoleHandingOver {
				g.role = GateRoleLeader
				g.cond.Broadcast()
			}
			return util.StatusFromContext(ctx)
		}
		g.cond.Wait()
	}
	if g.role != GateRoleHandingOver {
		return status.Error(codes.Unavailable, "Leadership changed while quiescing")
	}
	return nil
}

// AbortHandover returns the gate from GateRoleHandingOver to
// GateRoleLeader and wakes all parked callers so they resume locally.
func (g *LeaderGate) AbortHandover() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.role == GateRoleHandingOver {
		g.role = GateRoleLeader
		g.cond.Broadcast()
	}
}

// CommitHandover transitions the gate from GateRoleHandingOver to
// GateRoleRetired pointing at newLeader and wakes all parked callers
// and interrupted streams so they relay to newLeader.
func (g *LeaderGate) CommitHandover(newLeader *schedulerhandover.LeaderEndpoints) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.role == GateRoleFenced {
		return
	}
	g.leaderEndpoints = newLeader
	g.role = GateRoleRetired
	g.cond.Broadcast()
}

// WaitForRelays blocks until all active relayed streams have completed
// or ctx is canceled.
func (g *LeaderGate) WaitForRelays(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			g.mu.Lock()
			g.cond.Broadcast()
			g.mu.Unlock()
		case <-done:
		}
	}()
	defer close(done)

	g.mu.Lock()
	defer g.mu.Unlock()
	for g.activeExecutionCalls > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		g.cond.Wait()
	}
	return nil
}

func (g *LeaderGate) getConnLocked(endpoint string) (grpc.ClientConnInterface, error) {
	if endpoint == "" {
		return nil, status.Error(codes.Unavailable, "No scheduler leader endpoint available")
	}
	if conn, ok := g.conns[endpoint]; ok {
		return conn, nil
	}
	if g.dialer == nil {
		return nil, status.Error(codes.Unavailable, "No endpoint dialer configured on LeaderGate")
	}
	conn, err := g.dialer(endpoint)
	if err != nil {
		return nil, util.StatusWrapf(err, "Failed to dial leader endpoint %#v", endpoint)
	}
	g.conns[endpoint] = conn
	return conn, nil
}

func forwardIncomingMetadata(ctx context.Context) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		return metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}

// waitWhileHandingOver blocks while g.role == GateRoleHandingOver or until ctx is done.
// Must be called with g.mu held.
func (g *LeaderGate) waitWhileHandingOverLocked(ctx context.Context) error {
	if g.role == GateRoleFenced {
		return status.Error(codes.Unavailable, "Scheduler leadership has been fenced")
	}
	if g.role != GateRoleHandingOver {
		return nil
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			g.mu.Lock()
			g.cond.Broadcast()
			g.mu.Unlock()
		case <-done:
		}
	}()
	defer close(done)

	for g.role == GateRoleHandingOver {
		if err := ctx.Err(); err != nil {
			return util.StatusFromContext(ctx)
		}
		g.cond.Wait()
	}
	if g.role == GateRoleFenced {
		return status.Error(codes.Unavailable, "Scheduler leadership has been fenced")
	}
	return nil
}

type executionSend struct {
	op     *longrunningpb.Operation
	result chan error
}

// executionSender owns the only goroutine writing to a client stream.
// Queue calls can abandon a pending send on handover without waiting for
// HTTP/2 flow control, while subsequent relays retain message ordering.
type executionSender struct {
	requests chan executionSend
	done     chan struct{}
	err      error
	stop     context.CancelFunc
}

func newExecutionSender(out remoteexecution.Execution_ExecuteServer) *executionSender {
	ctx, stop := context.WithCancel(out.Context())
	s := &executionSender{requests: make(chan executionSend), done: make(chan struct{}), stop: stop}
	go func() {
		defer close(s.done)
		for {
			select {
			case <-ctx.Done():
				s.err = util.StatusFromContext(ctx)
				return
			case request := <-s.requests:
				err := out.Send(request.op)
				request.result <- err
				if err != nil {
					s.err = err
					return
				}
			}
		}
	}()
	return s
}

func (s *executionSender) send(ctx context.Context, op *longrunningpb.Operation) error {
	request := executionSend{op: op, result: make(chan error, 1)}
	select {
	case <-ctx.Done():
		return util.StatusFromContext(ctx)
	case <-s.done:
		return s.err
	case s.requests <- request:
	}
	select {
	case <-ctx.Done():
		return util.StatusFromContext(ctx)
	case err := <-request.result:
		return err
	}
}

type capturingExecutionServer struct {
	remoteexecution.Execution_ExecuteServer
	ctx           context.Context
	operationName *string
	sender        *executionSender
}

func (s *capturingExecutionServer) Context() context.Context {
	return s.ctx
}

func (s *capturingExecutionServer) Send(op *longrunningpb.Operation) error {
	if op.GetName() != "" && *s.operationName == "" {
		*s.operationName = op.GetName()
	}
	return s.sender.send(s.ctx, op)
}

func (g *LeaderGate) executeOrWait(in *remoteexecution.ExecuteRequest, initialOperationName string, out remoteexecution.Execution_ExecuteServer) error {
	g.mu.Lock()
	g.activeExecutionCalls++
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.activeExecutionCalls--
		g.cond.Broadcast()
		g.mu.Unlock()
	}()
	ctx := out.Context()
	capturedName := initialOperationName
	sender := newExecutionSender(out)
	defer sender.stop()

	for {
		g.mu.Lock()
		if err := g.waitWhileHandingOverLocked(ctx); err != nil {
			g.mu.Unlock()
			return err
		}

		if g.role == GateRoleLeader {
			callCtx, callCancel := context.WithCancel(ctx)
			cancelPtr := &callCancel
			g.activeLocalCancels[cancelPtr] = struct{}{}
			g.inFlightLocalCalls++
			g.mu.Unlock()

			wrapped := &capturingExecutionServer{
				Execution_ExecuteServer: out,
				ctx:                     callCtx,
				operationName:           &capturedName,
				sender:                  sender,
			}
			var err error
			if capturedName == "" {
				err = g.bq.Execute(in, wrapped)
			} else {
				err = g.bq.WaitExecution(&remoteexecution.WaitExecutionRequest{
					Name: capturedName,
				}, wrapped)
			}

			g.mu.Lock()
			delete(g.activeLocalCancels, cancelPtr)
			g.inFlightLocalCalls--
			if g.inFlightLocalCalls == 0 {
				g.cond.Broadcast()
			}
			interruptedByGate := ctx.Err() == nil && callCtx.Err() != nil
			g.mu.Unlock()
			callCancel()

			if interruptedByGate {
				continue
			}
			return err
		}

		conn, err := g.getConnLocked(g.leaderEndpoints.GetClientEndpoint())
		if err != nil {
			g.mu.Unlock()
			return err
		}
		g.activeRelays++
		g.mu.Unlock()

		defer func() {
			g.mu.Lock()
			g.activeRelays--
			if g.activeRelays == 0 {
				g.cond.Broadcast()
			}
			g.mu.Unlock()
		}()

		execClient := remoteexecution.NewExecutionClient(conn)
		outCtx := forwardIncomingMetadata(ctx)
		var clientStream remoteexecution.Execution_ExecuteClient
		if capturedName != "" {
			clientStream, err = execClient.WaitExecution(outCtx, &remoteexecution.WaitExecutionRequest{
				Name: capturedName,
			})
		} else {
			clientStream, err = execClient.Execute(outCtx, in)
		}
		if err != nil {
			return err
		}
		for {
			op, err := clientStream.Recv()
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
			if op.GetName() != "" && capturedName == "" {
				capturedName = op.GetName()
			}
			if err := sender.send(ctx, op); err != nil {
				return err
			}
		}
	}
}

// Execute forwards or locally executes an REv2 Execute request.
func (g *LeaderGate) Execute(in *remoteexecution.ExecuteRequest, out remoteexecution.Execution_ExecuteServer) error {
	return g.executeOrWait(in, "", out)
}

// WaitExecution forwards or locally attaches to an in-flight REv2 operation.
func (g *LeaderGate) WaitExecution(in *remoteexecution.WaitExecutionRequest, out remoteexecution.Execution_WaitExecutionServer) error {
	if in.GetName() == "" {
		return status.Error(codes.InvalidArgument, "Operation name must not be empty")
	}
	return g.executeOrWait(nil, in.GetName(), out)
}

// Synchronize forwards or locally processes a worker Synchronize request.
func (g *LeaderGate) Synchronize(ctx context.Context, request *remoteworker.SynchronizeRequest) (*remoteworker.SynchronizeResponse, error) {
	for {
		g.mu.Lock()
		if err := g.waitWhileHandingOverLocked(ctx); err != nil {
			g.mu.Unlock()
			return nil, err
		}

		if g.role == GateRoleLeader {
			callCtx, callCancel := context.WithCancel(ctx)
			cancelPtr := &callCancel
			g.activeLocalCancels[cancelPtr] = struct{}{}
			g.inFlightLocalCalls++
			g.mu.Unlock()

			resp, err := g.bq.Synchronize(callCtx, request)

			g.mu.Lock()
			delete(g.activeLocalCancels, cancelPtr)
			g.inFlightLocalCalls--
			if g.inFlightLocalCalls == 0 {
				g.cond.Broadcast()
			}
			interruptedByGate := ctx.Err() == nil && callCtx.Err() != nil
			g.mu.Unlock()
			callCancel()

			if interruptedByGate {
				continue
			}
			return resp, err
		}

		conn, err := g.getConnLocked(g.leaderEndpoints.GetWorkerEndpoint())
		g.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return remoteworker.NewOperationQueueClient(conn).Synchronize(forwardIncomingMetadata(ctx), request)
	}
}

func runBuildQueueStateUnary[Req, Resp any](
	ctx context.Context,
	g *LeaderGate,
	req Req,
	localFn func(context.Context, Req) (Resp, error),
	forwardFn func(context.Context, buildqueuestate.BuildQueueStateClient, Req) (Resp, error),
) (Resp, error) {
	var zero Resp
	for {
		g.mu.Lock()
		if err := g.waitWhileHandingOverLocked(ctx); err != nil {
			g.mu.Unlock()
			return zero, err
		}

		if g.role == GateRoleLeader {
			callCtx, callCancel := context.WithCancel(ctx)
			cancelPtr := &callCancel
			g.activeLocalCancels[cancelPtr] = struct{}{}
			g.inFlightLocalCalls++
			g.mu.Unlock()

			resp, err := localFn(callCtx, req)

			g.mu.Lock()
			delete(g.activeLocalCancels, cancelPtr)
			g.inFlightLocalCalls--
			if g.inFlightLocalCalls == 0 {
				g.cond.Broadcast()
			}
			interruptedByGate := ctx.Err() == nil && callCtx.Err() != nil
			g.mu.Unlock()
			callCancel()

			if interruptedByGate {
				continue
			}
			return resp, err
		}

		conn, err := g.getConnLocked(g.leaderEndpoints.GetStateEndpoint())
		g.mu.Unlock()
		if err != nil {
			return zero, err
		}
		return forwardFn(forwardIncomingMetadata(ctx), buildqueuestate.NewBuildQueueStateClient(conn), req)
	}
}

func (g *LeaderGate) GetOperation(ctx context.Context, req *buildqueuestate.GetOperationRequest) (*buildqueuestate.GetOperationResponse, error) {
	return runBuildQueueStateUnary(ctx, g, req, g.bq.GetOperation, func(ctx context.Context, c buildqueuestate.BuildQueueStateClient, r *buildqueuestate.GetOperationRequest) (*buildqueuestate.GetOperationResponse, error) {
		return c.GetOperation(ctx, r)
	})
}

func (g *LeaderGate) ListOperations(ctx context.Context, req *buildqueuestate.ListOperationsRequest) (*buildqueuestate.ListOperationsResponse, error) {
	return runBuildQueueStateUnary(ctx, g, req, g.bq.ListOperations, func(ctx context.Context, c buildqueuestate.BuildQueueStateClient, r *buildqueuestate.ListOperationsRequest) (*buildqueuestate.ListOperationsResponse, error) {
		return c.ListOperations(ctx, r)
	})
}

func (g *LeaderGate) KillOperations(ctx context.Context, req *buildqueuestate.KillOperationsRequest) (*emptypb.Empty, error) {
	return runBuildQueueStateUnary(ctx, g, req, g.bq.KillOperations, func(ctx context.Context, c buildqueuestate.BuildQueueStateClient, r *buildqueuestate.KillOperationsRequest) (*emptypb.Empty, error) {
		return c.KillOperations(ctx, r)
	})
}

func (g *LeaderGate) ListPlatformQueues(ctx context.Context, req *emptypb.Empty) (*buildqueuestate.ListPlatformQueuesResponse, error) {
	return runBuildQueueStateUnary(ctx, g, req, g.bq.ListPlatformQueues, func(ctx context.Context, c buildqueuestate.BuildQueueStateClient, r *emptypb.Empty) (*buildqueuestate.ListPlatformQueuesResponse, error) {
		return c.ListPlatformQueues(ctx, r)
	})
}

func (g *LeaderGate) ListInvocationChildren(ctx context.Context, req *buildqueuestate.ListInvocationChildrenRequest) (*buildqueuestate.ListInvocationChildrenResponse, error) {
	return runBuildQueueStateUnary(ctx, g, req, g.bq.ListInvocationChildren, func(ctx context.Context, c buildqueuestate.BuildQueueStateClient, r *buildqueuestate.ListInvocationChildrenRequest) (*buildqueuestate.ListInvocationChildrenResponse, error) {
		return c.ListInvocationChildren(ctx, r)
	})
}

func (g *LeaderGate) ListQueuedOperations(ctx context.Context, req *buildqueuestate.ListQueuedOperationsRequest) (*buildqueuestate.ListQueuedOperationsResponse, error) {
	return runBuildQueueStateUnary(ctx, g, req, g.bq.ListQueuedOperations, func(ctx context.Context, c buildqueuestate.BuildQueueStateClient, r *buildqueuestate.ListQueuedOperationsRequest) (*buildqueuestate.ListQueuedOperationsResponse, error) {
		return c.ListQueuedOperations(ctx, r)
	})
}

func (g *LeaderGate) ListWorkers(ctx context.Context, req *buildqueuestate.ListWorkersRequest) (*buildqueuestate.ListWorkersResponse, error) {
	return runBuildQueueStateUnary(ctx, g, req, g.bq.ListWorkers, func(ctx context.Context, c buildqueuestate.BuildQueueStateClient, r *buildqueuestate.ListWorkersRequest) (*buildqueuestate.ListWorkersResponse, error) {
		return c.ListWorkers(ctx, r)
	})
}

func (g *LeaderGate) TerminateWorkers(ctx context.Context, req *buildqueuestate.TerminateWorkersRequest) (*emptypb.Empty, error) {
	return runBuildQueueStateUnary(ctx, g, req, g.bq.TerminateWorkers, func(ctx context.Context, c buildqueuestate.BuildQueueStateClient, r *buildqueuestate.TerminateWorkersRequest) (*emptypb.Empty, error) {
		return c.TerminateWorkers(ctx, r)
	})
}

func (g *LeaderGate) ListDrains(ctx context.Context, req *buildqueuestate.ListDrainsRequest) (*buildqueuestate.ListDrainsResponse, error) {
	return runBuildQueueStateUnary(ctx, g, req, g.bq.ListDrains, func(ctx context.Context, c buildqueuestate.BuildQueueStateClient, r *buildqueuestate.ListDrainsRequest) (*buildqueuestate.ListDrainsResponse, error) {
		return c.ListDrains(ctx, r)
	})
}

func (g *LeaderGate) AddDrain(ctx context.Context, req *buildqueuestate.AddOrRemoveDrainRequest) (*emptypb.Empty, error) {
	return runBuildQueueStateUnary(ctx, g, req, g.bq.AddDrain, func(ctx context.Context, c buildqueuestate.BuildQueueStateClient, r *buildqueuestate.AddOrRemoveDrainRequest) (*emptypb.Empty, error) {
		return c.AddDrain(ctx, r)
	})
}

func (g *LeaderGate) RemoveDrain(ctx context.Context, req *buildqueuestate.AddOrRemoveDrainRequest) (*emptypb.Empty, error) {
	return runBuildQueueStateUnary(ctx, g, req, g.bq.RemoveDrain, func(ctx context.Context, c buildqueuestate.BuildQueueStateClient, r *buildqueuestate.AddOrRemoveDrainRequest) (*emptypb.Empty, error) {
		return c.RemoveDrain(ctx, r)
	})
}
