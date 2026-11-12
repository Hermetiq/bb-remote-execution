package routing

import (
	"context"
	"sync/atomic"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/initialsizeclass"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/invocation"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/platform"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// ReloadableActionRouter is an ActionRouter that forwards all calls to
// a backend that may be replaced at runtime, for example after the
// scheduler's configuration file has been reloaded.
//
// Replacing the backend only affects calls to RouteAction() that are
// made afterwards. Selectors and learners that were returned by the
// previous backend remain valid, so that tasks that were already
// created can complete normally.
type ReloadableActionRouter struct {
	backend atomic.Pointer[actionRouterHolder]
}

// actionRouterHolder wraps an ActionRouter, as atomic.Pointer cannot
// store interface values directly.
type actionRouterHolder struct {
	ActionRouter
}

var _ ActionRouter = (*ReloadableActionRouter)(nil)

// NewReloadableActionRouter creates a ReloadableActionRouter that
// initially forwards calls to the provided backend.
func NewReloadableActionRouter(initial ActionRouter) *ReloadableActionRouter {
	r := &ReloadableActionRouter{}
	r.Set(initial)
	return r
}

// Set replaces the backend to which calls are forwarded.
func (r *ReloadableActionRouter) Set(backend ActionRouter) {
	r.backend.Store(&actionRouterHolder{ActionRouter: backend})
}

// RouteAction forwards the call to the current backend.
func (r *ReloadableActionRouter) RouteAction(ctx context.Context, digestFunction digest.Function, action *remoteexecution.Action, requestMetadata *remoteexecution.RequestMetadata) (*remoteexecution.Action, platform.Key, []invocation.Key, initialsizeclass.Selector, error) {
	return r.backend.Load().RouteAction(ctx, digestFunction, action, requestMetadata)
}
