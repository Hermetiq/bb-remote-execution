package main

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_scheduler"
	scheduler_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/scheduler"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/initialsizeclass"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/invocation"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/platform"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/routing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// namedActionRouter is an ActionRouter that fails every call with its
// name, making it possible to determine which router is installed.
type namedActionRouter string

func (r namedActionRouter) RouteAction(ctx context.Context, digestFunction digest.Function, action *remoteexecution.Action, requestMetadata *remoteexecution.RequestMetadata) (*remoteexecution.Action, platform.Key, []invocation.Key, initialsizeclass.Selector, error) {
	return nil, platform.Key{}, nil, nil, status.Error(codes.Internal, string(r))
}

// fakeReconciler records calls to ReconcilePredeclaredPlatformQueues().
type fakeReconciler struct {
	calls          [][]scheduler.PredeclaredPlatformQueueParams
	pendingRestart []string
	err            error
	onCall         func()
}

func (r *fakeReconciler) ReconcilePredeclaredPlatformQueues(queues []scheduler.PredeclaredPlatformQueueParams) ([]string, error) {
	r.calls = append(r.calls, queues)
	if r.onCall != nil {
		r.onCall()
	}
	return r.pendingRestart, r.err
}

func actionRouterConfigurationForTesting(propertyValue string) *scheduler_pb.ActionRouterConfiguration {
	return &scheduler_pb.ActionRouterConfiguration{
		Kind: &scheduler_pb.ActionRouterConfiguration_Simple{
			Simple: &scheduler_pb.SimpleActionRouterConfiguration{
				PlatformKeyExtractor: &scheduler_pb.PlatformKeyExtractorConfiguration{
					Kind: &scheduler_pb.PlatformKeyExtractorConfiguration_Static{
						Static: &remoteexecution.Platform{
							Properties: []*remoteexecution.Platform_Property{
								{Name: "pool", Value: propertyValue},
							},
						},
					},
				},
			},
		},
	}
}

func requireInstalledActionRouter(t *testing.T, actionRouter routing.ActionRouter, name string) {
	_, _, _, _, err := actionRouter.RouteAction(
		context.Background(),
		digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256),
		&remoteexecution.Action{},
		&remoteexecution.RequestMetadata{},
	)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, name), err)
}

func reload(t *testing.T, r *configurationReloader) error {
	t.Helper()
	applied, err := r.reload()
	if err == nil {
		require.True(t, applied)
	}
	return err
}

func TestConfigurationReloader(t *testing.T) {
	initialRouterConfiguration := actionRouterConfigurationForTesting("initial")
	queuesConfiguration := []*bb_scheduler.PredeclaredPlatformQueueConfiguration{{
		InstanceNamePrefix: "main",
		Platform: &remoteexecution.Platform{
			Properties: []*remoteexecution.Platform_Property{
				{Name: "pool", Value: "ub22-pw-r4x9"},
			},
		},
		SizeClasses: []uint32{0},
	}}

	type fixture struct {
		reloader           *configurationReloader
		actionRouter       *routing.ReloadableActionRouter
		reconciler         *fakeReconciler
		configuration      *bb_scheduler.ApplicationConfiguration
		loadErr            error
		newActionRouterErr error
		routersCreated     []*scheduler_pb.ActionRouterConfiguration
	}
	newFixture := func() *fixture {
		f := &fixture{
			actionRouter: routing.NewReloadableActionRouter(namedActionRouter("initial")),
			reconciler:   &fakeReconciler{},
			configuration: &bb_scheduler.ApplicationConfiguration{
				ActionRouter:              proto.Clone(initialRouterConfiguration).(*scheduler_pb.ActionRouterConfiguration),
				PredeclaredPlatformQueues: queuesConfiguration,
			},
		}
		f.reloader = newConfigurationReloader(
			func() (*bb_scheduler.ApplicationConfiguration, error) {
				return f.configuration, f.loadErr
			},
			func(configuration *scheduler_pb.ActionRouterConfiguration) (routing.ActionRouter, error) {
				f.routersCreated = append(f.routersCreated, configuration)
				if f.newActionRouterErr != nil {
					return nil, f.newActionRouterErr
				}
				return namedActionRouter(configuration.GetSimple().GetPlatformKeyExtractor().GetStatic().GetProperties()[0].GetValue()), nil
			},
			f.reconciler,
			f.actionRouter,
			initialRouterConfiguration,
			func(fn func()) bool {
				fn()
				return true
			},
		)
		return f
	}

	t.Run("UnchangedActionRouter", func(t *testing.T) {
		// Queues must always be reconciled, but an action router
		// whose configuration is unchanged must not be rebuilt.
		f := newFixture()
		require.NoError(t, reload(t, f.reloader))
		require.Len(t, f.reconciler.calls, 1)
		require.Len(t, f.reconciler.calls[0], 1)
		require.Equal(t, []uint32{0}, f.reconciler.calls[0][0].SizeClasses)
		require.Empty(t, f.routersCreated)
		requireInstalledActionRouter(t, f.actionRouter, "initial")
	})

	t.Run("ChangedActionRouter", func(t *testing.T) {
		// A changed action router is installed, but only after
		// the queues have been reconciled.
		f := newFixture()
		f.configuration.ActionRouter = actionRouterConfigurationForTesting("updated")
		f.reconciler.onCall = func() {
			requireInstalledActionRouter(t, f.actionRouter, "initial")
		}
		require.NoError(t, reload(t, f.reloader))
		require.Len(t, f.reconciler.calls, 1)
		require.Len(t, f.routersCreated, 1)
		requireInstalledActionRouter(t, f.actionRouter, "updated")

		// Reloading the same configuration again must not
		// rebuild the action router.
		f.reconciler.onCall = nil
		require.NoError(t, reload(t, f.reloader))
		require.Len(t, f.routersCreated, 1)
		requireInstalledActionRouter(t, f.actionRouter, "updated")
	})

	t.Run("LoadFailure", func(t *testing.T) {
		f := newFixture()
		f.loadErr = status.Error(codes.InvalidArgument, "Syntax error")
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to load configuration: Syntax error"), reload(t, f.reloader))
		require.Empty(t, f.reconciler.calls)
		requireInstalledActionRouter(t, f.actionRouter, "initial")
	})

	t.Run("InvalidQueues", func(t *testing.T) {
		// Invalid queues must prevent the action router from
		// being replaced as well.
		f := newFixture()
		f.configuration.ActionRouter = actionRouterConfigurationForTesting("updated")
		f.configuration.PredeclaredPlatformQueues = []*bb_scheduler.PredeclaredPlatformQueueConfiguration{{
			InstanceNamePrefix: "/invalid",
			SizeClasses:        []uint32{0},
		}}
		require.Error(t, reload(t, f.reloader))
		require.Empty(t, f.reconciler.calls)
		requireInstalledActionRouter(t, f.actionRouter, "initial")
	})

	t.Run("ActionRouterCreationFailure", func(t *testing.T) {
		// An action router that cannot be created must prevent
		// the queues from being reconciled.
		f := newFixture()
		f.configuration.ActionRouter = actionRouterConfigurationForTesting("updated")
		f.newActionRouterErr = status.Error(codes.InvalidArgument, "Feedback driven analysis can only be enabled if an Initial Size Class Cache (ISCC) is configured")
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Failed to create action router: Feedback driven analysis can only be enabled if an Initial Size Class Cache (ISCC) is configured"),
			reload(t, f.reloader),
		)
		require.Empty(t, f.reconciler.calls)
		requireInstalledActionRouter(t, f.actionRouter, "initial")
	})

	t.Run("ReconcileFailure", func(t *testing.T) {
		// If the queues cannot be reconciled, the new action
		// router must not be installed.
		f := newFixture()
		f.configuration.ActionRouter = actionRouterConfigurationForTesting("updated")
		f.reconciler.err = status.Error(codes.AlreadyExists, "A queue with the same instance name prefix or platform already exists")
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.AlreadyExists, "Failed to reconcile predeclared platform queues: A queue with the same instance name prefix or platform already exists"),
			reload(t, f.reloader),
		)
		requireInstalledActionRouter(t, f.actionRouter, "initial")

		// Once the problem is resolved, the new action router
		// must be installed on the next reload.
		f.reconciler.err = nil
		require.NoError(t, reload(t, f.reloader))
		requireInstalledActionRouter(t, f.actionRouter, "updated")
	})

	t.Run("PendingRestart", func(t *testing.T) {
		// Queue changes that require a restart are tracked, but
		// do not cause the reload to fail.
		f := newFixture()
		f.reconciler.pendingRestart = []string{"Predeclared platform queue needs a restart"}
		require.NoError(t, reload(t, f.reloader))
		require.Equal(t, []string{"Predeclared platform queue needs a restart"}, f.reloader.pendingRestart)

		f.reconciler.pendingRestart = nil
		require.NoError(t, reload(t, f.reloader))
		require.Empty(t, f.reloader.pendingRestart)
	})
	for name, role := range map[string]scheduler.GateRole{
		"Follower":    scheduler.GateRoleFollower,
		"HandingOver": scheduler.GateRoleHandingOver,
		"Retired":     scheduler.GateRoleRetired,
		"Fenced":      scheduler.GateRoleFenced,
	} {
		t.Run("Skips"+name, func(t *testing.T) {
			f := newFixture()
			gate := scheduler.NewLeaderGate(nil, nil, role, nil)
			f.reloader.runMaintenance = gate.RunMaintenance
			f.configuration.ActionRouter = actionRouterConfigurationForTesting("updated")
			applied, err := f.reloader.reload()
			require.NoError(t, err)
			require.False(t, applied)
			require.Empty(t, f.reconciler.calls)
			require.Empty(t, f.routersCreated)
			requireInstalledActionRouter(t, f.actionRouter, "initial")
		})
	}

	t.Run("HandoverDuringPreparation", func(t *testing.T) {
		f := newFixture()
		gate := scheduler.NewLeaderGate(nil, nil, scheduler.GateRoleLeader, nil)
		f.reloader.runMaintenance = gate.RunMaintenance
		f.configuration.ActionRouter = actionRouterConfigurationForTesting("updated")
		newRouter := f.reloader.newActionRouter
		f.reloader.newActionRouter = func(configuration *scheduler_pb.ActionRouterConfiguration) (routing.ActionRouter, error) {
			// Starting handover must not wait for configuration evaluation,
			// and a prepared reload must not mutate the frozen queue.
			require.NoError(t, gate.BeginHandover())
			return newRouter(configuration)
		}
		applied, err := f.reloader.reload()
		require.NoError(t, err)
		require.False(t, applied)
		require.Empty(t, f.reconciler.calls)
		requireInstalledActionRouter(t, f.actionRouter, "initial")

		gate.AbortHandover()
		f.reloader.newActionRouter = newRouter
		require.NoError(t, reload(t, f.reloader))
		require.Len(t, f.reconciler.calls, 1)
		requireInstalledActionRouter(t, f.actionRouter, "updated")
	})

}
