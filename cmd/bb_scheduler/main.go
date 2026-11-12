package main

import (
	"context"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_blobstore "github.com/buildbarn/bb-remote-execution/pkg/blobstore"
	re_cas "github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_scheduler"
	scheduler_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/scheduler"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/coordinator"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/schedulerhandover"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/initialsizeclass"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/routing"
	auth_configuration "github.com/buildbarn/bb-storage/pkg/auth/configuration"
	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	http_server "github.com/buildbarn/bb-storage/pkg/http/server"
	"github.com/buildbarn/bb-storage/pkg/program"
	grpc_configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/buildbarn/bb-storage/pkg/proto/iscc"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"
	"github.com/gorilla/mux"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func main() {
	program.RunMain(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		if len(os.Args) != 2 {
			return status.Error(codes.InvalidArgument, "Usage: bb_scheduler bb_scheduler.jsonnet")
		}
		var configuration bb_scheduler.ApplicationConfiguration
		if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
			return util.StatusWrapf(err, "Failed to read configuration from %s", os.Args[1])
		}
		if (configuration.Coordinator != nil) != (len(configuration.HandoverGrpcServers) > 0) {
			return status.Error(codes.InvalidArgument, "Coordinator and handover gRPC servers must be configured together")
		}
		lifecycleState, grpcClientFactory, err := global.ApplyConfiguration(configuration.Global, dependenciesGroup)
		if err != nil {
			return util.StatusWrap(err, "Failed to apply global configuration options")
		}

		portalURL, err := url.Parse(configuration.DeprecatedPortalUrl)
		if err != nil {
			return util.StatusWrap(err, "Failed to parse portal URL")
		}

		// Storage access. The scheduler requires access to the Action
		// and Command messages stored in the CAS to obtain platform
		// properties.
		casInfo, err := blobstore_configuration.NewCASFromConfiguration(
			dependenciesGroup,
			configuration.ContentAddressableStorage,
			grpcClientFactory,
			int(configuration.MaximumMessageSizeBytes),
		)
		if err != nil {
			return util.StatusWrap(err, "Failed to create Content Adddressable Storage")
		}

		// Optional: Initial Size Class Cache (ISCC) access. This data
		// store is only used if one or more parts of the ActionRouter
		// are configured to use feedback driven initial size class
		// analysis.
		var previousExecutionStatsStore initialsizeclass.PreviousExecutionStatsStore
		if isccConfiguration := configuration.InitialSizeClassCache; isccConfiguration != nil {
			info, err := blobstore_configuration.NewBlobAccessFromConfiguration(
				dependenciesGroup,
				isccConfiguration,
				blobstore_configuration.NewISCCBlobAccessCreator(
					grpcClientFactory,
					int(configuration.MaximumMessageSizeBytes),
				),
			)
			if err != nil {
				return util.StatusWrap(err, "Failed to create Initial Size Class Cache")
			}
			previousExecutionStatsStore = re_blobstore.NewBlobAccessMutableProtoStore[iscc.PreviousExecutionStats](
				info.BlobAccess,
			)
		}

		// Create an action router that is responsible for analyzing
		// incoming execution requests and determining how they are
		// scheduled. It may be replaced at runtime when the
		// configuration file is reloaded.
		initialActionRouter, err := routing.NewActionRouterFromConfiguration(configuration.ActionRouter, previousExecutionStatsStore, grpcClientFactory, dependenciesGroup)
		if err != nil {
			return util.StatusWrap(err, "Failed to create action router")
		}
		actionRouter := routing.NewReloadableActionRouter(initialActionRouter)

		authorizerFactory := auth_configuration.DefaultAuthorizerFactory
		executeAuthorizer, err := authorizerFactory.NewAuthorizerFromConfiguration(configuration.ExecuteAuthorizer, dependenciesGroup, grpcClientFactory)
		if err != nil {
			return util.StatusWrap(err, "Failed to create execute authorizer")
		}
		modifyDrainsAuthorizer, err := authorizerFactory.NewAuthorizerFromConfiguration(configuration.ModifyDrainsAuthorizer, dependenciesGroup, grpcClientFactory)
		if err != nil {
			return util.StatusWrap(err, "Failed to create modify drains authorizer")
		}
		killOperationsAuthorizer, err := authorizerFactory.NewAuthorizerFromConfiguration(configuration.KillOperationsAuthorizer, dependenciesGroup, grpcClientFactory)
		if err != nil {
			return util.StatusWrap(err, "Failed to create kill operaitons authorizer")
		}
		synchronizeAuthorizer, err := authorizerFactory.NewAuthorizerFromConfiguration(configuration.SynchronizeAuthorizer, dependenciesGroup, grpcClientFactory)
		if err != nil {
			return util.StatusWrap(err, "Failed to create synchronize authorizer")
		}

		platformQueueWithNoWorkersTimeout := configuration.PlatformQueueWithNoWorkersTimeout
		if err := platformQueueWithNoWorkersTimeout.CheckValid(); err != nil {
			return util.StatusWrap(err, "Invalid platform queue with no workers timeout")
		}

		var adoptionWindow, recoveredGracePeriod, relayDrainPeriod time.Duration
		if coordCfg := configuration.Coordinator; coordCfg != nil {
			if coordCfg.AdoptionWindow != nil {
				if err := coordCfg.AdoptionWindow.CheckValid(); err != nil {
					return util.StatusWrap(err, "Invalid coordinator adoption window")
				}
				adoptionWindow = coordCfg.AdoptionWindow.AsDuration()
			}
			if coordCfg.RecoveredOperationGracePeriod != nil {
				if err := coordCfg.RecoveredOperationGracePeriod.CheckValid(); err != nil {
					return util.StatusWrap(err, "Invalid coordinator recovered operation grace period")
				}
				recoveredGracePeriod = coordCfg.RecoveredOperationGracePeriod.AsDuration()
			}
			if coordCfg.RelayDrainPeriod != nil {
				if err := coordCfg.RelayDrainPeriod.CheckValid(); err != nil {
					return util.StatusWrap(err, "Invalid coordinator relay drain period")
				}
				relayDrainPeriod = coordCfg.RelayDrainPeriod.AsDuration()
			}
		}

		// Create in-memory build queue.
		// TODO: Make timeouts configurable.
		generator := random.NewFastSingleThreadedGenerator()
		buildQueue := scheduler.NewInMemoryBuildQueue(
			re_cas.NewExistencePreconditionReader(cas.NewMessageReader[remoteexecution.Action](
				casInfo.ReadInfo,
				int(configuration.MaximumMessageSizeBytes),
			)),
			clock.SystemClock,
			uuid.NewRandom,
			&scheduler.InMemoryBuildQueueConfiguration{
				ExecutionUpdateInterval:           time.Minute,
				OperationWithNoWaitersTimeout:     time.Minute,
				PlatformQueueWithNoWorkersTimeout: platformQueueWithNoWorkersTimeout.AsDuration(),
				BusyWorkerSynchronizationInterval: 10 * time.Second,
				GetIdleWorkerSynchronizationInterval: func() time.Duration {
					// Let synchronization calls block somewhere
					// between 0 and 2 minutes. Add jitter to
					// prevent recurring traffic spikes.
					return random.Duration(generator, 2*time.Minute)
				},
				WorkerTaskRetryCount:                   9,
				WorkerWithNoSynchronizationsTimeout:    time.Minute,
				WorkerAdoptionTimeout:                  adoptionWindow,
				RecoveredOperationWithNoWaitersTimeout: recoveredGracePeriod,
			},
			actionRouter,
			executeAuthorizer,
			modifyDrainsAuthorizer,
			killOperationsAuthorizer,
			synchronizeAuthorizer,
		)

		var clientBuildQueue builder.BuildQueue = buildQueue
		var workerQueueServer remoteworker.OperationQueueServer = buildQueue
		var stateServer buildqueuestate.BuildQueueStateServer = buildQueue
		serverGroup := siblingsGroup

		var gate *scheduler.LeaderGate
		var dialer scheduler.EndpointDialer
		var coordClient coordinator.CoordinatorClient
		runMaintenance := func(fn func()) bool {
			fn()
			return true
		}
		if configuration.Coordinator != nil {
			coordConn, err := grpcClientFactory.NewClientFromConfiguration(configuration.Coordinator.Client, dependenciesGroup)
			if err != nil {
				return util.StatusWrap(err, "Failed to create coordinator gRPC client")
			}
			coordClient = coordinator.NewCoordinatorClient(coordConn)
			dialer = func(endpoint string) (grpc.ClientConnInterface, error) {
				var clientCfg grpc_configuration.ClientConfiguration
				if configuration.Coordinator.PeerClientTemplate != nil {
					proto.Merge(&clientCfg, configuration.Coordinator.PeerClientTemplate)
				}
				clientCfg.Address = endpoint
				return grpcClientFactory.NewClientFromConfiguration(&clientCfg, dependenciesGroup)
			}
			gate = scheduler.NewLeaderGate(buildQueue, dialer, scheduler.GateRoleFollower, nil)
			clientBuildQueue = gate
			workerQueueServer = gate
			stateServer = gate
			serverGroup = dependenciesGroup
			runMaintenance = gate.RunMaintenance
		}

		// Force periodic cleanups of stale workers. This also
		// happens automatically when RPCs occur, but that's not
		// sufficient to ensure Prometheus metrics are updated
		// if the final workers disappear.
		//
		// TODO: Maybe it's better to let InMemoryBuildQueue
		// implement prometheus.Collector? Then cleanups can run
		// whenever the scheduler is scraped.
		dependenciesGroup.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
			t := time.NewTicker(time.Minute)
			for {
				select {
				case <-t.C:
					runMaintenance(buildQueue.ForceCleanup)
				case <-ctx.Done():
					t.Stop()
					return nil
				}
			}
		})

		// Create predeclared platform queues.
		predeclaredQueues, err := parsePredeclaredPlatformQueues(configuration.PredeclaredPlatformQueues)
		if err != nil {
			return err
		}
		if _, err := buildQueue.ReconcilePredeclaredPlatformQueues(predeclaredQueues); err != nil {
			return util.StatusWrap(err, "Failed to register predeclared platform queues")
		}

		// Optional: periodically reload the configuration file, so
		// that changes to the predeclared platform queues and the
		// action router are applied without a restart.
		if reloadIntervalMessage := configuration.ConfigurationReloadInterval; reloadIntervalMessage != nil {
			if err := reloadIntervalMessage.CheckValid(); err != nil {
				return util.StatusWrap(err, "Invalid configuration reload interval")
			}
			if reloadInterval := reloadIntervalMessage.AsDuration(); reloadInterval > 0 {
				configurationPath := os.Args[1]
				initialActionRouterConfiguration := configuration.ActionRouter
				dependenciesGroup.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
					newConfigurationReloader(
						func() (*bb_scheduler.ApplicationConfiguration, error) {
							var reloadedConfiguration bb_scheduler.ApplicationConfiguration
							if err := util.UnmarshalConfigurationFromFile(configurationPath, &reloadedConfiguration); err != nil {
								return nil, util.StatusWrapf(err, "Failed to read configuration from %s", configurationPath)
							}
							return &reloadedConfiguration, nil
						},
						func(actionRouterConfiguration *scheduler_pb.ActionRouterConfiguration) (routing.ActionRouter, error) {
							return routing.NewActionRouterFromConfiguration(actionRouterConfiguration, previousExecutionStatsStore, grpcClientFactory, dependenciesGroup)
						},
						buildQueue,
						actionRouter,
						initialActionRouterConfiguration,
						runMaintenance,
					).run(ctx, reloadInterval)
					return nil
				})
			}
		}

		// Spawn gRPC servers for client and worker traffic.
		if err := bb_grpc.NewServersFromConfigurationAndServe(
			configuration.ClientGrpcServers,
			func(s grpc.ServiceRegistrar) {
				remoteexecution.RegisterCapabilitiesServer(
					s,
					capabilities.NewServer(clientBuildQueue),
				)
				remoteexecution.RegisterExecutionServer(s, clientBuildQueue)
			},
			serverGroup,
			grpcClientFactory,
		); err != nil {
			return util.StatusWrap(err, "Client gRPC server failure")
		}
		if err := bb_grpc.NewServersFromConfigurationAndServe(
			configuration.WorkerGrpcServers,
			func(s grpc.ServiceRegistrar) {
				remoteworker.RegisterOperationQueueServer(s, workerQueueServer)
			},
			serverGroup,
			grpcClientFactory,
		); err != nil {
			return util.StatusWrap(err, "Worker gRPC server failure")
		}
		if err := bb_grpc.NewServersFromConfigurationAndServe(
			configuration.BuildQueueStateGrpcServers,
			func(s grpc.ServiceRegistrar) {
				buildqueuestate.RegisterBuildQueueStateServer(s, stateServer)
			},
			serverGroup,
			grpcClientFactory,
		); err != nil {
			return util.StatusWrap(err, "Build queue state gRPC server failure")
		}
		if len(configuration.HandoverGrpcServers) > 0 {
			handoverServer := scheduler.NewHandoverServer(gate, coordClient)
			if err := bb_grpc.NewServersFromConfigurationAndServe(
				configuration.HandoverGrpcServers,
				func(s grpc.ServiceRegistrar) {
					schedulerhandover.RegisterHandoverServer(s, handoverServer)
				},
				serverGroup,
				grpcClientFactory,
			); err != nil {
				return util.StatusWrap(err, "Handover gRPC server failure")
			}
		}

		// Web server for metrics and profiling.
		router := mux.NewRouter()
		routePrefix := path.Join("/", configuration.DeprecatedAdminRoutePrefix)
		if !strings.HasSuffix(routePrefix, "/") {
			routePrefix += "/"
		}
		subrouter := router.PathPrefix(routePrefix).Subrouter()
		newBuildQueueStateService(stateServer, clock.SystemClock, portalURL, subrouter)
		http_server.NewServersFromConfigurationAndServe(
			configuration.DeprecatedAdminHttpServers,
			http_server.NewMetricsHandler(router, "SchedulerUI"),
			serverGroup,
			grpcClientFactory,
		)

		if configuration.Coordinator != nil {
			selfEndpoints := &schedulerhandover.LeaderEndpoints{
				LeaderId:         configuration.Coordinator.SelfId,
				ClientEndpoint:   configuration.Coordinator.SelfClientEndpoint,
				WorkerEndpoint:   configuration.Coordinator.SelfWorkerEndpoint,
				StateEndpoint:    configuration.Coordinator.SelfStateEndpoint,
				HandoverEndpoint: configuration.Coordinator.SelfHandoverEndpoint,
			}
			siblingsGroup.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
				return scheduler.RunCoordinatorLoop(
					ctx,
					gate,
					dialer,
					coordClient,
					selfEndpoints,
					relayDrainPeriod,
					func() {
						lifecycleState.MarkReadyAndWait(dependenciesGroup)
					},
				)
			})
			return nil
		}

		lifecycleState.MarkReadyAndWait(siblingsGroup)
		return nil
	})
}

func parsePredeclaredPlatformQueues(configs []*bb_scheduler.PredeclaredPlatformQueueConfiguration) ([]scheduler.PredeclaredPlatformQueueParams, error) {
	queues := make([]scheduler.PredeclaredPlatformQueueParams, 0, len(configs))
	for _, platformQueue := range configs {
		instanceName, err := digest.NewInstanceName(platformQueue.InstanceNamePrefix)
		if err != nil {
			return nil, util.StatusWrapf(err, "Invalid instance name prefix %#v", platformQueue.InstanceNamePrefix)
		}
		workerInvocationStickinessLimits := make([]time.Duration, 0, len(platformQueue.WorkerInvocationStickinessLimits))
		for i, d := range platformQueue.WorkerInvocationStickinessLimits {
			if err := d.CheckValid(); err != nil {
				return nil, util.StatusWrapf(err, "Invalid worker invocation stickiness limit at index %d", i)
			}
			workerInvocationStickinessLimits = append(workerInvocationStickinessLimits, d.AsDuration())
		}
		queues = append(queues, scheduler.PredeclaredPlatformQueueParams{
			InstanceNamePrefix:                        instanceName,
			Platform:                                  platformQueue.Platform,
			WorkerInvocationStickinessLimits:          workerInvocationStickinessLimits,
			MaximumQueuedBackgroundLearningOperations: int(platformQueue.MaximumQueuedBackgroundLearningOperations),
			BackgroundLearningOperationPriority:       platformQueue.BackgroundLearningOperationPriority,
			SizeClasses:                               platformQueue.SizeClasses,
		})
	}
	return queues, nil
}
