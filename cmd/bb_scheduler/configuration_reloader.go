package main

import (
	"context"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_scheduler"
	scheduler_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/scheduler"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/routing"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/protobuf/proto"
)

var (
	configurationReloaderPrometheusMetrics sync.Once

	configurationReloadsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "scheduler",
			Name:      "configuration_reloads_total",
			Help:      "Number of times the configuration file was reloaded, partitioned by whether the reloaded configuration was applied.",
		},
		[]string{"result"},
	)
	configurationReloadLastSuccessTimestampSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "buildbarn",
			Subsystem: "scheduler",
			Name:      "configuration_reload_last_success_timestamp_seconds",
			Help:      "Time at which the configuration file was last reloaded and applied successfully.",
		},
	)
	predeclaredPlatformQueuesPendingRestart = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "buildbarn",
			Subsystem: "scheduler",
			Name:      "predeclared_platform_queues_pending_restart",
			Help:      "Number of predeclared platform queues whose configured size classes or worker invocation stickiness limits differ from the ones in use, and can only be applied by restarting the scheduler.",
		},
	)
)

// predeclaredPlatformQueueReconciler is the subset of
// scheduler.InMemoryBuildQueue that is used by configurationReloader.
type predeclaredPlatformQueueReconciler interface {
	ReconcilePredeclaredPlatformQueues(queues []scheduler.PredeclaredPlatformQueueParams) ([]string, error)
}

var _ predeclaredPlatformQueueReconciler = (*scheduler.InMemoryBuildQueue)(nil)

// configurationReloader periodically re-evaluates the scheduler's
// configuration file, and applies the parts of it that can be changed
// without restarting the scheduler: the predeclared platform queues and
// the action router.
//
// Reloads are applied as a whole or not at all. The new action router
// is only installed after the predeclared platform queues have been
// reconciled, so that actions routed to a newly added platform queue
// never observe it being absent.
type configurationReloader struct {
	loadConfiguration func() (*bb_scheduler.ApplicationConfiguration, error)
	newActionRouter   func(configuration *scheduler_pb.ActionRouterConfiguration) (routing.ActionRouter, error)
	queues            predeclaredPlatformQueueReconciler
	actionRouter      *routing.ReloadableActionRouter
	runMaintenance    func(func()) bool

	currentActionRouterConfiguration *scheduler_pb.ActionRouterConfiguration
	pendingRestart                   []string
}

func newConfigurationReloader(
	loadConfiguration func() (*bb_scheduler.ApplicationConfiguration, error),
	newActionRouter func(configuration *scheduler_pb.ActionRouterConfiguration) (routing.ActionRouter, error),
	queues predeclaredPlatformQueueReconciler,
	actionRouter *routing.ReloadableActionRouter,
	initialActionRouterConfiguration *scheduler_pb.ActionRouterConfiguration,
	runMaintenance func(func()) bool,
) *configurationReloader {
	configurationReloaderPrometheusMetrics.Do(func() {
		prometheus.MustRegister(configurationReloadsTotal)
		prometheus.MustRegister(configurationReloadLastSuccessTimestampSeconds)
		prometheus.MustRegister(predeclaredPlatformQueuesPendingRestart)
	})

	return &configurationReloader{
		loadConfiguration:                loadConfiguration,
		newActionRouter:                  newActionRouter,
		queues:                           queues,
		actionRouter:                     actionRouter,
		runMaintenance:                   runMaintenance,
		currentActionRouterConfiguration: initialActionRouterConfiguration,
	}
}

// reload the configuration file once and apply it. If an error is
// returned, none of the changes have been applied. A false result means the
// scheduler is no longer the leader, so the prepared changes were skipped.
func (r *configurationReloader) reload() (bool, error) {
	if !r.runMaintenance(func() {}) {
		return false, nil
	}
	configuration, err := r.loadConfiguration()
	if err != nil {
		return false, util.StatusWrap(err, "Failed to load configuration")
	}

	// Validate everything before making any changes.
	queues, err := parsePredeclaredPlatformQueues(configuration.PredeclaredPlatformQueues)
	if err != nil {
		return false, util.StatusWrap(err, "Invalid predeclared platform queues")
	}
	var newActionRouter routing.ActionRouter
	actionRouterChanged := !proto.Equal(r.currentActionRouterConfiguration, configuration.ActionRouter)
	if actionRouterChanged {
		newActionRouter, err = r.newActionRouter(configuration.ActionRouter)
		if err != nil {
			return false, util.StatusWrap(err, "Failed to create action router")
		}
	}

	// Loading configuration and constructing the router can block. Only
	// the commit is serialized with handover; recheck leadership here so
	// a handover started during preparation cannot modify frozen state.
	applied := r.runMaintenance(func() {
		// ReconcilePredeclaredPlatformQueues() validates its input before
		// making any changes, so if it fails, nothing has been changed.
		var pendingRestart []string
		pendingRestart, err = r.queues.ReconcilePredeclaredPlatformQueues(queues)
		if err != nil {
			return
		}
		if !slices.Equal(pendingRestart, r.pendingRestart) {
			for _, description := range pendingRestart {
				log.Printf("Configuration change can only be applied by restarting the scheduler: %s", description)
			}
			r.pendingRestart = pendingRestart
			predeclaredPlatformQueuesPendingRestart.Set(float64(len(pendingRestart)))
		}

		if actionRouterChanged {
			r.actionRouter.Set(newActionRouter)
			r.currentActionRouterConfiguration = configuration.ActionRouter
			log.Print("Applied reloaded action router configuration")
		}
	})
	if err != nil {
		return false, util.StatusWrap(err, "Failed to reconcile predeclared platform queues")
	}
	return applied, nil
}

// run reloads the configuration file at the provided interval, until
// the context is canceled.
func (r *configurationReloader) run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if applied, err := r.reload(); err != nil {
				configurationReloadsTotal.WithLabelValues("Failure").Inc()
				log.Print("Failed to reload configuration; continuing with the previous configuration: ", err)
			} else if applied {
				configurationReloadsTotal.WithLabelValues("Success").Inc()
				configurationReloadLastSuccessTimestampSeconds.SetToCurrentTime()
			}
		case <-ctx.Done():
			return
		}
	}
}
