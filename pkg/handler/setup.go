package handler

import (
	"context"
	"github.com/quantumwake/alethic-ism-core-go/pkg/cache"
	"github.com/quantumwake/alethic-ism-core-go/pkg/repository/processor"
	"github.com/quantumwake/alethic-ism-core-go/pkg/repository/route"
	"github.com/quantumwake/alethic-ism-core-go/pkg/repository/state"
	"github.com/quantumwake/alethic-ism-core-go/pkg/routing"
	rnats "github.com/quantumwake/alethic-ism-core-go/pkg/routing/nats"
	"log"
	"os"
	"time"
)

const (
	SelectorSubscriber = "data/transformers/mixer/state-tables-1.0"
	SelectorMonitor    = "processor/monitor"
	SelectorStoreSync  = "processor/state/sync"
)

var (
	dsn = os.Getenv("DSN")

	subscriberRoute routing.Route // the route we are listening on
	monitorRoute    routing.Route // route for sending errors
	syncRoute       routing.Route // route for sending sync messages

	// no cache
	stateBackend = state.NewBackend(dsn)

	// backendCache
	backendCache     cache.Cache
	routeBackend     *route.CachedBackendStorage
	processorBackend *processor.CachedBackendStorage
)

func SetupCachedBackend() error {
	// Create a local cache with an aggressive cache expiration, since we do not have a central cache
	backendCache = cache.NewLocalCache(&cache.Config{
		DefaultTTL: 30 * time.Second,
	})

	baseTTL := backendCache.GetDefaultTTL() // 30 seconds

	// All backends now respect this TTL
	processorBackend = processor.NewCachedBackend(dsn, backendCache, baseTTL)
	routeBackend = route.NewCachedBackend(dsn, backendCache, baseTTL)

	//cachedBackend := cache.NewCachedBackend(processor.NewBackend(dsn), localCache, 5*time.Minute)
	return nil
}

func Startup(ctx context.Context) {
	var err error
	if err = SetupCachedBackend(); err != nil {
		panic(err)
	}

	// set up listener route such that events can be received from the NATS server and processed
	if subscriberRoute, err = rnats.NewRouteSubscriberUsingSelector(ctx, SelectorSubscriber, MessageCallback); err != nil {
		log.Fatalf("unable to create nats route subscriber: %v", err)
	}

	// setup other require routes, monitor, state sync, state router
	if monitorRoute, err = rnats.NewRouteUsingSelector(ctx, SelectorMonitor); err != nil {
		log.Fatalf("unable to create nats route: %v", err)
	}

	if syncRoute, err = rnats.NewRouteUsingSelector(ctx, SelectorStoreSync); err != nil {
		log.Fatalf("unable to initialize route: %v", err)
	}
}

func Teardown(ctx context.Context) {
	// Stop all batch writers and cleanup goroutines
	StopWriterCache()

	if backendCache != nil {
		backendCache.Close()
	}

	if err := subscriberRoute.Unsubscribe(ctx); err != nil {
		return
	}

	if err := subscriberRoute.Disconnect(ctx); err != nil {
		panic(err)
	}

	if err := syncRoute.Disconnect(ctx); err != nil {
		panic(err)
	}

	if err := monitorRoute.Disconnect(ctx); err != nil {
		panic(err)
	}

}
