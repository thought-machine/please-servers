// Package common implements common functionality for both the API and worker servers.
package common

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"gocloud.dev/pubsub"
	"gopkg.in/op/go-logging.v1"

	// Must import the schemes we want to use.
	_ "gocloud.dev/pubsub/gcppubsub"
	_ "gocloud.dev/pubsub/mempubsub"
)

var log = logging.MustGetLogger("common")

// For hacking around the fact that mempubsub doesn't allow reopening the same subscription (each call creates a new one)
// In production use this makes no real difference since we never open more than one per process.
var subscriptions = map[string]*pubsub.Subscription{}
var subMutex sync.Mutex

// MustOpenSubscription opens a subscription, which must have been created ahead of time.
// It dies on any errors.
func MustOpenSubscription(url string) *pubsub.Subscription {
	subMutex.Lock()
	defer subMutex.Unlock()
	if sub, present := subscriptions[url]; present {
		log.Debug("Re-opened existing subscription to %s", url)
		return sub
	}
	ctx, cancel := context.WithCancel(context.Background())
	s, err := pubsub.OpenSubscription(ctx, url)
	if err != nil {
		log.Fatalf("Failed to open subscription %s: %s", url, err)
	}
	log.Debug("Opened subscription to %s", url)
	handleSignals(cancel, s)
	subscriptions[url] = s
	return s
}

// MustOpenTopic opens a topic, which must have been created ahead of time.
func MustOpenTopic(url string) *pubsub.Topic {
	ctx, cancel := context.WithCancel(context.Background())
	t, err := pubsub.OpenTopic(ctx, url)
	if err != nil {
		log.Fatalf("Failed to open topic %s: %s", url, err)
	}
	log.Debug("Opened topic %s", url)
	handleSignals(cancel, t)
	return t
}

func handleSignals(cancel context.CancelFunc, s Shutdownable) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGTERM)
	go func() {
		log.Warning("Received signal %s, shutting down queue", <-ch)
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		if err := s.Shutdown(ctx); err != nil {
			log.Error("Failed to shut down queue: %s", err)
		}
		cancel()
		log.Fatalf("Shutting down server")
	}()
}

type Shutdownable interface {
	Shutdown(context.Context) error
}
