// Package common implements common functionality for both the API and worker servers.
package common

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/peterebden/go-cli-init/v3"
	"gocloud.dev/pubsub"
	pspb "google.golang.org/genproto/googleapis/pubsub/v1"

	// Must import the schemes we want to use.
	"github.com/thought-machine/please-servers/mettle/omempubsub"
	_ "gocloud.dev/pubsub/gcppubsub"
	_ "gocloud.dev/pubsub/mempubsub"
)

var log = cli.MustGetLogger()

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
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			log.Error("Failed to shut down queue: %s", err)
		}
		cancel()
	}()
}

type Shutdownable interface {
	Shutdown(context.Context) error
}

// PublishWithOrderingKey publishes a message and sets the ordering key if possible
// (i.e. if it is a GCP Pub/Sub message, otherwise no).
func PublishWithOrderingKey(ctx context.Context, topic *pubsub.Topic, body []byte, key string) error {
	return topic.Send(ctx, &pubsub.Message{
		Body: body,
		BeforeSend: func(asFunc func(interface{}) bool) error {
			var message *pspb.PubsubMessage
			if asFunc(message) {
				message.OrderingKey = key
				return nil
			}
			var om *omempubsub.OrderedMessage
			if asFunc(om) {
				om.Key = key
			}
			return nil
		},
	})
}
