// Package common implements common functionality for both the API and worker servers.
package common

import (
	"context"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/peterebden/go-cli-init/v4/logging"
	"gocloud.dev/pubsub"
	pspb "google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	// Must import the schemes we want to use.
	_ "github.com/thought-machine/please-servers/mettle/gcppubsub"
	"github.com/thought-machine/please-servers/mettle/mempubsub"
)

var log = logging.MustGetLogger()

// For hacking around the fact that mempubsub doesn't allow reopening the same subscription (each call creates a new one)
// In production use this makes no real difference since we never open more than one per process.
var subscriptions = map[string]*pubsub.Subscription{}
var subMutex sync.Mutex

// workerKey is the metadata key we set to identify workers in messages.
const workerKey = "build.please.mettle.worker"

// MustOpenSubscription opens a subscription, which must have been created ahead of time.
// It dies on any errors.
func MustOpenSubscription(url string) *pubsub.Subscription {
	url = renameURL(url)
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
	url = renameURL(url)
	t, err := pubsub.OpenTopic(context.Background(), url)
	if err != nil {
		log.Fatalf("Failed to open topic %s: %s", url, err)
	}
	log.Debug("Opened topic %s", url)
	return t
}

// renameURL maps from old names (omem and gcprpubsub) to new ones (mem and gcppubsub)
func renameURL(url string) string {
	if strings.HasPrefix(url, "gcprpubsub://") {
		return "gcppubsub://" + strings.TrimPrefix(url, "gcprpubsub://")
	} else if strings.HasPrefix(url, "gcprpubsub://") {
		return "gcppubsub://" + strings.TrimPrefix(url, "gcprpubsub://")
	}
	return url
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
// The worker name will be attached to the outgoing message as metadata.
func PublishWithOrderingKey(ctx context.Context, topic *pubsub.Topic, body []byte, key, workerName string) error {
	return topic.Send(ctx, &pubsub.Message{
		Body: body,
		BeforeSend: func(asFunc func(interface{}) bool) error {
			var message *pspb.PubsubMessage
			if asFunc(&message) {
				message.OrderingKey = key
				return nil
			}
			var om *mempubsub.OrderedMessage
			if asFunc(&om) {
				om.Key = key
				return nil
			}
			log.Warning("Failed to set ordering key on message")
			return nil
		},
		Metadata: map[string]string{workerKey: workerName},
	})
}

// WorkerName returns the name of a worker associated with a message, or the empty string if there isn't one.
func WorkerName(msg *pubsub.Message) string {
	if msg.Metadata != nil {
		if worker, present := msg.Metadata[workerKey]; present {
			return worker
		}
	}
	return ""
}

// CheckOutputPaths checks that all output paths are OK (i.e. don't contain ../ or other such naughtiness)
func CheckOutputPaths(cmd *pb.Command) error {
	// Check OutputDirectories and OutputFiles although we don't normally use them (the SDK might choose to).
	if err := checkOutputPaths(cmd.OutputDirectories); err != nil {
		return err
	}
	if err := checkOutputPaths(cmd.OutputFiles); err != nil {
		return err
	}
	if err := checkOutputPaths(cmd.OutputPaths); err != nil {
		return err
	}
	return nil
}

func checkOutputPaths(paths []string) error {
	for _, path := range paths {
		if err := CheckPath(path); err != nil {
			return err
		}
	}
	return nil
}

// CheckPath checks that an individual input or output path doesn't contain any illegal entities.
func CheckPath(path string) error {
	for _, part := range strings.Split(path, string(filepath.Separator)) {
		if part == ".." {
			return status.Errorf(codes.InvalidArgument, "Output path %s attempts directory traversal", path)
		}
	}
	if strings.HasPrefix(path, "/") {
		return status.Errorf(codes.InvalidArgument, "Output path %s is absolute, that's not permitted", path)
	}
	return nil
}
