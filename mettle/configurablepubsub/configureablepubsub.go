package configurablepubsub

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"

	"gocloud.dev/gcp"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/gcppubsub"
)

const Scheme = "configurable-gcppubsub"

var topicPathRE = regexp.MustCompile("^projects/.+/topics/.+$")

func init() {
	uo := &urlOpener{}
	pubsub.DefaultURLMux().RegisterTopic(Scheme, uo)
}

type urlOpener struct {
}

func (uo *urlOpener) OpenTopicURL(ctx context.Context, u *url.URL) (*pubsub.Topic, error) {
	opts := gcppubsub.TopicOptions{}
	for param, value := range u.Query() {
		switch param {
		case "max_send_batch_size":
			maxBatchSize, err := queryParameterInt(value)
			if err != nil {
				return nil, fmt.Errorf("open topic %v: invalid query parameter %q: %v", u, param, err)
			}

			if maxBatchSize <= 0 || maxBatchSize > 1000 {
				return nil, fmt.Errorf("open topic %v: invalid query parameter %q: must be between 1 and 1000", u, param)
			}

			opts.BatcherOptions.MaxBatchSize = maxBatchSize
		case "num_handlers":
			maxHandlers, err := queryParameterInt(value)
			if err != nil {
				return nil, fmt.Errorf("open topic %v: invalid query parameter %q: %v", u, param, err)
			}
			if maxHandlers < 1 {
				return nil, fmt.Errorf("open topic %v: numHandlers cannot be less than 1")
			}
			opts.BatcherOptions.MaxHandlers = maxHandlers
		default:
			return nil, fmt.Errorf("open topic %v: invalid query parameter %q", u, param)
		}
	}

	creds, err := gcp.DefaultCredentials(ctx)
	if err != nil {
		return nil, err
	}
	conn, _, err := gcppubsub.Dial(ctx, creds.TokenSource)
	if err != nil {
		return nil, err
	}

	pc, err := gcppubsub.PublisherClient(ctx, conn)
	if err != nil {
		return nil, err
	}
	topicPath := path.Join(u.Host, u.Path)
	if topicPathRE.MatchString(topicPath) {
		return gcppubsub.OpenTopicByPath(pc, topicPath, &opts)
	}
	// Shortened form?
	topicName := strings.TrimPrefix(u.Path, "/")
	return gcppubsub.OpenTopic(pc, gcp.ProjectID(u.Host), topicName, &opts), nil
}

func queryParameterInt(value []string) (int, error) {
	if len(value) > 1 {
		return 0, fmt.Errorf("expected only one parameter value, got: %v", len(value))
	}

	return strconv.Atoi(value[0])
}
