// Package api implements the remote execution API server.
package api

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"time"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/peterebden/go-cli-init/v3"
	"github.com/prometheus/client_golang/prometheus"
	"gocloud.dev/pubsub"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"gopkg.in/op/go-logging.v1"

	"github.com/thought-machine/please-servers/grpcutil"
	"github.com/thought-machine/please-servers/mettle/common"
)

var log = cli.MustGetLogger()

const timeout = 10 * time.Second

// retentionTime is the length of time we retain completed actions for; after this we
// can no longer answer WaitExecution requests for them.
const retentionTime = 5 * time.Minute

var totalRequests = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "requests_total",
})
var currentRequests = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "mettle",
	Name:      "requests_current",
})

var serveHTTPOnce sync.Once

func init() {
	prometheus.MustRegister(totalRequests)
	prometheus.MustRegister(currentRequests)
}

// ServeForever serves on the given port until terminated.
func ServeForever(opts grpcutil.Opts, requestQueue, responseQueue, preResponseQueue string) {
	s, lis, err := serve(opts, requestQueue, responseQueue, preResponseQueue)
	if err != nil {
		log.Fatalf("%s", err)
	}
	grpcutil.ServeForever(lis, s)
}

func serve(opts grpcutil.Opts, requestQueue, responseQueue, preResponseQueue string) (*grpc.Server, net.Listener, error) {
	srv := &server{
		requests:     common.MustOpenTopic(requestQueue),
		responses:    common.MustOpenSubscription(responseQueue),
		preResponses: common.MustOpenTopic(preResponseQueue),
		jobs:         map[string]*job{},
	}
	go srv.Receive()
	lis, s := grpcutil.NewServer(opts)
	pb.RegisterCapabilitiesServer(s, srv)
	pb.RegisterExecutionServer(s, srv)
	serveHTTPOnce.Do(func() {
		http.HandleFunc("/executions", srv.ServeExecutions)
	})
	return s, lis, nil
}

type server struct {
	requests     *pubsub.Topic
	responses    *pubsub.Subscription
	preResponses *pubsub.Topic
	jobs         map[string]*job
	mutex        sync.Mutex
}

// ServeExecutions serves a list of currently executing jobs over HTTP.
func (s *server) ServeExecutions(w http.ResponseWriter, r *http.Request) {
	resp := map[string]string{}
	m := jsonpb.Marshaler{Indent: "  "}
	s.mutex.Lock()
	for k, v := range s.jobs {
		s, _ := m.MarshalToString(v.Current)
		resp[k] = s
	}
	s.mutex.Unlock()
	e := json.NewEncoder(w)
	e.SetIndent("", "  ")
	e.Encode(resp)
}

func (s *server) GetCapabilities(ctx context.Context, req *pb.GetCapabilitiesRequest) (*pb.ServerCapabilities, error) {
	return &pb.ServerCapabilities{
		ExecutionCapabilities: &pb.ExecutionCapabilities{
			DigestFunction: pb.DigestFunction_SHA256,
			ExecEnabled:    true,
		},
		LowApiVersion:  &semver.SemVer{Major: 2, Minor: 0},
		HighApiVersion: &semver.SemVer{Major: 2, Minor: 1}, // optimistic
	}, nil
}

func (s *server) Execute(req *pb.ExecuteRequest, stream pb.Execution_ExecuteServer) error {
	if req.ActionDigest == nil {
		return status.Errorf(codes.InvalidArgument, "Action digest not specified")
	}
	if md := s.contextMetadata(stream.Context()); md != nil {
		log.Notice("Received an ExecuteRequest for %s. Tool: %s %s Action id: %s Correlation ID: %s", req.ActionDigest.Hash, md.ToolDetails.ToolName, md.ToolDetails.ToolVersion, md.ActionId, md.CorrelatedInvocationsId)
	} else {
		log.Notice("Received an ExecuteRequest for %s", req.ActionDigest.Hash)
	}
	// N.B. We never try a cache lookup here because Please always tells us not to; it's not
	//      clear to me that is ever useful (because a good client will try to optimise by
	//      not uploading sources unnecessarily, and to work out that it can not do that it
	//      needs to contact the action cache itself anyway).
	ch := s.eventStream(req.ActionDigest, true)
	// Dispatch a pre-emptive response message to let our colleagues know we've queued it.
	// We will also receive & forward this message.
	any, _ := ptypes.MarshalAny(&pb.ExecuteOperationMetadata{
		Stage:        pb.ExecutionStage_QUEUED,
		ActionDigest: req.ActionDigest,
	})
	b, _ := proto.Marshal(&longrunning.Operation{
		Name:     req.ActionDigest.Hash,
		Metadata: any,
	})
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := common.PublishWithOrderingKey(ctx, s.preResponses, b, req.ActionDigest.Hash, ""); err != nil {
		log.Error("Failed to communicate pre-response message: %s", err)
	}
	b, _ = proto.Marshal(req)
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := s.requests.Send(ctx, &pubsub.Message{Body: b}); err != nil {
		log.Error("Failed to submit work to stream: %s", err)
		return err
	}
	return s.streamEvents(req.ActionDigest, ch, stream)
}

// contextMetadata returns the RequestMetadata associated with a context
func (s *server) contextMetadata(ctx context.Context) *pb.RequestMetadata {
	const key = "build.bazel.remote.execution.v2.requestmetadata-bin" // as defined by the proto
	msg := &pb.RequestMetadata{}
	if md, ok := metadata.FromIncomingContext(ctx); !ok {
		return nil
	} else if v := md.Get(key); len(v) == 0 {
		return nil
	} else if err := proto.Unmarshal([]byte(v[0]), msg); err != nil {
		log.Errorf("Invalid incoming metadata: %s", err)
		return nil
	} else if msg.ToolDetails == nil {
		msg.ToolDetails = &pb.ToolDetails{} // Ensure this is non-nil, it's easier for the receiver.
	}
	return msg
}

func (s *server) WaitExecution(req *pb.WaitExecutionRequest, stream pb.Execution_WaitExecutionServer) error {
	log.Info("Received a request to wait for %s", req.Name)
	digest := &pb.Digest{Hash: req.Name}
	ch := s.eventStream(digest, false)
	if ch == nil {
		log.Warning("Request for execution %s which is not in progress", req.Name)
		return status.Errorf(codes.NotFound, "No execution in progress for %s", req.Name)
	}
	return s.streamEvents(digest, ch, stream)
}

// streamEvents streams a series of events back to the client.
func (s *server) streamEvents(digest *pb.Digest, ch <-chan *longrunning.Operation, stream pb.Execution_ExecuteServer) error {
	for op := range ch {
		op.Name = digest.Hash
		if err := stream.Send(op); err != nil {
			log.Warning("Failed to forward event for %s: %s", digest.Hash, err)
			s.stopStream(digest, ch)
			return err
		}
	}
	log.Notice("Completed stream for %s", digest.Hash)
	return nil
}

// eventStream registers an event stream for a job.
func (s *server) eventStream(digest *pb.Digest, create bool) <-chan *longrunning.Operation {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	j, present := s.jobs[digest.Hash]
	if !present {
		if !create {
			return nil
		}
		any, _ := ptypes.MarshalAny(&pb.ExecuteOperationMetadata{
			Stage:        pb.ExecutionStage_QUEUED,
			ActionDigest: digest,
		})
		j = &job{Current: &longrunning.Operation{Metadata: any}}
		s.jobs[digest.Hash] = j
		log.Debug("Created job for %s", digest.Hash)
		totalRequests.Inc()
		currentRequests.Inc()
	}
	ch := make(chan *longrunning.Operation, 100)
	j.Streams = append(j.Streams, ch)
	if create {
		// This request is creating a new stream, clear out any existing current job info; it is now
		// at best irrelevant and at worst outdated.
		j.Current = nil
	} else if j.Current != nil {
		// This request is resuming an existing stream, give them an update on the latest thing to happen.
		// This helps avoid 504s from taking too long to send response headers since it can be an arbitrary
		// amount of time until we receive the next real update.
		ch <- j.Current
	}
	return ch
}

// stopStream de-registers the given event stream for a job.
func (s *server) stopStream(digest *pb.Digest, ch <-chan *longrunning.Operation) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	job, present := s.jobs[digest.Hash]
	if !present {
		log.Warning("stopStream for non-existent job %s", digest.Hash)
		return
	}
	for i, stream := range job.Streams {
		if stream == ch {
			job.Streams = append(job.Streams[:i], job.Streams[i+1:]...)
			break
		}
	}
}

// Receive runs forever, receiving responses from the queue.
func (s *server) Receive() {
	for {
		msg, err := s.responses.Receive(context.Background())
		if err != nil {
			log.Fatalf("Failed to receive message: %s", err)
		}
		s.process(msg)
		msg.Ack()
	}
}

// process processes a single message off the responses queue.
func (s *server) process(msg *pubsub.Message) {
	op := &longrunning.Operation{}
	metadata := &pb.ExecuteOperationMetadata{}
	if err := proto.Unmarshal(msg.Body, op); err != nil {
		log.Error("Failed to deserialise message: %s", err)
		return
	} else if err := ptypes.UnmarshalAny(op.Metadata, metadata); err != nil {
		log.Error("Failed to deserialise metadata: %s", err)
		return
	} else if metadata.ActionDigest == nil {
		log.Error("ActionDigest in received message is nil: %s", op)
		return
	}
	if op.Done {
		switch result := op.Result.(type) {
		case *longrunning.Operation_Response, *longrunning.Operation_Error:
		default:
			log.Error("Received a done response with neither response nor error field set: %#v", result)
		}
	}
	if log.IsEnabledFor(logging.DEBUG) {
		switch result := op.Result.(type) {
		case *longrunning.Operation_Response:
			response := &pb.ExecuteResponse{}
			if err := ptypes.UnmarshalAny(result.Response, response); err == nil && response.Status != nil && response.Status.Code != int32(codes.OK) {
				log.Debug("Got a failed update for %s: %s", metadata.ActionDigest.Hash, response.Status.Message)
			}
		case *longrunning.Operation_Error:
		}
	}
	worker := common.WorkerName(msg, "another server")
	log.Notice("Got an update for %s from %s, now %s", metadata.ActionDigest.Hash, worker, metadata.Stage)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if j, present := s.jobs[metadata.ActionDigest.Hash]; !present {
		// This is legit, we are getting an update about a job someone else started.
		log.Debug("Update for %s is for a previously unknown job", metadata.ActionDigest.Hash)
		s.jobs[metadata.ActionDigest.Hash] = &job{Current: op}
	} else if metadata.Stage != pb.ExecutionStage_QUEUED || !j.SentFirst {
		// Only send QUEUED messages if they're the first one. This prevents us from
		// re-broadcasting a QUEUED message after something's already started executing.
		j.SentFirst = true
		if j.Done && !op.Done {
			log.Warning("Got a progress message after completion for %s, discarding", metadata.ActionDigest.Hash)
			return
		} else if op.Done {
			j.Done = true
		}
		j.Current = op
		for _, stream := range j.Streams {
			// Invoke this in a goroutine so we do not block.
			go func(ch chan<- *longrunning.Operation) {
				defer func() {
					recover() // Avoid any chance of panicking from a 'send on closed channel'
				}()
				log.Debug("Dispatching update for %s", metadata.ActionDigest.Hash)
				ch <- op
				if op.Done {
					close(ch)
				}
			}(stream)
		}
		if op.Done {
			log.Info("Job %s completed by %s", metadata.ActionDigest.Hash, worker)
			go s.deleteJob(metadata.ActionDigest.Hash)
			currentRequests.Dec()
		}
	}
}

// deleteJob waits for a period then removes the given job from memory.
func (s *server) deleteJob(hash string) {
	time.Sleep(retentionTime)
	log.Debug("Removing job %s", hash)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.jobs, hash)
}

// A job represents a single execution request.
type job struct {
	Streams   []chan *longrunning.Operation
	Current   *longrunning.Operation
	SentFirst bool
	Done      bool
}
