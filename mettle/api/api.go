// Package api implements the remote execution API server.
package api

import (
	"context"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	clilogging "github.com/peterebden/go-cli-init/v4/logging"
	"github.com/prometheus/client_golang/prometheus"
	bpb "github.com/thought-machine/please-servers/proto/mettle"
	"gocloud.dev/pubsub"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"gopkg.in/op/go-logging.v1"

	"github.com/thought-machine/please-servers/grpcutil"
	"github.com/thought-machine/please-servers/mettle/common"
	"github.com/thought-machine/please-servers/rexclient"
)

var log = clilogging.MustGetLogger()

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

func init() {
	prometheus.MustRegister(totalRequests)
	prometheus.MustRegister(currentRequests)
}

// ServeForever serves on the given port until terminated.
func ServeForever(opts grpcutil.Opts, name, requestQueue, responseQueue, preResponseQueue, apiURL string, connTLS bool, allowedPlatform map[string][]string, storageURL string, storageTLS bool) {
	s, lis, err := serve(opts, name, requestQueue, responseQueue, preResponseQueue, apiURL, connTLS, allowedPlatform, storageURL, storageTLS)
	if err != nil {
		log.Fatalf("%s", err)
	}
	grpcutil.ServeForever(lis, s)
}

func serve(opts grpcutil.Opts, name, requestQueue, responseQueue, preResponseQueue, apiURL string, connTLS bool, allowedPlatform map[string][]string, storageURL string, storageTLS bool) (*grpc.Server, net.Listener, error) {
	if name == "" {
		name = "mettle API server"
	}
	log.Notice("Contacting CAS server on %s...", storageURL)
	client, err := rexclient.New(name, storageURL, storageTLS, "")
	if err != nil {
		return nil, nil, err
	}
	srv := &server{
		name:         name,
		requests:     common.MustOpenTopic(requestQueue),
		responses:    common.MustOpenSubscription(responseQueue),
		preResponses: common.MustOpenTopic(preResponseQueue),
		jobs:         map[string]*job{},
		platform:     allowedPlatform,
		client:       client,
	}
	log.Notice("Allowed platform values:")
	for k, v := range allowedPlatform {
		log.Notice("    %s: %s", k, strings.Join(v, ", "))
	}

	go srv.Receive()
	if jobs, err := getExecutions(opts, apiURL, connTLS); err != nil {
		log.Warningf("Failed to get inflight executions: %s", err)
	} else {
		srv.jobs = jobs
		log.Notice("Updated server with %d inflight executions", len(srv.jobs))
	}

	lis, s := grpcutil.NewServer(opts)
	pb.RegisterCapabilitiesServer(s, srv)
	pb.RegisterExecutionServer(s, srv)
	bpb.RegisterBootstrapServer(s, srv)
	return s, lis, nil
}

type server struct {
	name         string
	client       *client.Client
	requests     *pubsub.Topic
	responses    *pubsub.Subscription
	preResponses *pubsub.Topic
	jobs         map[string]*job
	platform     map[string][]string
	mutex        sync.Mutex
}

// ServeExecutions serves a list of currently executing jobs over GRPC.
func (s *server) ServeExecutions(ctx context.Context, req *bpb.ServeExecutionsRequest) (*bpb.ServeExecutionsResponse, error) {
	log.Notice("Received request for inflight executions")
	s.mutex.Lock()
	defer s.mutex.Unlock()
	executions := []*bpb.Job{}
	for k, v := range s.jobs {
		current, _ := proto.Marshal(v.Current)
		job := &bpb.Job{
			ID:        k,
			Current:   current,
			SentFirst: v.SentFirst,
			Done:      v.Done,
		}
		executions = append(executions, job)
	}
	res := &bpb.ServeExecutionsResponse{
		Jobs: executions,
	}
	log.Notice("Serving %d inflight executions", len(executions))
	return res, nil
}

// getExecutions requests a list of currently executing jobs over grpc
func getExecutions(opts grpcutil.Opts, apiURL string, connTLS bool) (map[string]*job, error) {
	if apiURL == "" {
		log.Notice("No API URL provided, will not request inflight executions")
		return map[string]*job{}, nil
	}
	conn, err := grpcutil.Dial(apiURL, connTLS, opts.CertFile, opts.TokenFile)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := bpb.NewBootstrapClient(conn)
	log.Notice("Requesting inflight executions...")
	req := &bpb.ServeExecutionsRequest{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := client.ServeExecutions(ctx, req)
	if err != nil {
		return nil, err
	}
	jobs := make(map[string]*job, len(res.Jobs))
	for _, j := range res.Jobs {
		current := &longrunning.Operation{}
		if err := proto.Unmarshal(j.Current, current); err != nil {
			log.Warningf("unable to unmarshal s%", j.ID)
			continue
		}
		jobs[j.ID] = &job{
			Current:   current,
			SentFirst: j.SentFirst,
			Done:      j.Done,
		}
	}
	return jobs, nil
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
	platform, err := s.validatePlatform(req)
	if err != nil {
		return err
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
	if err := common.PublishWithOrderingKey(ctx, s.preResponses, b, req.ActionDigest.Hash, s.name); err != nil {
		log.Error("Failed to communicate pre-response message: %s", err)
	}
	b, _ = proto.Marshal(req)
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := s.requests.Send(ctx, &pubsub.Message{
		Body:     b,
		Metadata: platform,
	}); err != nil {
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
	if !present && !create {
		return nil
	} else if !present || (!j.Successful() && create) {
		any, _ := ptypes.MarshalAny(&pb.ExecuteOperationMetadata{
			Stage:        pb.ExecutionStage_QUEUED,
			ActionDigest: digest,
		})
		j = &job{Current: &longrunning.Operation{Metadata: any}}
		s.jobs[digest.Hash] = j
		log.Debug("Created job for %s", digest.Hash)
		totalRequests.Inc()
		currentRequests.Inc()
	} else {
		log.Debug("Resuming existing job for %s", digest.Hash)
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
		if response := unmarshalResponse(op); response != nil && response.Status != nil && response.Status.Code != int32(codes.OK) {
			log.Debug("Got a failed update for %s: %s", metadata.ActionDigest.Hash, response.Status.Message)
		}
	}
	worker := common.WorkerName(msg)
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

// validatePlatform fetches the platform requirements for this request and checks them.
func (s *server) validatePlatform(req *pb.ExecuteRequest) (map[string]string, error) {
	action := &pb.Action{}
	if err := s.client.ReadProto(context.Background(), digest.NewFromProtoUnvalidated(req.ActionDigest), action); err != nil {
		return nil, err
	}
	if action.Platform == nil {
		return map[string]string{}, nil
	}
	m := make(map[string]string, len(action.Platform.Properties))
	for _, prop := range action.Platform.Properties {
		m[prop.Name] = prop.Value
		if len(s.platform) == 0 {
			continue // If nothing is specified as allowed, everything is permitted.
		}
		if allowed, present := s.platform[prop.Name]; !present {
			return nil, status.Errorf(codes.InvalidArgument, "Unsupported platform property %s", prop.Name)
		} else if !contains(allowed, prop.Value) {
			return nil, status.Errorf(codes.InvalidArgument, "Invalid platform property value %s, must be one of: %s", prop.Name, strings.Join(allowed, ", "))
		} else {
			log.Debug("Valid platform property %s: %s (from %s)", prop.Name, prop.Value, allowed)
		}
	}
	return m, nil
}

func contains(haystack []string, needle string) bool {
	for _, straw := range haystack {
		if straw == needle {
			return true
		}
	}
	return false
}

// A job represents a single execution request.
type job struct {
	Streams   []chan *longrunning.Operation
	Current   *longrunning.Operation
	SentFirst bool
	Done      bool
}

// Successful returns true if this job represents a successfully completed execution
func (j *job) Successful() bool {
	if !j.Done || j.Current == nil {
		return false
	}
	resp := unmarshalResponse(j.Current)
	return resp != nil && resp.Result != nil && resp.Result.ExitCode == 0
}

// unmarshalResponse retrieves the REAPI ExecuteResponse from a longrunning.Operation if it exists.
func unmarshalResponse(op *longrunning.Operation) *pb.ExecuteResponse {
	if result, ok := op.Result.(*longrunning.Operation_Response); ok {
		response := &pb.ExecuteResponse{}
		if err := ptypes.UnmarshalAny(result.Response, response); err != nil {
			log.Warning("Failed to unmarshal response: %s", err)
			return nil
		}
		return response
	}
	return nil
}
