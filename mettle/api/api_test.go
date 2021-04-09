package api

import (
	"context"
	"fmt"
	"os"
	"testing"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bpb "github.com/thought-machine/please-servers/proto/mettle"
	"gocloud.dev/pubsub"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"

	"github.com/thought-machine/please-servers/grpcutil"
	"github.com/thought-machine/please-servers/mettle/common"
)

const (
	uncachedHash = "1234"
	failedHash   = "3456"
	flakyHash    = "5678"
)

var (
	flakyFail = true // determines whether flakyHash passes or fails
)

func TestUncached(t *testing.T) {
	client, ex, s := setupServers(t, 9996, "omem://requests1", "omem://responses1")
	defer s.Stop()
	runExecution(t, client, ex, uncachedHash, 0)
}

func TestFlaky(t *testing.T) {
	client, ex, s := setupServers(t, 9996, "omem://requests2", "omem://responses2")
	defer s.Stop()

	flakyFail = true
	runExecution(t, client, ex, flakyHash, 1)
	flakyFail = false
	runExecution(t, client, ex, flakyHash, 0)
}

func runExecution(t *testing.T, client pb.ExecutionClient, ex *executor, hash string, expectedExitCode int) {
	stream, err := client.Execute(context.Background(), &pb.ExecuteRequest{
		ActionDigest: &pb.Digest{Hash: hash},
	})
	assert.NoError(t, err)

	receiveUpdate := func(expectedStage pb.ExecutionStage_Value) *longrunning.Operation {
		log.Debug("Waiting for %s update...", expectedStage)
		op, metadata := recv(stream)
		assert.Equal(t, expectedStage, metadata.Stage)
		assert.Equal(t, hash, metadata.ActionDigest.Hash)
		log.Debug("Received (hopefully) %s update", expectedStage)
		return op
	}

	receiveUpdate(pb.ExecutionStage_QUEUED)
	assert.Equal(t, hash, ex.Receive().Hash)
	receiveUpdate(pb.ExecutionStage_EXECUTING)
	ex.Finish(&pb.Digest{Hash: hash})
	op := receiveUpdate(pb.ExecutionStage_COMPLETED)
	response := &pb.ExecuteResponse{}
	err = ptypes.UnmarshalAny(op.GetResponse(), response)
	assert.NoError(t, err)
	assert.NotNil(t, response.Result)
	assert.EqualValues(t, expectedExitCode, response.Result.ExitCode)
}

func TestWaitExecution(t *testing.T) {
	// TODO(peterebden): We should really be using omem:// but the semantics are in some way different
	//                   that this test fails. I suspect this is a sign of some bad assumption here
	//                   (it tends to be more immediate then mem since it doesn't have the 250ms cooldown
	//                   thing and instead just blocks for arbitrary periods of time).
	client, ex, s := setupServers(t, 9999, "mem://requests3", "mem://responses3")
	defer s.Stop()

	digest := &pb.Digest{Hash: uncachedHash}
	stream, err := client.Execute(context.Background(), &pb.ExecuteRequest{
		ActionDigest: digest,
	})
	assert.NoError(t, err)

	op, metadata := recv(stream)
	assert.Equal(t, pb.ExecutionStage_QUEUED, metadata.Stage)
	assert.Equal(t, digest.Hash, metadata.ActionDigest.Hash)
	assert.Equal(t, digest.Hash, ex.Receive().Hash)

	// Now dial it up with WaitExecution, we should get the responses back on that too.
	stream2, err := client.WaitExecution(context.Background(), &pb.WaitExecutionRequest{
		Name: op.Name,
	})
	assert.NoError(t, err)

	// We re-receive the QUEUED notification on this stream.
	_, metadata = recv(stream2)
	assert.Equal(t, pb.ExecutionStage_QUEUED, metadata.Stage)
	assert.EqualValues(t, digest.Hash, metadata.ActionDigest.Hash)

	_, metadata = recv(stream2)
	assert.Equal(t, pb.ExecutionStage_EXECUTING, metadata.Stage)
	assert.EqualValues(t, digest.Hash, metadata.ActionDigest.Hash)

	ex.Finish(digest)
	op, metadata = recv(stream2)
	assert.Equal(t, pb.ExecutionStage_COMPLETED, metadata.Stage)
	assert.Equal(t, digest.Hash, metadata.ActionDigest.Hash)
	response := &pb.ExecuteResponse{}
	err = ptypes.UnmarshalAny(op.GetResponse(), response)
	assert.NoError(t, err)
	assert.NotNil(t, response.Result)
	assert.EqualValues(t, 0, response.Result.ExitCode)
}

func setupServers(t *testing.T, port int, requests, responses string) (pb.ExecutionClient, *executor, *grpc.Server) {
	common.MustOpenTopic(requests)  // Ensure these are created before anything tries
	common.MustOpenTopic(responses) // to open a subscription to either.
	s, lis, err := serve(grpcutil.Opts{
		Host: "127.0.0.1",
		Port: port,
	}, "", requests, responses, responses, "", true)
	require.NoError(t, err)
	go s.Serve(lis)
	conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithInsecure())
	require.NoError(t, err)
	return pb.NewExecutionClient(conn), newExecutor(requests, responses), s
}

func TestGetExecutions(t *testing.T) {
	port := 9999
	opts := grpcutil.Opts{
		Host:      "127.0.0.1",
		Port:      port,
		CertFile:  "",
		TokenFile: "",
	}
	srv := &server{
		name: "mettle API server",
		jobs: loadJob(),
	}
	lis, s := grpcutil.NewServer(opts)
	bpb.RegisterBootstrapServer(s, srv)
	go s.Serve(lis)

	jobs, err := getExecutions(opts, "127.0.0.1:9999", false)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(jobs))
	assert.Equal(t, "Unfinished Operation", jobs["1234"].Current.Name)
}

func loadJob() map[string]*job {
	jobs := map[string]*job{
		"1234": {
			Current: &longrunning.Operation{
				Name: "Unfinished Operation",
				Done: true,
			},
			SentFirst: true,
			Done:      false,
		},
		"5678": {
			Current: &longrunning.Operation{
				Name: "Finished Operation",
				Done: true,
			},
			SentFirst: true,
			Done:      true,
		},
	}
	return jobs
}

func recv(stream pb.Execution_ExecuteClient) (*longrunning.Operation, *pb.ExecuteOperationMetadata) {
	op, err := stream.Recv()
	if err != nil {
		log.Fatalf("Failed to receive message: %s", err)
	}
	metadata := &pb.ExecuteOperationMetadata{}
	if err := ptypes.UnmarshalAny(op.Metadata, metadata); err != nil {
		log.Fatalf("Failed to deserialise metadata: %s", err)
	}
	return op, metadata
}

type executor struct {
	requests  *pubsub.Subscription
	responses *pubsub.Topic
}

func newExecutor(requests, responses string) *executor {
	return &executor{
		requests:  common.MustOpenSubscription(requests),
		responses: common.MustOpenTopic(responses),
	}
}

// Receive receives the next request from the queue and begins "execution" of it.
func (ex *executor) Receive() *pb.Digest {
	msg, _ := ex.requests.Receive(context.Background())
	req := &pb.ExecuteRequest{}
	if err := proto.Unmarshal(msg.Body, req); err != nil {
		log.Fatalf("Failed to deserialise message: %s", err)
	}
	metadata, _ := ptypes.MarshalAny(&pb.ExecuteOperationMetadata{
		Stage:        pb.ExecutionStage_EXECUTING,
		ActionDigest: req.ActionDigest,
	})
	b, _ := proto.Marshal(&longrunning.Operation{
		Name:     req.ActionDigest.Hash,
		Metadata: metadata,
	})
	ex.responses.Send(context.Background(), &pubsub.Message{Body: b})
	msg.Ack()
	return req.ActionDigest
}

// Finish "completes" execution and sends a response.
func (ex *executor) Finish(digest *pb.Digest) {
	metadata, _ := ptypes.MarshalAny(&pb.ExecuteOperationMetadata{
		Stage:        pb.ExecutionStage_COMPLETED,
		ActionDigest: digest,
	})
	if digest.Hash == failedHash || (digest.Hash == flakyHash && flakyFail) {
		response, _ := ptypes.MarshalAny(&pb.ExecuteResponse{
			Result: &pb.ActionResult{
				ExitCode: 1,
			},
		})
		b, _ := proto.Marshal(&longrunning.Operation{
			Name:     digest.Hash,
			Metadata: metadata,
			Done:     true,
			Result:   &longrunning.Operation_Response{Response: response},
		})
		ex.responses.Send(context.Background(), &pubsub.Message{Body: b})
		flakyFail = !flakyFail
	} else {
		response, _ := ptypes.MarshalAny(&pb.ExecuteResponse{
			Result: &pb.ActionResult{},
		})
		b, _ := proto.Marshal(&longrunning.Operation{
			Name:     digest.Hash,
			Metadata: metadata,
			Done:     true,
			Result:   &longrunning.Operation_Response{Response: response},
		})
		ex.responses.Send(context.Background(), &pubsub.Message{Body: b})
	}
}

func TestMain(m *testing.M) {
	grpcutil.Silence()
	os.Exit(m.Run())
}
