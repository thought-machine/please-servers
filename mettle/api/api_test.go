package api

import (
	"context"
	"fmt"
	"testing"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gocloud.dev/pubsub"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"

	"github.com/thought-machine/please-servers/grpcutil"
	"github.com/thought-machine/please-servers/mettle/common"
)

const (
	uncachedHash = "1234"
	failedHash   = "3456"
)

func TestUncached(t *testing.T) {
	client, ex, s := setupServers(t, 9996, "omem://requests1", "omem://responses1")
	defer s.Stop()

	digest := &pb.Digest{Hash: uncachedHash}
	stream, err := client.Execute(context.Background(), &pb.ExecuteRequest{
		ActionDigest: digest,
	})
	assert.NoError(t, err)

	_, metadata := recv(stream)
	assert.Equal(t, pb.ExecutionStage_QUEUED, metadata.Stage)
	assert.Equal(t, digest.Hash, metadata.ActionDigest.Hash)
	assert.Equal(t, digest.Hash, ex.Receive().Hash)

	_, metadata = recv(stream)
	assert.Equal(t, pb.ExecutionStage_EXECUTING, metadata.Stage)
	assert.EqualValues(t, digest.Hash, metadata.ActionDigest.Hash)

	ex.Finish(digest)
	op, metadata := recv(stream)
	assert.Equal(t, pb.ExecutionStage_COMPLETED, metadata.Stage)
	assert.Equal(t, digest.Hash, metadata.ActionDigest.Hash)
	response := &pb.ExecuteResponse{}
	err = ptypes.UnmarshalAny(op.GetResponse(), response)
	assert.NoError(t, err)
	assert.NotNil(t, response.Result)
	assert.EqualValues(t, 0, response.Result.ExitCode)
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
	}, "", requests, responses, responses)
	require.NoError(t, err)
	go s.Serve(lis)
	conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithInsecure())
	require.NoError(t, err)
	return pb.NewExecutionClient(conn), newExecutor(requests, responses), s
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
	return req.ActionDigest
}

// Finish "completes" execution and sends a response.
func (ex *executor) Finish(digest *pb.Digest) {
	metadata, _ := ptypes.MarshalAny(&pb.ExecuteOperationMetadata{
		Stage:        pb.ExecutionStage_COMPLETED,
		ActionDigest: digest,
	})
	if digest.Hash == failedHash {
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
