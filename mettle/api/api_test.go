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
	queueID   = 1    // id of queues created in the next call to setupServers
)

func TestUncached(t *testing.T) {
	client, ex, s := setupServers(t)
	defer s.Stop()
	runExecution(t, client, ex, uncachedHash, 0)
}

func TestFlaky(t *testing.T) {
	client, ex, s := setupServers(t)
	defer s.Stop()

	flakyFail = true
	runExecution(t, client, ex, flakyHash, 1)
	flakyFail = false
	runExecution(t, client, ex, flakyHash, 0)
}

func TestTwiceFlaky(t *testing.T) {
	client, ex, s := setupServers(t)
	defer s.Stop()

	flakyFail = true
	runExecution(t, client, ex, flakyHash, 1)
	runExecution(t, client, ex, flakyHash, 1)
	flakyFail = false
	runExecution(t, client, ex, flakyHash, 0)
}

func TestExecuteAndWait(t *testing.T) {
	client, ex, s := setupServers(t)
	defer s.Stop()

	const hash = uncachedHash
	stream1, err := client.Execute(context.Background(), &pb.ExecuteRequest{
		ActionDigest: &pb.Digest{Hash: hash},
	})
	assert.NoError(t, err)
	op := receiveUpdate(t, stream1, hash, pb.ExecutionStage_QUEUED)

	stream2, err := client.WaitExecution(context.Background(), &pb.WaitExecutionRequest{
		Name: op.Name,
	})
	assert.NoError(t, err)
	receiveUpdate(t, stream2, hash, pb.ExecutionStage_QUEUED)

	assert.Equal(t, hash, ex.Receive().Hash)

	receiveUpdate(t, stream1, hash, pb.ExecutionStage_EXECUTING)
	receiveUpdate(t, stream2, hash, pb.ExecutionStage_EXECUTING)

	ex.Finish(&pb.Digest{Hash: hash})
	op1 := receiveUpdate(t, stream1, hash, pb.ExecutionStage_COMPLETED)
	op2 := receiveUpdate(t, stream2, hash, pb.ExecutionStage_COMPLETED)
	checkExitCode(t, op1, 0)
	checkExitCode(t, op2, 0)
}

func TestExecuteAndWaitTwice(t *testing.T) {
	client, ex, s := setupServers(t)
	defer s.Stop()

	const hash = uncachedHash
	stream1, err := client.Execute(context.Background(), &pb.ExecuteRequest{
		ActionDigest: &pb.Digest{Hash: hash},
	})
	assert.NoError(t, err)
	op := receiveUpdate(t, stream1, hash, pb.ExecutionStage_QUEUED)

	stream2, err := client.WaitExecution(context.Background(), &pb.WaitExecutionRequest{
		Name: op.Name,
	})
	assert.NoError(t, err)
	receiveUpdate(t, stream2, hash, pb.ExecutionStage_QUEUED)

	stream3, err := client.WaitExecution(context.Background(), &pb.WaitExecutionRequest{
		Name: op.Name,
	})
	assert.NoError(t, err)
	receiveUpdate(t, stream3, hash, pb.ExecutionStage_QUEUED)

	assert.Equal(t, hash, ex.Receive().Hash)

	receiveUpdate(t, stream1, hash, pb.ExecutionStage_EXECUTING)
	receiveUpdate(t, stream2, hash, pb.ExecutionStage_EXECUTING)
	receiveUpdate(t, stream3, hash, pb.ExecutionStage_EXECUTING)

	ex.Finish(&pb.Digest{Hash: hash})
	op1 := receiveUpdate(t, stream1, hash, pb.ExecutionStage_COMPLETED)
	op2 := receiveUpdate(t, stream2, hash, pb.ExecutionStage_COMPLETED)
	op3 := receiveUpdate(t, stream3, hash, pb.ExecutionStage_COMPLETED)
	checkExitCode(t, op1, 0)
	checkExitCode(t, op2, 0)
	checkExitCode(t, op3, 0)
}

func runExecution(t *testing.T, client pb.ExecutionClient, ex *executor, hash string, expectedExitCode int) {
	stream, err := client.Execute(context.Background(), &pb.ExecuteRequest{
		ActionDigest: &pb.Digest{Hash: hash},
	})
	assert.NoError(t, err)

	receiveUpdate(t, stream, hash, pb.ExecutionStage_QUEUED)
	assert.Equal(t, hash, ex.Receive().Hash)
	receiveUpdate(t, stream, hash, pb.ExecutionStage_EXECUTING)
	ex.Finish(&pb.Digest{Hash: hash})
	op := receiveUpdate(t, stream, hash, pb.ExecutionStage_COMPLETED)
	checkExitCode(t, op, expectedExitCode)
}

func receiveUpdate(t *testing.T, stream pb.Execution_ExecuteClient, expectedHash string, expectedStage pb.ExecutionStage_Value) *longrunning.Operation {
	log.Debug("Waiting for %s update...", expectedStage)
	op, metadata := recv(stream)
	assert.Equal(t, expectedStage, metadata.Stage)
	assert.Equal(t, expectedHash, metadata.ActionDigest.Hash)
	log.Debug("Received (hopefully) %s update", expectedStage)
	return op
}

func checkExitCode(t *testing.T, op *longrunning.Operation, expectedExitCode int) {
	response := &pb.ExecuteResponse{}
	err := ptypes.UnmarshalAny(op.GetResponse(), response)
	assert.NoError(t, err)
	assert.NotNil(t, response.Result)
	assert.EqualValues(t, expectedExitCode, response.Result.ExitCode)
}

func setupServers(t *testing.T) (pb.ExecutionClient, *executor, *grpc.Server) {
	requests := fmt.Sprintf("omem://requests%d", queueID)
	responses := fmt.Sprintf("omem://responses%d", queueID)
	queueID++
	return setupServersWithQueues(t, requests, responses)
}

func setupServersWithQueues(t *testing.T, requests, responses string) (pb.ExecutionClient, *executor, *grpc.Server) {
	common.MustOpenTopic(requests)  // Ensure these are created before anything tries
	common.MustOpenTopic(responses) // to open a subscription to either.
	s, lis, err := serve(grpcutil.Opts{
		Host: "127.0.0.1",
		Port: 0,
	}, "", requests, responses, responses, "", true)
	require.NoError(t, err)
	go s.Serve(lis)
	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	require.NoError(t, err)
	return pb.NewExecutionClient(conn), newExecutor(requests, responses), s
}

func TestGetExecutions(t *testing.T) {
	port := 9990
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

	jobs, err := getExecutions(opts, "127.0.0.1:9990", false)
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
