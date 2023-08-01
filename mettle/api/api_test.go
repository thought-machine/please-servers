package api

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bpb "github.com/thought-machine/please-servers/proto/mettle"
	"gocloud.dev/pubsub"
	bs "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

func TestCreateNewInsteadOfResume(t *testing.T) {
	client, ex, s := setupServers(t)
	defer s.Stop()
	runExecution(t, client, ex, uncachedHash, 0)

	// Temporarily set this to zero so we don't have to wait ten minutes in this test.
	old := resumptionTime
	resumptionTime = 0
	defer func() { resumptionTime = old }()

	// We should now get a complete new execution since this job counts as 'expired'
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
	receiveUpdate(t, stream1, hash, pb.ExecutionStage_CACHE_CHECK)
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
	receiveUpdate(t, stream1, hash, pb.ExecutionStage_CACHE_CHECK)
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

func TestExecuteAndWaitLater(t *testing.T) {
	client, ex, s := setupServers(t)
	defer s.Stop()

	const hash = uncachedHash
	stream1, err := client.Execute(context.Background(), &pb.ExecuteRequest{
		ActionDigest: &pb.Digest{Hash: hash},
	})
	assert.NoError(t, err)
	receiveUpdate(t, stream1, hash, pb.ExecutionStage_CACHE_CHECK)
	op := receiveUpdate(t, stream1, hash, pb.ExecutionStage_QUEUED)

	assert.Equal(t, hash, ex.Receive().Hash)
	receiveUpdate(t, stream1, hash, pb.ExecutionStage_EXECUTING)

	stream2, err := client.WaitExecution(context.Background(), &pb.WaitExecutionRequest{
		Name: op.Name,
	})
	assert.NoError(t, err)
	receiveUpdate(t, stream2, hash, pb.ExecutionStage_EXECUTING)

	ex.Finish(&pb.Digest{Hash: hash})
	op1 := receiveUpdate(t, stream1, hash, pb.ExecutionStage_COMPLETED)
	op2 := receiveUpdate(t, stream2, hash, pb.ExecutionStage_COMPLETED)
	checkExitCode(t, op1, 0)
	checkExitCode(t, op2, 0)
}

func TestExecuteAndWaitAfterCompletion(t *testing.T) {
	client, ex, s := setupServers(t)
	defer s.Stop()

	const hash = uncachedHash
	stream1, err := client.Execute(context.Background(), &pb.ExecuteRequest{
		ActionDigest: &pb.Digest{Hash: hash},
	})
	assert.NoError(t, err)
	receiveUpdate(t, stream1, hash, pb.ExecutionStage_CACHE_CHECK)
	op := receiveUpdate(t, stream1, hash, pb.ExecutionStage_QUEUED)
	assert.Equal(t, hash, ex.Receive().Hash)
	receiveUpdate(t, stream1, hash, pb.ExecutionStage_EXECUTING)
	ex.Finish(&pb.Digest{Hash: hash})
	op1 := receiveUpdate(t, stream1, hash, pb.ExecutionStage_COMPLETED)
	checkExitCode(t, op1, 0)

	stream2, err := client.WaitExecution(context.Background(), &pb.WaitExecutionRequest{
		Name: op.Name,
	})
	assert.NoError(t, err)
	op2 := receiveUpdate(t, stream2, hash, pb.ExecutionStage_COMPLETED)
	checkExitCode(t, op2, 0)
}

func runExecution(t *testing.T, client pb.ExecutionClient, ex *executor, hash string, expectedExitCode int) {
	stream, err := client.Execute(context.Background(), &pb.ExecuteRequest{
		ActionDigest: &pb.Digest{Hash: hash},
	})
	assert.NoError(t, err)

	receiveUpdate(t, stream, hash, pb.ExecutionStage_CACHE_CHECK)
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
	log.Debug("Received %s update (expected %s)", metadata.Stage, expectedStage)
	if expectedStage == pb.ExecutionStage_COMPLETED {
		// The stream should also have ended at this point.
		_, err := stream.Recv()
		assert.Error(t, err)
		assert.Equal(t, err, io.EOF)
	}
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
	casaddr := setupCASServer()
	requests := fmt.Sprintf("mem://requests%d", queueID)
	responses := fmt.Sprintf("mem://responses%d", queueID)
	queueID++
	common.MustOpenTopic(requests)  // Ensure these are created before anything tries
	common.MustOpenTopic(responses) // to open a subscription to either.
	s, lis, err := serve(grpcutil.Opts{
		Host: "127.0.0.1",
		Port: 0,
	}, "", requests, responses, responses, "", true, map[string][]string{}, casaddr, false, 1)
	require.NoError(t, err)
	go s.Serve(lis)
	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	require.NoError(t, err)
	return pb.NewExecutionClient(conn), newExecutor(requests, responses), s
}

func setupCASServer() string {
	srv := cas{}
	lis, s := grpcutil.NewServer(grpcutil.Opts{
		Host: "127.0.0.1",
		Port: 0,
	})
	bs.RegisterByteStreamServer(s, srv)
	pb.RegisterCapabilitiesServer(s, srv)
	go s.Serve(lis)
	return lis.Addr().String()
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

// A cas is a fake implementation of the CAS server.
type cas struct{}

func (c cas) Read(req *bs.ReadRequest, srv bs.ByteStream_ReadServer) error {
	return status.Errorf(codes.NotFound, "blob %s not found", req.ResourceName)
}

func (c cas) Write(srv bs.ByteStream_WriteServer) error {
	return status.Errorf(codes.Unimplemented, "bytestream.Write not implemented")
}

func (c cas) QueryWriteStatus(ctx context.Context, req *bs.QueryWriteStatusRequest) (*bs.QueryWriteStatusResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "bytestream.QueryWriteStatus not implemented")
}

func (c cas) GetCapabilities(ctx context.Context, req *pb.GetCapabilitiesRequest) (*pb.ServerCapabilities, error) {
	return &pb.ServerCapabilities{
		CacheCapabilities: &pb.CacheCapabilities{
			DigestFunctions: []pb.DigestFunction_Value{
				pb.DigestFunction_SHA256,
			},
			ActionCacheUpdateCapabilities: &pb.ActionCacheUpdateCapabilities{
				UpdateEnabled: true,
			},
			SupportedCompressors: []pb.Compressor_Value{
				pb.Compressor_IDENTITY,
				pb.Compressor_ZSTD,
			},
			SupportedBatchUpdateCompressors: []pb.Compressor_Value{
				pb.Compressor_IDENTITY,
				pb.Compressor_ZSTD,
			},
		},
	}, nil
}

func TestMain(m *testing.M) {
	grpcutil.Silence()
	os.Exit(m.Run())
}
