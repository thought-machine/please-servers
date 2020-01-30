// Package worker implements the worker side of Mettle.
package worker

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/chunker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	sdkdigest "github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/tree"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/prometheus/client_golang/prometheus"
	"gocloud.dev/pubsub"
	"google.golang.org/genproto/googleapis/longrunning"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"gopkg.in/op/go-logging.v1"

	"github.com/thought-machine/please-servers/mettle/common"
	bbcas "github.com/thought-machine/please-servers/third_party/proto/cas"
)

var log = logging.MustGetLogger("worker")

const timeout = 3 * time.Minute

var totalBuilds = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "builds_total",
})
var currentBuilds = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "mettle",
	Name:      "builds_current",
})
var executeDurations = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "mettle",
	Name:      "build_durations_secs",
	Buckets:   []float64{1, 2, 5, 10, 20, 50, 200, 500},
})
var fetchDurations = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "mettle",
	Name:      "fetch_durations_secs",
	Buckets:   []float64{1, 2, 5, 10, 20, 50, 200, 500},
})
var uploadDurations = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "mettle",
	Name:      "upload_durations_secs",
	Buckets:   []float64{1, 2, 5, 10, 20, 50, 200, 500},
})
var peakMemory = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "mettle",
	Name:      "peak_memory_usage_mb",
	Buckets:   []float64{50, 200, 500, 1000, 5000, 10000},
})
var cpuUsage = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "mettle",
	Name:      "cpu_usage_per_sec",
	Buckets:   []float64{0.2, 0.5, 1.0, 2.0, 5.0},
})

func init() {
	prometheus.MustRegister(totalBuilds)
	prometheus.MustRegister(currentBuilds)
	prometheus.MustRegister(executeDurations)
	prometheus.MustRegister(fetchDurations)
	prometheus.MustRegister(uploadDurations)
	prometheus.MustRegister(peakMemory)
	prometheus.MustRegister(cpuUsage)
}

// RunForever runs the worker, receiving jobs until terminated.
func RunForever(requestQueue, responseQueue, name, storage, dir, browserURL, sandbox string, clean, secureStorage bool) {
	if err := runForever(requestQueue, responseQueue, name, storage, dir, browserURL, sandbox, clean, secureStorage); err != nil {
		log.Fatalf("Failed to run: %s", err)
	}
}

func runForever(requestQueue, responseQueue, name, storage, dir, browserURL, sandbox string, clean, secureStorage bool) error {
	// Make sure we have a directory to run in
	if err := os.MkdirAll(dir, os.ModeDir|0755); err != nil {
		return fmt.Errorf("Failed to create working directory: %s", err)
	}
	// If no name is given, default to the hostname.
	if name == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("Failed to determine hostname, must pass --name explicitly: %s", err)
		}
		name = hostname
		log.Notice("This is %s", name)
	}
	// Check this exists upfront
	if sandbox != "" {
		if _, err := os.Stat(sandbox); err != nil {
			return fmt.Errorf("Error checking sandbox tool: %s", err)
		}
	}
	client, err := client.NewClient(context.Background(), "mettle", client.DialParams{
		Service:            storage,
		NoSecurity:         !secureStorage,
		TransportCredsOnly: secureStorage,
		DialOpts:           []grpc.DialOption{grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(419430400))},
	})
	if err != nil {
		return err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	abspath, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("Failed to make path absolute: %s", err)
	}
	w := &worker{
		requests:   common.MustOpenSubscription(requestQueue),
		responses:  common.MustOpenTopic(responseQueue),
		client:     client,
		rootDir:    abspath,
		clean:      clean,
		home:       home,
		name:       name,
		sandbox:    sandbox,
		limiter:    make(chan struct{}, downloadParallelism),
		browserURL: browserURL,
	}
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGTERM)
	go func() {
		log.Warning("Received signal %s, shutting down when ready...", <-ch)
		cancel()
		log.Fatalf("Received another signal %s, shutting down immediately", <-ch)
	}()
	for {
		if err := w.RunTask(ctx); err != nil {
			// If we get an error back here, we have failed to communicate with one of
			// our queues, so we are basically doomed and should stop.
			return fmt.Errorf("Failed to run task: %s", err)
		}
	}
}

type worker struct {
	requests     *pubsub.Subscription
	responses    *pubsub.Topic
	client       *client.Client
	dir, rootDir string
	home         string
	name         string
	browserURL   string
	sandbox      string
	actionDigest *pb.Digest
	metadata     *pb.ExecutedActionMetadata
	clean        bool

	// For limiting parallelism during download actions
	limiter chan struct{}
}

// RunTask runs a single task.
// Note that it only returns errors for reasons this service controls (i.e. queue comms),
// failures at actually running the task are communicated back on the responses queue.
func (w *worker) RunTask(ctx context.Context) error {
	log.Notice("Waiting for next task...")
	msg, err := w.requests.Receive(ctx)
	if err != nil {
		log.Error("Error receiving message: %s", err)
		return err
	}
	// Mark message as consumed now. Alternatively we could not ack it until we
	// run the command, but we *probably* want to do that kind of retrying at a
	// higher level. TBD.
	msg.Ack()
	response := w.runTask(msg.Body)
	return w.update(pb.ExecutionStage_COMPLETED, response)
}

// runTask does the actual running of a task.
func (w *worker) runTask(msg []byte) *pb.ExecuteResponse {
	totalBuilds.Inc()
	currentBuilds.Inc()
	defer currentBuilds.Dec()
	w.metadata = &pb.ExecutedActionMetadata{
		Worker:               w.name,
		WorkerStartTimestamp: ptypes.TimestampNow(),
	}
	req, action, command, status := w.readRequest(msg)
	if req != nil {
		w.actionDigest = req.ActionDigest
	}
	if status != nil {
		log.Error("Bad request: %s", status)
		return &pb.ExecuteResponse{
			Result: &pb.ActionResult{},
			Status: status,
		}
	}
	log.Notice("Received task for action digest %s", w.actionDigest.Hash)
	w.actionDigest = req.ActionDigest
	if status := w.prepareDir(action, command); status != nil {
		log.Warning("Failed to prepare directory for action digest %s: %s", w.actionDigest.Hash, status)
		return &pb.ExecuteResponse{
			Result: &pb.ActionResult{},
			Status: status,
		}
	}
	return w.execute(action, command)
}

// readRequest unmarshals the original execution request.
func (w *worker) readRequest(msg []byte) (*pb.ExecuteRequest, *pb.Action, *pb.Command, *rpcstatus.Status) {
	req := &pb.ExecuteRequest{}
	action := &pb.Action{}
	command := &pb.Command{}
	if err := proto.Unmarshal(msg, req); err != nil {
		return nil, nil, nil, status(codes.FailedPrecondition, "Badly serialised request: %s", err)
	} else if err := w.readBlobToProto(req.ActionDigest, action); err != nil {
		return req, nil, nil, status(codes.FailedPrecondition, "Invalid action digest: %s", err)
	} else if err := w.readBlobToProto(action.CommandDigest, command); err != nil {
		return req, nil, nil, status(codes.FailedPrecondition, "Invalid command digest: %s", err)
	}
	return req, action, command, nil
}

// prepareDir prepares the directory for executing this request.
func (w *worker) prepareDir(action *pb.Action, command *pb.Command) *rpcstatus.Status {
	log.Info("Preparing directory for %s", w.actionDigest.Hash)
	w.update(pb.ExecutionStage_EXECUTING, nil)
	dir, err := ioutil.TempDir(w.rootDir, "mettle")
	if err != nil {
		return status(codes.Internal, "Failed to create temp dir: %s", err)
	}
	w.dir = dir
	start := time.Now()
	w.metadata.InputFetchStartTimestamp = toTimestamp(start)
	if err := w.downloadDirectory(w.dir, action.InputRootDigest); err != nil {
		return status(codes.Internal, "Failed to download input root: %s", err)
	}
	// We are required to create directories for all the outputs.
	for _, out := range command.OutputPaths {
		if dir := path.Dir(out); out != "" && out != "." {
			if err := os.MkdirAll(path.Join(w.dir, dir), os.ModeDir|0755); err != nil {
				return status(codes.Internal, "Failed to create directory: %s", err)
			}
		}
	}
	end := time.Now()
	w.metadata.InputFetchCompletedTimestamp = toTimestamp(end)
	fetchDurations.Observe(end.Sub(start).Seconds())
	log.Info("Prepared directory for %s", w.actionDigest.Hash)
	return nil
}

// execute runs the actual commands once the inputs are prepared.
func (w *worker) execute(action *pb.Action, command *pb.Command) *pb.ExecuteResponse {
	log.Notice("Beginning execution for %s: %s", w.actionDigest.Hash, command.Arguments)
	if w.clean {
		defer func() {
			if err := os.RemoveAll(w.dir); err != nil {
				log.Error("Failed to clean workdir: %s", err)
			}
		}()
	}
	if w.sandbox != "" && w.shouldSandbox(command) {
		command.Arguments = append([]string{w.sandbox}, command.Arguments...)
	}
	start := time.Now()
	w.metadata.ExecutionStartTimestamp = toTimestamp(start)
	duration, _ := ptypes.Duration(action.Timeout)
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	cmd := exec.CommandContext(ctx, command.Arguments[0], command.Arguments[1:]...)
	// Setting Pdeathsig should ideally make subprocesses get kill signals if we die.
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGTERM,
		Setpgid:   true,
	}
	cmd.Dir = path.Join(w.dir, command.WorkingDirectory)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Env = make([]string, len(command.EnvironmentVariables))
	for i, v := range command.EnvironmentVariables {
		// This is a crappy little hack; tool paths that are made relative don't always work
		// (notably for "go build" which needs an absolute path for -toolexec). For now, we
		// fix up here, but ideally we shouldn't need to know the detail of this.
		if strings.HasPrefix(v.Name, "TOOL") && !path.IsAbs(v.Value) && strings.ContainsRune(v.Value, '/') {
			v.Value = path.Join(w.dir, v.Value)
		} else if v.Name == "PATH" {
			v.Value = strings.Replace(v.Value, "~", w.home, -1)
		} else if v.Name == "TEST" {
			v.Value = path.Join(w.dir, v.Value)
		}
		cmd.Env[i] = v.Name + "=" + v.Value
	}
	err := cmd.Run()
	execEnd := time.Now()
	w.metadata.ExecutionCompletedTimestamp = toTimestamp(execEnd)
	w.metadata.OutputUploadStartTimestamp = w.metadata.ExecutionCompletedTimestamp
	execDuration := execEnd.Sub(start).Seconds()
	executeDurations.Observe(execDuration)
	// Regardless of the result, upload stdout / stderr.
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()
	stdoutDigest, _ := w.client.WriteBlob(ctx, stdout.Bytes())
	stderrDigest, _ := w.client.WriteBlob(ctx, stderr.Bytes())
	ar := &pb.ActionResult{
		ExitCode:          int32(cmd.ProcessState.ExitCode()),
		StdoutDigest:      stdoutDigest.ToProto(),
		StderrDigest:      stderrDigest.ToProto(),
		ExecutionMetadata: w.metadata,
	}
	log.Notice("Completed execution for %s", w.actionDigest.Hash)
	w.observeSysUsage(cmd, execDuration)
	if err != nil {
		msg := "Execution failed: " + err.Error()
		msg = appendStd(msg, "Stdout", stdout.String())
		msg = appendStd(msg, "Stderr", stderr.String())
		if w.browserURL != "" {
			// Attempt to store the failed action result
			ctx, cancel = context.WithTimeout(context.Background(), timeout)
			defer cancel()
			if digest, err := w.client.WriteProto(ctx, &bbcas.UncachedActionResult{
				ActionDigest: w.actionDigest,
				ExecuteResponse: &pb.ExecuteResponse{
					Status: status(codes.Unknown, msg),
					Result: ar,
				},
			}); err != nil {
				log.Warning("Failed to save uncached action result: %s", err)
			} else {
				msg += fmt.Sprintf("\nFailed action details: %s/uncached_action_result/mettle/%s/%d/\n", w.browserURL, digest.Hash, digest.Size)
			}
		}
		return &pb.ExecuteResponse{
			Status: status(codes.Unknown, msg),
			Result: ar,
		}
	}
	if err := w.collectOutputs(ar, command); err != nil {
		return &pb.ExecuteResponse{
			Status: status(codes.Unknown, "Failed to collect outputs: %s", err),
			Result: ar,
		}
	}
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if _, err := w.client.UpdateActionResult(ctx, &pb.UpdateActionResultRequest{
		InstanceName: w.client.InstanceName,
		ActionDigest: w.actionDigest,
		ActionResult: ar,
	}); err != nil {
		return &pb.ExecuteResponse{
			Status: status(codes.Unknown, "Failed to upload action result: %s", err),
			Result: ar,
		}
	}
	end := time.Now()
	w.metadata.OutputUploadCompletedTimestamp = toTimestamp(end)
	uploadDurations.Observe(end.Sub(execEnd).Seconds())
	return &pb.ExecuteResponse{
		Status: &rpcstatus.Status{Code: int32(codes.OK)},
		Result: ar,
	}
}

// shouldSandbox returns true if we should sandbox execution of the given command.
// This is determined by it having a SANDBOX environment variable set to "true".
func (w *worker) shouldSandbox(command *pb.Command) bool {
	for _, e := range command.EnvironmentVariables {
		if e.Name == "SANDBOX" && e.Value == "true" {
			return true
		}
	}
	return false
}

// observeSysUsage observes some stats from a running process.
// It's split to a separate function to handle panics; the docs are a little unclear under what
// circumstances this might happen, but we've definitely seen it.
func (w *worker) observeSysUsage(cmd *exec.Cmd, execDuration float64) {
	defer func() {
		if r := recover(); r != nil {
			log.Warning("Failed to observe process sys usage: %s", r)
		}
	}()
	if cmd.ProcessState != nil {
		rusage := cmd.ProcessState.SysUsage().(*syscall.Rusage)
		peakMemory.Observe(float64(rusage.Maxrss) / 1024.0)                         // maxrss is in kb, we use mb for convenience
		cpuUsage.Observe(float64(rusage.Utime.Sec+rusage.Stime.Sec) / execDuration) // just drop usec, can't be bothered
	}
}

// collectOutputs collects all the outputs of a command and adds them to the given ActionResult.
func (w *worker) collectOutputs(ar *pb.ActionResult, cmd *pb.Command) error {
	m, ar2, err := tree.ComputeOutputsToUpload(w.dir, cmd.OutputPaths, int(w.client.ChunkMaxSize), &filemetadata.NoopFileMetadataCache{})
	if err != nil {
		return err
	}
	chomks := make([]*chunker.Chunker, 0, len(m))
	for _, c := range m {
		chomks = append(chomks, c)
	}
	err = w.client.UploadIfMissing(context.Background(), chomks...)
	ar.OutputFiles = ar2.OutputFiles
	ar.OutputDirectories = ar2.OutputDirectories
	ar.OutputFileSymlinks = ar2.OutputFileSymlinks
	ar.OutputDirectorySymlinks = ar2.OutputDirectorySymlinks
	return err
}

// update sends an update on the response channel
func (w *worker) update(stage pb.ExecutionStage_Value, response *pb.ExecuteResponse) error {
	any, _ := ptypes.MarshalAny(&pb.ExecuteOperationMetadata{
		Stage:        stage,
		ActionDigest: w.actionDigest,
	})
	op := &longrunning.Operation{
		Metadata: any,
		Done:     stage == pb.ExecutionStage_COMPLETED,
	}
	if response != nil {
		any, _ = ptypes.MarshalAny(response)
		op.Result = &longrunning.Operation_Response{Response: any}
	}
	body, _ := proto.Marshal(op)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return w.responses.Send(ctx, &pubsub.Message{Body: body})
}

// readBlobToProto reads an entire blob and deserialises it into a message.
func (w *worker) readBlobToProto(digest *pb.Digest, msg proto.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return w.client.ReadProto(ctx, sdkdigest.NewFromProtoUnvalidated(digest), msg)
}

func status(code codes.Code, msg string, args ...interface{}) *rpcstatus.Status {
	return &rpcstatus.Status{
		Code:    int32(code),
		Message: fmt.Sprintf(msg, args...),
	}
}

// appendStd appends the contents of a std stream to an error message, if it is not empty.
func appendStd(msg, name, contents string) string {
	contents = strings.TrimSpace(contents)
	if contents == "" {
		return msg
	}
	return fmt.Sprintf("%s\n%s:\n%s\n", msg, name, contents)
}

// toTimestamp converts the given time to a proto timestamp, ignoring errors
func toTimestamp(t time.Time) *timestamp.Timestamp {
	ts, _ := ptypes.TimestampProto(t)
	return ts
}
