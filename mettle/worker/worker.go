// Package worker implements the worker side of Mettle.
package worker

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math"
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
	"github.com/dgraph-io/ristretto"
	"github.com/dustin/go-humanize"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/peterebden/go-cli-init"
	"github.com/prometheus/client_golang/prometheus"
	"gocloud.dev/pubsub"
	"google.golang.org/genproto/googleapis/longrunning"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"

	"github.com/thought-machine/please-servers/grpcutil"
	"github.com/thought-machine/please-servers/mettle/common"
	lpb "github.com/thought-machine/please-servers/proto/lucidity"
	bbcas "github.com/thought-machine/please-servers/third_party/proto/cas"
)

var log = cli.MustGetLogger()

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
var cacheHits = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "cache_hits_total",
})
var cacheMisses = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "cache_misses_total",
})

func init() {
	prometheus.MustRegister(totalBuilds)
	prometheus.MustRegister(currentBuilds)
	prometheus.MustRegister(executeDurations)
	prometheus.MustRegister(fetchDurations)
	prometheus.MustRegister(uploadDurations)
	prometheus.MustRegister(peakMemory)
	prometheus.MustRegister(cpuUsage)
	prometheus.MustRegister(cacheHits)
	prometheus.MustRegister(cacheMisses)
}

// RunForever runs the worker, receiving jobs until terminated.
func RunForever(instanceName, requestQueue, responseQueue, name, storage, dir, cacheDir, browserURL, sandbox, lucidity, tokenFile string, cachePrefix []string, clean, secureStorage bool, timeout time.Duration, maxCacheSize, minDiskSpace int64) {
	if err := runForever(instanceName, requestQueue, responseQueue, name, storage, dir, cacheDir, browserURL, sandbox, lucidity, tokenFile, cachePrefix, clean, secureStorage, timeout, maxCacheSize, minDiskSpace); err != nil {
		log.Fatalf("Failed to run: %s", err)
	}
}

// RunOne runs one single request, returning any error received.
func RunOne(instanceName, name, storage, dir, cacheDir, sandbox, tokenFile string, cachePrefix []string, clean, secureStorage bool, timeout time.Duration, hash string, size int64) error {
	// Must create this to submit on first
	topic := common.MustOpenTopic("mem://requests")
	w, err := initialiseWorker(instanceName, "mem://requests", "mem://responses", name, storage, dir, cacheDir, "", sandbox, "", tokenFile, cachePrefix, clean, secureStorage, timeout, 0, math.MaxInt64)
	if err != nil {
		return err
	}
	// Have to do this async since mempubsub doesn't seem to store messages?
	go func() {
		time.Sleep(500 * time.Millisecond) // this is dodgy obvs
		b, _ := proto.Marshal(&pb.ExecuteRequest{
			InstanceName: instanceName,
			ActionDigest: &pb.Digest{
				Hash:      hash,
				SizeBytes: size,
			},
		})
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		log.Notice("Sending request to build %s...", hash)
		if err := topic.Send(ctx, &pubsub.Message{Body: b}); err != nil {
			log.Fatalf("Failed to submit job to internal queue: %s", err)
		}
		log.Notice("Sent request to build %s", hash)
	}()
	response, err := w.RunTask(context.Background())
	if err != nil {
		return fmt.Errorf("Failed to run task: %s", err)
	} else if response.Result.ExitCode != 0 {
		return fmt.Errorf("Execution failed: %s", response.Message)
	}
	log.Notice("Completed execution successfully for %s", hash)
	return nil
}

func runForever(instanceName, requestQueue, responseQueue, name, storage, dir, cacheDir, browserURL, sandbox, lucidity, tokenFile string, cachePrefix []string, clean, secureStorage bool, timeout time.Duration, maxCacheSize, minDiskSpace int64) error {
	w, err := initialiseWorker(instanceName, requestQueue, responseQueue, name, storage, dir, cacheDir, browserURL, sandbox, lucidity, tokenFile, cachePrefix, clean, secureStorage, timeout, maxCacheSize, minDiskSpace)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGTERM)
	go func() {
		sig := <-ch
		log.Warning("Received signal %s, shutting down when ready...", sig)
		w.Report(false, false, false, "Received signal %s, shutting down when ready...", sig)
		cancel()
		sig = <-ch
		log.Fatalf("Received another signal %s, shutting down immediately", sig)
		w.Report(false, false, false, "Received another signal %s, shutting down immediately...", sig)
	}()
	for {
		w.waitForFreeSpace()
		w.Report(true, false, true, "Awaiting next task...")
		if _, err := w.RunTask(ctx); err != nil {
			if ctx.Err() != nil {
				// Error came from a signal triggered above. Give a brief period to send reports then die.
				time.Sleep(500 * time.Millisecond)
				return fmt.Errorf("terminated by signal")
			}
			// If we get an error back here, we have failed to communicate with one of
			// our queues, so we are basically doomed and should stop.
			err = fmt.Errorf("Failed to run task: %s", err)
			w.Report(false, false, false, err.Error())
			return err
		}
	}
}

func initialiseWorker(instanceName, requestQueue, responseQueue, name, storage, dir, cacheDir, browserURL, sandbox, lucidity, tokenFile string, cachePrefix []string, clean, secureStorage bool, timeout time.Duration, maxCacheSize, minDiskSpace int64) (*worker, error) {
	// Make sure we have a directory to run in
	if err := os.MkdirAll(dir, os.ModeDir|0755); err != nil {
		return nil, fmt.Errorf("Failed to create working directory: %s", err)
	}
	if cacheDir != "" && len(cachePrefix) > 0 {
		// If we're gonna be copying to the cache dir we should make sure it exists first.
		if err := os.MkdirAll(cacheDir, os.ModeDir|0755); err != nil {
			return nil, fmt.Errorf("Failed to create cache directory: %s", err)
		}
	}
	// Remove anything existing within this directory.
	// We don't just do a RemoveAll above in case we don't have permissions to create it in the first place.
	if clean {
		files, err := ioutil.ReadDir(dir)
		if err != nil {
			return nil, err // If we can't even list the directory, may as well bomb out now.
		}
		for _, file := range files {
			if err := os.RemoveAll(path.Join(dir, file.Name())); err != nil {
				log.Warning("Failed to remove existing work directory: %s", err)
			}
		}
	}
	// If no name is given, default to the hostname.
	if name == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("Failed to determine hostname, must pass --name explicitly: %s", err)
		}
		name = hostname
		log.Notice("This is %s", name)
	}
	// Check this exists upfront
	if sandbox != "" {
		if _, err := os.Stat(sandbox); err != nil {
			return nil, fmt.Errorf("Error checking sandbox tool: %s", err)
		}
	}
	log.Notice("Dialling remote %s...", storage)
	client, err := client.NewClient(context.Background(), instanceName, client.DialParams{
		Service:            storage,
		NoSecurity:         !secureStorage,
		TransportCredsOnly: secureStorage,
		DialOpts:           grpcutil.DialOptions(tokenFile),
	}, client.UseBatchOps(true), client.RetryTransient())
	if err != nil {
		return nil, err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	abspath, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("Failed to make path absolute: %s", err)
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
		iolimiter:  make(chan struct{}, ioParallelism),
		browserURL: browserURL,
		timeout:    timeout,
		startTime:  time.Now(),
		diskSpace:  minDiskSpace,
	}
	if cacheDir != "" {
		w.fileCache = newCache(cacheDir, cachePrefix)
	}
	if maxCacheSize > 0 {
		c, err := ristretto.NewCache(&ristretto.Config{
			NumCounters: maxCacheSize / 10, // bit of a guess
			MaxCost:     maxCacheSize,
			BufferItems: 64, // recommended by upstream
		})
		if err != nil {
			return nil, fmt.Errorf("Failed to create cache: %s", err)
		}
		w.cache = c
	}
	if lucidity != "" {
		w.lucidChan = make(chan *lpb.UpdateRequest, 100)
		log.Notice("Dialling Lucidity...")
		conn, err := grpcutil.Dial(lucidity, true, "", tokenFile) // CA is currently not configurable.
		if err != nil {
			return nil, fmt.Errorf("Failed to dial Lucidity server: %s", err)
		}
		w.lucidity = lpb.NewLucidityClient(conn)
		go w.sendReports()
	}
	log.Notice("Initialised with settings: max batch size: %d max batch count: %d chunk max size: %d cache dir: %s max cache size: %d", client.MaxBatchSize, client.MaxBatchDigests, client.ChunkMaxSize, cacheDir, maxCacheSize)
	return w, nil
}

type worker struct {
	requests     *pubsub.Subscription
	responses    *pubsub.Topic
	client       *client.Client
	lucidity     lpb.LucidityClient
	lucidChan    chan *lpb.UpdateRequest
	cache        *ristretto.Cache
	dir, rootDir string
	home         string
	name         string
	browserURL   string
	sandbox      string
	clean        bool
	timeout      time.Duration
	fileCache    *cache
	startTime    time.Time
	diskSpace    int64

	// These properties are per-action and reset each time.
	actionDigest    *pb.Digest
	metadata        *pb.ExecutedActionMetadata
	downloadedBytes int64
	cachedBytes     int64
	metadataFetch   time.Duration
	dirCreation     time.Duration
	fileDownload    time.Duration
	lastURL         string // This is reset somewhat lazily.

	// For limiting parallelism during download / write actions
	limiter, iolimiter chan struct{}
}

// RunTask runs a single task.
// Note that it only returns errors for reasons this service controls (i.e. queue comms),
// failures at actually running the task are communicated back on the responses queue.
func (w *worker) RunTask(ctx context.Context) (*pb.ExecuteResponse, error) {
	log.Notice("Waiting for next task...")
	msg, err := w.requests.Receive(ctx)
	if err != nil {
		log.Error("Error receiving message: %s", err)
		return nil, err
	}
	// Mark message as consumed now. Alternatively we could not ack it until we
	// run the command, but we *probably* want to do that kind of retrying at a
	// higher level. TBD.
	msg.Ack()
	w.downloadedBytes = 0
	w.cachedBytes = 0
	response := w.runTask(msg.Body)
	return response, w.update(pb.ExecutionStage_COMPLETED, response)
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
	w.lastURL = w.actionURL()
	w.Report(true, true, true, "Hard at work...")
	if status := w.prepareDir(action, command); status != nil {
		log.Warning("Failed to prepare directory for action digest %s: %s", w.actionDigest.Hash, status)
		ar := &pb.ActionResult{
			ExitCode:          255, // Not really but shouldn't look like it was successful
			ExecutionMetadata: w.metadata,
		}
		status.Message += w.writeUncachedResult(ar, status.Message)
		return &pb.ExecuteResponse{
			Result: ar,
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
		return req, nil, nil, status(codes.FailedPrecondition, "Invalid action digest %s/%d: %s", req.ActionDigest.Hash, req.ActionDigest.SizeBytes, err)
	} else if err := w.readBlobToProto(action.CommandDigest, command); err != nil {
		return req, nil, nil, status(codes.FailedPrecondition, "Invalid command digest %s/%d: %s", action.CommandDigest.Hash, action.CommandDigest.SizeBytes, err)
	}
	return req, action, command, nil
}

// prepareDir prepares the directory for executing this request.
func (w *worker) prepareDir(action *pb.Action, command *pb.Command) *rpcstatus.Status {
	log.Info("Preparing directory for %s", w.actionDigest.Hash)
	defer func() {
		w.metadata.InputFetchCompletedTimestamp = toTimestamp(time.Now())
	}()
	w.update(pb.ExecutionStage_EXECUTING, nil)
	dir, err := ioutil.TempDir(w.rootDir, "mettle")
	if err != nil {
		return status(codes.Internal, "Failed to create temp dir: %s", err)
	}
	w.dir = dir
	start := time.Now()
	w.metadata.InputFetchStartTimestamp = toTimestamp(start)
	if err := w.downloadDirectory(action.InputRootDigest); err != nil {
		return status(codes.Internal, "Failed to download input root: %s", err)
	}
	// We are required to create directories for all the outputs.
	if !containsEnvVar(command, "_CREATE_OUTPUT_DIRS", "false") {
		for _, out := range command.OutputPaths {
			if dir := path.Dir(out); out != "" && out != "." {
				if err := os.MkdirAll(path.Join(w.dir, dir), os.ModeDir|0755); err != nil {
					return status(codes.Internal, "Failed to create directory: %s", err)
				}
			}
		}
	}
	fetchDurations.Observe(time.Since(start).Seconds())
	if total := w.cachedBytes + w.downloadedBytes; total > 0 {
		percentage := float64(w.downloadedBytes) * 100.0 / float64(total)
		log.Notice("Prepared directory for %s; downloaded %s / %s (%0.1f%%).", w.actionDigest.Hash, humanize.Bytes(uint64(w.downloadedBytes)), humanize.Bytes(uint64(total)), percentage)
	} else {
		log.Notice("Prepared directory for %s", w.actionDigest.Hash)
	}
	log.Notice("Metadata fetch: %s, dir creation: %s, file download: %s", w.metadataFetch, w.dirCreation, w.fileDownload)
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
	cmd.SysProcAttr = sysProcAttr()
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
	ctx, cancel = context.WithTimeout(context.Background(), w.timeout)
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
		msg += w.writeUncachedResult(ar, msg)
		log.Warning("%s", appendStd(appendStd(msg, "Stdout", stdout.String()), "Stderr", stderr.String()))
		return &pb.ExecuteResponse{
			Status:  &rpcstatus.Status{Code: int32(codes.OK)}, // Still counts as OK on a status code.
			Result:  ar,
			Message: msg,
		}
	}
	if err := w.collectOutputs(ar, command); err != nil {
		log.Error("Failed to collect outputs: %s", err)
		return &pb.ExecuteResponse{
			Status: status(codes.Internal, "Failed to collect outputs: %s", err),
			Result: ar,
		}
	}
	end := time.Now()
	w.metadata.OutputUploadCompletedTimestamp = toTimestamp(end)
	uploadDurations.Observe(end.Sub(execEnd).Seconds())
	w.metadata.WorkerCompletedTimestamp = toTimestamp(time.Now())
	ctx, cancel = context.WithTimeout(context.Background(), w.timeout)
	defer cancel()
	ar, err = w.client.UpdateActionResult(ctx, &pb.UpdateActionResultRequest{
		InstanceName: w.client.InstanceName,
		ActionDigest: w.actionDigest,
		ActionResult: ar,
	})
	if err != nil {
		log.Error("Failed to upload action result: %s", err)
		return &pb.ExecuteResponse{
			Status: status(codes.Internal, "Failed to upload action result: %s", err),
			Result: ar,
		}
	}
	return &pb.ExecuteResponse{
		Status: &rpcstatus.Status{Code: int32(codes.OK)},
		Result: ar,
	}
}

// writeUncachedResult attempts to write an uncached action result proto.
// This is an extension for buildbarn-browser that lets it display results of a failed build.
// It returns a string that, on success, contains an appropriate message about it (they are
// communicated back in the human-readable part of the response).
func (w *worker) writeUncachedResult(ar *pb.ActionResult, msg string) string {
	// No point if we don't know where the browser is.
	if w.browserURL == "" {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()
	digest, err := w.client.WriteProto(ctx, &bbcas.UncachedActionResult{
		ActionDigest: w.actionDigest,
		ExecuteResponse: &pb.ExecuteResponse{
			Status: status(codes.Unknown, msg),
			Result: ar,
		},
	})
	if err != nil {
		log.Warning("Failed to save uncached action result: %s", err)
		return ""
	}
	w.lastURL = fmt.Sprintf("%s/uncached_action_result/%s/%s/%d/", w.browserURL, w.client.InstanceName, digest.Hash, digest.Size)
	s := "\nFailed action details: " + w.lastURL + "\n"
	return s + "\n      Original action: " + w.actionURL() + "\n"
}

// actionURL returns a browser URL for the currently executed action, or the empty string if no browser is configured.
func (w *worker) actionURL() string {
	if w.browserURL == "" {
		return ""
	}
	return fmt.Sprintf("%s/action/%s/%s/%d/", w.browserURL, w.client.InstanceName, w.actionDigest.Hash, w.actionDigest.SizeBytes)
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
	m, ar2, err := tree.ComputeOutputsToUpload(w.dir, cmd.OutputPaths, int(w.client.ChunkMaxSize), filemetadata.NewNoopCache())
	if err != nil {
		return err
	}
	chomks := make([]*chunker.Chunker, 0, len(m))
	for _, c := range m {
		chomks = append(chomks, c)
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()
	err = w.client.UploadIfMissing(ctx, chomks...)
	// This is not strictly required but makes things slightly nicer for plz; it won't need
	// to do this itself and re-update the actionresult.
	if containsEnvVar(cmd, "_BINARY", "true") {
		for _, f := range ar2.OutputFiles {
			f.IsExecutable = true
		}
	}
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
	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()
	return w.responses.Send(ctx, &pubsub.Message{Body: body})
}

// readBlobToProto reads an entire blob and deserialises it into a message.
func (w *worker) readBlobToProto(digest *pb.Digest, msg proto.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
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

// containsEnvVar returns true if the given env var is set on this command.
func containsEnvVar(command *pb.Command, name, value string) bool {
	return getEnvVar(command, name) == value
}

// getEnvVar returns the value of an env var on the given command.
func getEnvVar(command *pb.Command, name string) string {
	for _, e := range command.EnvironmentVariables {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}
