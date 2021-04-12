// Package cli implements some simple shared CLI flag types.
package cli

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/peterebden/go-cli-init/v4"
	"github.com/thought-machine/http-admin"
)

var log = cli.MustGetLogger()

var actionRe = regexp.MustCompile("([0-9a-fA-F]+)/([0-9]+)/?")
var shortActionRe = regexp.MustCompile("([0-9a-fA-F]+)")
var currencyRe = regexp.MustCompile(`([A-Z]{3})([0-9]+(?:\.[0-9]+))`)

// LoggingOpts are a common set of logging options that we use across the repo.
type LoggingOpts struct {
	Verbosity     cli.Verbosity `short:"v" long:"verbosity" default:"notice" description:"Verbosity of output (higher number = more output)"`
	FileVerbosity cli.Verbosity `long:"file_verbosity" default:"debug" description:"Verbosity of file logging output"`
	LogFile       string        `long:"log_file" description:"File to additionally log output to"`
	Structured    bool          `long:"structured_logs" env:"STRUCTURED_LOGS" description:"Output logs in structured (JSON) format"`
}

// AdminOpts is a re-export of the admin type so servers don't need to import it directly.
type AdminOpts = admin.Opts

// ParseFlagsOrDie parses incoming flags and sets up logging etc.
func ParseFlagsOrDie(name string, opts interface{}, loggingOpts *LoggingOpts) (string, cli.LogLevelInfo) {
	cmd := cli.ParseFlagsOrDie(name, opts)
	return cmd, cli.MustInitStructuredLogging(loggingOpts.Verbosity, loggingOpts.FileVerbosity, loggingOpts.LogFile, loggingOpts.Structured)
}

// ServeAdmin starts the admin HTTP server.
// It will block forever so the caller may well want to use a goroutine.
func ServeAdmin(opts AdminOpts, info cli.LogLevelInfo) {
	opts.Logger = cli.MustGetLoggerNamed("github.com.thought-machine.http-admin")
	opts.LogInfo = info
	go admin.Serve(opts)
}

// An Action represents a combined hash / size pair written like
// ff17a4efe382e245491d6a9f1ac6bf3adce454f7e4a5559a3579c3856edf1381/122
// This is a bit more concise than passing them with flags.
type Action struct {
	Hash string
	Size int
}

func (a *Action) UnmarshalFlag(in string) error {
	matches := actionRe.FindStringSubmatch(in)
	if matches != nil {
		return a.fromComponents(matches[1], matches[2])
	}
	matches = shortActionRe.FindStringSubmatch(in)
	if matches != nil {
		return a.fromComponents(matches[1], "")
	}
	return fmt.Errorf("Unknown action format: %s", in)
}

func (a *Action) fromComponents(hash, size string) error {
	if len(hash) != 64 {
		return fmt.Errorf("Invalid hash %s; has length %d, should be 64", hash, len(hash))
	}
	a.Hash = hash
	if size == "" {
		// This looks a bit arbitrary but for whatever reason most of our actions (which is
		// generally what you pass in here) are 147 bytes long.
		log.Warning("Missing size in hash; arbitrarily guessing 147...")
		a.Size = 147
	} else {
		a.Size, _ = strconv.Atoi(size)
	}
	return nil
}

// ToProto converts this Action to the proto digest.
func (a *Action) ToProto() *pb.Digest {
	return &pb.Digest{
		Hash:      a.Hash,
		SizeBytes: int64(a.Size),
	}
}

// AllToProto converts a slice of actions to protos.
func AllToProto(actions []Action) []*pb.Digest {
	ret := make([]*pb.Digest, len(actions))
	for i, a := range actions {
		ret[i] = a.ToProto()
	}
	return ret
}

// A Currency models an amount of real-world money (used to track costs for build actions)
// It is not pinpoint accurate due to use of floating-point; for our purposes exact accuracy is not needed.
type Currency struct {
	Denomination string // ISO4217 code
	Amount       float64
}

// UnmarshalFlag parses from a string such as "£2.20" or "$0.21" or "GBP3.36"
func (c *Currency) UnmarshalFlag(in string) error {
	if strings.HasPrefix(in, "£") {
		return c.UnmarshalFlag("GBP" + strings.TrimPrefix(in, "£"))
	} else if strings.HasPrefix(in, "$") {
		return c.UnmarshalFlag("USD" + strings.TrimPrefix(in, "$"))
	}
	matches := currencyRe.FindStringSubmatch(in)
	if matches == nil {
		return fmt.Errorf("Invalid currency: %s", in)
	}
	f, err := strconv.ParseFloat(matches[2], 64)
	if err != nil {
		return err
	}
	c.Denomination = matches[1]
	c.Amount = f
	return nil
}

// MustGetLogger is a re-export of the same function from the CLI library.
var MustGetLogger = cli.MustGetLogger
