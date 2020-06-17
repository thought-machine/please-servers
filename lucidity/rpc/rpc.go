package rpc

import (
	"context"
	"fmt"
	"mime"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/peterebden/go-cli-init"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/thought-machine/please-servers/grpcutil"
	pb "github.com/thought-machine/please-servers/proto/lucidity"
)

var log = cli.MustGetLogger()

var liveWorkers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "lucidity",
	Name:      "live_workers_total",
}, []string{"worker"})

var deadWorkers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "lucidity",
	Name:      "dead_workers_total",
}, []string{"worker"})

var healthyWorkers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "lucidity",
	Name:      "healthy_workers_total",
}, []string{"worker"})

var unhealthyWorkers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "lucidity",
	Name:      "unhealthy_workers_total",
}, []string{"worker"})

var busyWorkers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "lucidity",
	Name:      "busy_workers_total",
}, []string{"worker"})

func init() {
	prometheus.MustRegister(liveWorkers)
	prometheus.MustRegister(deadWorkers)
	prometheus.MustRegister(healthyWorkers)
	prometheus.MustRegister(unhealthyWorkers)
	prometheus.MustRegister(busyWorkers)
}

// ServeForever serves on the given port until terminated.
func ServeForever(opts grpcutil.Opts, httpPort int, maxAge time.Duration, audience string, allowedUsers []string) {
	srv := &server{}
	lis, s := grpcutil.NewServer(opts)
	pb.RegisterLucidityServer(s, srv)

	mux := http.NewServeMux()
	mux.HandleFunc("/workers", srv.ServeWorkers)
	mux.HandleFunc("/", srv.ServeAsset)
	mux.HandleFunc("/disable", srv.ServeDisable)
	authed := maybeAddValidation(mux, audience, allowedUsers)
	log.Notice("Serving HTTP on %s:%d", opts.Host, httpPort)
	go http.ListenAndServe(fmt.Sprintf("%s:%d", opts.Host, httpPort), authed)
	go srv.Clean(maxAge)
	grpcutil.ServeForever(lis, s)
}

type server struct {
	workers sync.Map
}

func (s *server) Update(ctx context.Context, req *pb.UpdateRequest) (*pb.UpdateResponse, error) {
	f := func(b bool) float64 {
		if b {
			return 1.0
		} else {
			return 0.0
		}
	}
	req.UpdateTime = time.Now().Unix()
	v, ok := s.workers.Load(req.Name)
	req.Disabled = ok && v.(*pb.UpdateRequest).Disabled
	s.workers.Store(req.Name, req)
	liveWorkers.WithLabelValues(req.Name).Set(f(req.Alive))
	deadWorkers.WithLabelValues(req.Name).Set(f(!req.Alive))
	healthyWorkers.WithLabelValues(req.Name).Set(f(req.Healthy))
	unhealthyWorkers.WithLabelValues(req.Name).Set(f(!req.Healthy))
	busyWorkers.WithLabelValues(req.Name).Set(f(req.Busy))
	return &pb.UpdateResponse{ShouldDisable: req.Disabled}, nil
}

func (s *server) ServeWorkers(w http.ResponseWriter, r *http.Request) {
	workers := &pb.Workers{}
	s.workers.Range(func(k, v interface{}) bool {
		workers.Workers = append(workers.Workers, v.(*pb.UpdateRequest))
		return true
	})
	m := jsonpb.Marshaler{
		OrigName: true,
		Indent:   "  ",
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	m.Marshal(w, workers)
}

func (s *server) ServeAsset(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" || r.URL.Path == "" {
		r.URL.Path = "index.html"
	}
	asset, err := Asset(strings.TrimPrefix(r.URL.Path, "/"))
	if err != nil {
		log.Warning("404 for URL %s", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", mime.TypeByExtension(path.Ext(r.URL.Path)))
	w.WriteHeader(http.StatusOK)
	w.Write(asset)
}

func (s *server) ServeDisable(w http.ResponseWriter, r *http.Request) {
	req := &pb.Disable{}
	if r.Method != http.MethodPost {
		log.Warning("Rejecting request to %s with method %s", r.URL.Path, r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
	} else if err := jsonpb.Unmarshal(r.Body, req); err != nil {
		log.Warning("Invalid request: %s", err)
		w.WriteHeader(http.StatusBadRequest)
	} else if v, ok := s.workers.Load(req.Name); !ok {
		log.Warning("Request to disable unknown worker %s", req.Name)
		w.WriteHeader(http.StatusNotFound)
	} else {
		v.(*pb.UpdateRequest).Disabled = req.Disable
	}
}

// Clean periodically checks all known workers and discards any older than the given duration.
func (s *server) Clean(maxAge time.Duration) {
	if maxAge <= 0 {
		return
	}
	for range time.NewTicker(maxAge / 10).C {
		min := time.Now().Add(-maxAge).Unix()
		s.workers.Range(func(k, v interface{}) bool {
			if v.(*pb.UpdateRequest).UpdateTime < min {
				s.workers.Delete(k)
			}
			return true
		})
	}
}
