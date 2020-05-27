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

	"github.com/peterebden/go-cli-init"
	"github.com/golang/protobuf/jsonpb"
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
func ServeForever(opts grpcutil.Opts, httpPort int) {
	srv := &server{}
	lis, s := grpcutil.NewServer(opts)
	pb.RegisterLucidityServer(s, srv)

	mux := http.NewServeMux()
	mux.HandleFunc("/workers", srv.ServeWorkers)
	mux.HandleFunc("/", srv.ServeAsset)
	log.Notice("Serving HTTP on %s:%d", opts.Host, httpPort)
	go http.ListenAndServe(fmt.Sprintf("%s:%d", opts.Host, httpPort), mux)
	grpcutil.ServeForever(lis, s)
}

type server struct{
	workers sync.Map
}

func (s *server) Update(ctx context.Context, req *pb.UpdateRequest) (*pb.UpdateResponse, error) {
	f := func(b bool) float64 { if b { return 1.0 } else { return 0.0 } }
	req.UpdateTime = time.Now().Unix()
	s.workers.Store(req.Name, req)
	liveWorkers.WithLabelValues(req.Name).Set(f(req.Alive))
	deadWorkers.WithLabelValues(req.Name).Set(f(!req.Alive))
	healthyWorkers.WithLabelValues(req.Name).Set(f(req.Healthy))
	unhealthyWorkers.WithLabelValues(req.Name).Set(f(!req.Healthy))
	busyWorkers.WithLabelValues(req.Name).Set(f(req.Busy))
	return &pb.UpdateResponse{}, nil
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
