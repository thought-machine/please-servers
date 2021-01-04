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
	"github.com/peterebden/go-cli-init/v3"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/thought-machine/please-servers/grpcutil"
	pb "github.com/thought-machine/please-servers/proto/lucidity"
)

var log = cli.MustGetLogger()

var liveWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "lucidity",
	Name:      "live_workers_total",
})

var deadWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "lucidity",
	Name:      "dead_workers_total",
})

var healthyWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "lucidity",
	Name:      "healthy_workers_total",
})

var unhealthyWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "lucidity",
	Name:      "unhealthy_workers_total",
})

var busyWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "lucidity",
	Name:      "busy_workers_total",
})

func init() {
	prometheus.MustRegister(liveWorkers)
	prometheus.MustRegister(deadWorkers)
	prometheus.MustRegister(healthyWorkers)
	prometheus.MustRegister(unhealthyWorkers)
	prometheus.MustRegister(busyWorkers)
}

// ServeForever serves on the given port until terminated.
func ServeForever(opts grpcutil.Opts, httpPort int, maxAge time.Duration, audience string, allowedUsers []string) {
	srv := &server{
		liveWorkers:      map[string]struct{}{},
		deadWorkers:      map[string]struct{}{},
		healthyWorkers:   map[string]struct{}{},
		unhealthyWorkers: map[string]struct{}{},
		busyWorkers:      map[string]struct{}{},
	}
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
	workers                                                                 sync.Map
	mutex                                                                   sync.Mutex
	liveWorkers, deadWorkers, healthyWorkers, unhealthyWorkers, busyWorkers map[string]struct{}
}

func (s *server) Update(ctx context.Context, req *pb.UpdateRequest) (*pb.UpdateResponse, error) {
	req.UpdateTime = time.Now().Unix()
	v, ok := s.workers.Load(req.Name)
	req.Disabled = ok && v.(*pb.UpdateRequest).Disabled
	s.workers.Store(req.Name, req)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	liveWorkers.Set(s.updateMap(s.liveWorkers, req.Name, req.Alive))
	deadWorkers.Set(s.updateMap(s.deadWorkers, req.Name, !req.Alive))
	healthyWorkers.Set(s.updateMap(s.healthyWorkers, req.Name, req.Healthy))
	unhealthyWorkers.Set(s.updateMap(s.unhealthyWorkers, req.Name, !req.Healthy))
	busyWorkers.Set(s.updateMap(s.busyWorkers, req.Name, req.Busy))
	return &pb.UpdateResponse{ShouldDisable: req.Disabled}, nil
}

func (s *server) updateMap(m map[string]struct{}, key string, in bool) float64 {
	if in {
		m[key] = struct{}{}
	} else {
		delete(m, key)
	}
	return float64(len(m))
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
				go s.removeWorker(k.(string))
			}
			return true
		})
	}
}

func (s *server) removeWorker(key string) {
	s.workers.Delete(key)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.liveWorkers, key)
	delete(s.deadWorkers, key)
	delete(s.healthyWorkers, key)
	delete(s.unhealthyWorkers, key)
	delete(s.busyWorkers, key)
}
