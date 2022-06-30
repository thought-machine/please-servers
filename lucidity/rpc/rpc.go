package rpc

import (
	"context"
	"embed"
	"fmt"
	"mime"
	"net/http"
	"path"
	"sync"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/peterebden/go-cli-init/v4/logging"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/thought-machine/please-servers/grpcutil"
	pb "github.com/thought-machine/please-servers/proto/lucidity"
)

var log = logging.MustGetLogger()

//go:embed static/*.*
var embeddedData embed.FS

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
func ServeForever(opts grpcutil.Opts, httpPort int, maxAge time.Duration, minProportion float64, audience string, allowedUsers []string) {
	srv := newServer(minProportion)
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
	workers, validVersions                                                  sync.Map
	mutex                                                                   sync.Mutex
	liveWorkers, deadWorkers, healthyWorkers, unhealthyWorkers, busyWorkers map[string]struct{}
	minProportion                                                           float64
}

func newServer(minProportion float64) *server {
	return &server{
		liveWorkers:      map[string]struct{}{},
		deadWorkers:      map[string]struct{}{},
		healthyWorkers:   map[string]struct{}{},
		unhealthyWorkers: map[string]struct{}{},
		busyWorkers:      map[string]struct{}{},
		minProportion:    minProportion,
	}
}

func (s *server) Update(ctx context.Context, req *pb.UpdateRequest) (*pb.UpdateResponse, error) {
	req.UpdateTime = time.Now().Unix()
	v, ok := s.workers.Load(req.Name)
	req.Disabled = ok && v.(*pb.UpdateRequest).Disabled
	s.workers.Store(req.Name, req)
	if !ok || v.(*pb.UpdateRequest).Version != req.Version {
		s.recalculateValidVersions()
	}
	validVersion := s.IsValidVersion(req.Version)
	if !validVersion {
		req.Healthy = false
		req.Status = "Invalid version"
	}
	s.updateMaps(req)
	return &pb.UpdateResponse{ShouldDisable: req.Disabled || !validVersion}, nil
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
	minHealthy := time.Now().Add(-10 * time.Minute).Unix()
	s.workers.Range(func(k, v interface{}) bool {
		r := v.(*pb.UpdateRequest)
		if r.Healthy && r.UpdateTime < minHealthy {
			r.Healthy = false
			r.Status = "Too long since last update"
		} else if !s.IsValidVersion(r.Version) {
			r.Healthy = false
			r.Status = "Invalid version"
		}
		s.updateMaps(r)
		workers.Workers = append(workers.Workers, r)
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

func (s *server) updateMaps(r *pb.UpdateRequest) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	liveWorkers.Set(s.updateMap(s.liveWorkers, r.Name, r.Alive))
	deadWorkers.Set(s.updateMap(s.deadWorkers, r.Name, !r.Alive))
	healthyWorkers.Set(s.updateMap(s.healthyWorkers, r.Name, r.Healthy))
	unhealthyWorkers.Set(s.updateMap(s.unhealthyWorkers, r.Name, !r.Healthy))
	busyWorkers.Set(s.updateMap(s.busyWorkers, r.Name, r.Busy))
}

func (s *server) ServeAsset(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" || r.URL.Path == "" {
		r.URL.Path = "index.html"
	}
	asset, err := embeddedData.ReadFile(path.Join("static", r.URL.Path))
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

// recalculateValidVersions runs through all the workers and calculates the new set of versions we'll allow to be active.
func (s *server) recalculateValidVersions() {
	// Collect counts of all known versions
	counts := map[string]int{}
	n := 0
	s.workers.Range(func(k, v interface{}) bool {
		counts[v.(*pb.UpdateRequest).Version]++
		n++
		return true
	})
	// Now add any of those over the threshold to the set of valid versions
	min := int(s.minProportion * float64(n))
	log.Notice("Updating valid versions, total workers: %d, min threshold: %d", n, min)
	for k, v := range counts {
		if v < min {
			log.Info("  %s: %d (invalid)", k, v)
		} else {
			log.Info("  %s: %d", k, v)
			s.validVersions.Store(k, v)
		}
	}
	// Delete any old versions that were valid but are no longer.
	s.validVersions.Range(func(k, v interface{}) bool {
		if counts[k.(string)] < min {
			log.Notice("Disabling version %s", k)
			s.validVersions.Delete(k)
		}
		return true
	})
}

// IsValidVersion returns true if this version is currently live.
func (s *server) IsValidVersion(v string) bool {
	_, valid := s.validVersions.Load(v)
	return valid
}
