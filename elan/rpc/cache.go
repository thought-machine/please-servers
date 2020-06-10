package rpc

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
	"github.com/peterebden/groupcache"
	"github.com/prometheus/client_golang/prometheus"
)

var mainBytesTotal = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "elan_cache",
	Name:      "main_bytes_total",
})
var mainItemsTotal = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "elan_cache",
	Name:      "main_items_total",
})
var mainGetsTotal = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "elan_cache",
	Name:      "main_gets_total",
})
var mainHitsTotal = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "elan_cache",
	Name:      "main_hits_total",
})
var mainEvictionsTotal = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "elan_cache",
	Name:      "main_evictions_total",
})
var hotBytesTotal = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "elan_cache",
	Name:      "hot_bytes_total",
})
var hotItemsTotal = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "elan_cache",
	Name:      "hot_items_total",
})
var hotGetsTotal = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "elan_cache",
	Name:      "hot_gets_total",
})
var hotHitsTotal = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "elan_cache",
	Name:      "hot_hits_total",
})
var hotEvictionsTotal = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "elan_cache",
	Name:      "hot_evictions_total",
})

// newCache returns a new cache based on the given settings.
func newCache(s *server, host string, port int, self string, peers []string, maxSize, maxItemSize int64) *cache {
	pool := groupcache.NewHTTPPoolOpts(self, &groupcache.HTTPPoolOptions{})
	if len(peers) > 0 {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/_groupcache/", pool)
			log.Notice("Serving groupcache on %s:%d", host, port)
			log.Fatalf("http.ListenAndServe failed: %s", http.ListenAndServe(fmt.Sprintf("%s:%d", host, port), mux))
		}()
	}
	c := &cache{
		server:      s,
		pool:        pool,
		maxItemSize: int(maxItemSize),
		selfIP:      self,
	}
	c.group = groupcache.NewGroup("blobs", maxSize, groupcache.GetterFunc(c.getter))
	if len(peers) > 0 {
		log.Notice("Monitoring peers %s (I am %s)", peers, self)
		go c.MonitorPeers(peers)
	} else {
		log.Warning("No peers specified, will run in local-only mode for the cache")
	}
	go c.RecordMetrics()
	return c
}

type cache struct {
	server      *server
	pool        *groupcache.HTTPPool
	group       *groupcache.Group
	selfIP      string
	maxItemSize int
}

func (c *cache) Get(key string) []byte {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var b []byte
	if err := c.group.Get(ctx, key, groupcache.AllocatingByteSliceSink(&b)); err != nil {
		return nil
	}
	return b
}

func (c *cache) GetProto(key string, msg proto.Message) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.group.Get(ctx, key, groupcache.ProtoSink(msg)); err != nil {
		log.Warning("Failed to retrieve proto for %s from cache: %s", key, err)
		return false
	}
	return true
}

func (c *cache) Has(key string) bool {
	return c.group.Has(key)
}

func (c *cache) Set(key string, value []byte) {
	if len(value) <= c.maxItemSize {
		c.group.Set(key, value)
	}
}

// getter is a getter function for groupcache; it's called when an item needs to be loaded.
func (c *cache) getter(ctx context.Context, key string, dest groupcache.Sink) error {
	// This is kinda dodgy that we don't know the size of the digest at this point.
	// Fortunately this kind of pairs with the fact that we don't store them that way so
	// conceptually at least it doesn't matter...
	if parts := strings.Split(key, "/"); parts[0] == "tree" {
		resp, err := c.server.getWholeTree(&pb.Digest{Hash: parts[2]})
		if err != nil {
			return err
		}
		return dest.SetProto(resp)
	} else {
		// either ac or cas, either way we just need to read a blob from the bucket.
		r, err := c.server.readBlobUncached(ctx, key, &pb.Digest{Hash: parts[2]}, 0, -1)
		if err != nil {
			return err
		}
		defer r.Close()
		b, err := ioutil.ReadAll(r)
		if err != nil {
			return err
		}
		return dest.SetBytes(b)
	}
}

func (c *cache) MonitorPeers(peers []string) {
	current := []string{}
	ticker := time.NewTicker(10 * time.Second)
	for ; true; <-ticker.C {
		if updated, err := c.resolveAddresses(peers); err != nil {
			log.Warning("Failed to resolve peer address: %s", err)
		} else if !equal(current, updated) {
			log.Notice("Updating cache peer list: %s", strings.Join(updated, ", "))
			c.pool.Set(updated...)
			current = updated
		}
	}
}

func (c *cache) RecordMetrics() {
	for range time.NewTicker(30 * time.Second).C {
		main := c.group.CacheStats(groupcache.MainCache)
		hot := c.group.CacheStats(groupcache.HotCache)
		mainBytesTotal.Set(float64(main.Bytes))
		mainItemsTotal.Set(float64(main.Items))
		mainGetsTotal.Set(float64(main.Gets))
		mainHitsTotal.Set(float64(main.Hits))
		mainEvictionsTotal.Set(float64(main.Evictions))
		hotBytesTotal.Set(float64(hot.Bytes))
		hotItemsTotal.Set(float64(hot.Items))
		hotGetsTotal.Set(float64(hot.Gets))
		hotHitsTotal.Set(float64(hot.Hits))
		hotEvictionsTotal.Set(float64(hot.Evictions))
	}
}

func (c *cache) resolveAddress(address string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if !strings.Contains(address, ":") {
		return net.DefaultResolver.LookupHost(ctx, address)
	}
	// Handle the port.
	parts := strings.SplitN(address, ":", 2)
	addrs, err := net.DefaultResolver.LookupHost(ctx, parts[0])
	ret := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if addr == c.selfIP {
			log.Debug("Skipping self (%s) from cache peers", addr)
		} else {
			ret = append(ret, addr+":"+parts[1])
		}
	}
	return ret, err
}

func (c *cache) resolveAddresses(addresses []string) ([]string, error) {
	ret := []string{}
	for _, address := range addresses {
		addrs, err := c.resolveAddress(address)
		if err != nil {
			return ret, err
		}
		ret = append(ret, addrs...)
	}
	sort.Strings(ret)
	return ret, nil
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, x := range a {
		if x != b[i] {
			return false
		}
	}
	return true
}
