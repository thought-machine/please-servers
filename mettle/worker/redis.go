package worker

import (
	"context"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	elan "github.com/thought-machine/please-servers/elan/rpc"
)

var redisHits = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "redis_hits_total",
})
var redisMisses = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "redis_misses_total",
})
var redisBytesRead = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "redis_bytes_read_total",
})

// newRedisClient augments an existing elan.Client with a Redis connection.
// All usage of Redis is best-effort only.
func newRedisClient(client elan.Client, url string) elan.Client {
	return &redisClient{
		elan:  client,
		redis: redis.NewClient(&redis.Options{
			Addr: url,
		}),
		timeout: 1 * time.Minute,
	}
}

type redisClient struct{
	elan   elan.Client
	redis  *redis.Client
	timeout time.Duration
}

func (r *redisClient) Healthcheck() error {
	return r.elan.Healthcheck()
}

func (r *redisClient) ReadBlob(dg *pb.Digest) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()
	cmd := r.redis.Get(ctx, dg.Hash)
	if err := cmd.Err(); err == redis.Nil {
		// Blob doesn't exist in Redis
		return r.elan.ReadBlob(dg)
	} else if err != nil {
		log.Warning("Failed to read blob from Redis: %s", err)
		return r.elan.ReadBlob(dg)
	}
	blob, _ := cmd.Bytes()
	return blob, nil
}

func (r *redisClient) WriteBlob(blob []byte) (*pb.Digest, error) {
	return r.elan.WriteBlob(blob)
}

func (r *redisClient) UpdateActionResult(ar *pb.UpdateActionResultRequest) (*pb.ActionResult, error) {
	// We don't currently do anything with this, because we never try to read action results from
	// Redis. If we do, uncomment the following line.
	// go r.updateActionResult(ar)
	return r.elan.UpdateActionResult(ar)
}

func (r *redisClient) updateActionResult(ar *pb.UpdateActionResultRequest) {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()
	if err := r.redis.Set(ctx, "ar/" + ar.ActionDigest.Hash, ar.ActionResult, 0).Err(); err != nil {
		log.Warning("Failed to set action result in Redis: %s", err)
	}
}

func (r *redisClient) UploadIfMissing(entries []*uploadinfo.Entry) error {
	// All we use Redis for is to filter down the missing set.
	// This is approximate only and assumes that Redis always has a strict subset of the total keys.
	missing := make([]*uploadinfo.Entry, 0, len(entries))
	keys := make([]string, len(entries))
	// Unfortunately there is no MEXISTS, so we reuse MGET but chuck away the values.
	// We could also do lots of EXISTS in parallel but this seems simpler...
	for i, entry := range entries {
		keys[i] = entry.Digest.Hash
	}
	blobs := r.readBlobs(keys, false)
	if blobs == nil {
		return r.elan.UploadIfMissing(entries)
	}
	for i, blob := range blobs {
		if blob == nil {
			missing = append(missing, entries[i])
		}
	}
	numPresent := len(entries) - len(missing)
	log.Debug("UploadIfMissing: %d / %d in Redis", numPresent, len(entries))
	if len(missing) == 0 {
		return nil  // Redis had everything
	}
	return r.elan.UploadIfMissing(missing)
}

func (r *redisClient) BatchDownload(dgs []digest.Digest, comps []pb.Compressor_Value) (map[digest.Digest][]byte, error) {
	log.Debug("Checking Redis for batch of %d files...", len(dgs))
	keys := make([]string, len(dgs))
	for i, dg := range dgs {
		keys[i] = dg.Hash
	}
	blobs := r.readBlobs(keys, true)
	if blobs == nil {
		return r.elan.BatchDownload(dgs, comps)
	}
	missingDigests := make([]digest.Digest, 0, len(dgs))
	missingComps := make([]pb.Compressor_Value, 0, len(comps))
	ret := make(map[digest.Digest][]byte, len(dgs))
	for i, blob := range blobs {
		if blob == nil {
			missingDigests = append(missingDigests, dgs[i])
			missingComps = append(missingComps, comps[i])
		} else {
			ret[dgs[i]] = blob
		}
	}
	if len(missingDigests) == 0 {
		return ret, nil
	}
	m, err := r.elan.BatchDownload(missingDigests, missingComps)
	if err != nil {
		return nil, err
	}
	if len(ret) == 0 {
		return m, nil
	}
	for k, v := range m {
		ret[k] = v
	}
	return ret, nil
}

// readBlobs reads a set of blobs from Redis. It returns nil on any failure.
// On success some blobs may not have been available in Redis, in which case they'll be nil.
func (r *redisClient) readBlobs(keys []string, metrics bool) [][]byte {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	resp, err := r.redis.MGet(ctx, keys...).Result()
	if err != nil {
		log.Warning("Failed to check blobs in Redis: %s", err)
		return nil
	} else if len(resp) != len(keys) {
		log.Warning("Length mismatch in Redis response; got %d, expected %d", len(resp), len(keys))
		// If the lengths don't match we don't know which is which so it's useless to us.
		// This shouldn't happen but we don't want to assume it never does and blindly index...
		return nil
	}
	ret := make([][]byte, len(keys))
	for i, blob := range resp {
		if blob == nil {
			if metrics {
				redisMisses.Inc()
			}
			continue
		}
		b, ok := blob.([]byte)
		if !ok {
			log.Warning("Failed to cast Redis response to []byte")
			return nil
		}
		if metrics {
			redisHits.Inc()
			redisBytesRead.Add(float64(len(b)))
		}
		ret[i] = b
	}
	return ret
}

func (r *redisClient) ReadToFile(dg digest.Digest, filename string, compressed bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	blob, err := r.redis.Get(ctx, dg.Hash).Bytes()
	if err != nil {
		if err != redis.Nil {  // Not found.
			log.Warning("Error reading blob from Redis: %s", err)
		}
		redisMisses.Inc()
		return r.elan.ReadToFile(dg, filename, compressed)
	}
	redisHits.Inc()
	return os.WriteFile(filename, blob, 0644)
}

func (r *redisClient) GetDirectoryTree(dg *pb.Digest) ([]*pb.Directory, error) {
	// TODO(peterebden): Figure out how we are going to stitch Redis into this. There are a lot of       //                   little internal requests that we ideally want to be able to cache into Redis.
	return r.elan.GetDirectoryTree(dg)
}
