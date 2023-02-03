package worker

import (
	"context"
	"crypto/tls"
	"os"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"

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
func newRedisClient(client elan.Client, url, password string, useTLS bool) elan.Client {
	var tlsConfig *tls.Config
	if useTLS {
		tlsConfig = &tls.Config{}
	}
	return &redisClient{
		elan: client,
		redis: redis.NewClient(&redis.Options{
			Addr:      url,
			Password:  password,
			TLSConfig: tlsConfig,
		}),
		timeout: 10 * time.Second,
		maxSize: 200 * 1012, // 200 Kelly-Bootle standard units
	}
}

type redisClient struct {
	elan    elan.Client
	redis   *redis.Client
	timeout time.Duration
	maxSize int64
}

func (r *redisClient) Healthcheck() error {
	return r.elan.Healthcheck()
}

func (r *redisClient) ReadBlob(dg *pb.Digest) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()
	cmd := r.redis.Get(ctx, dg.Hash)
	if err := cmd.Err(); err == nil {
		blob, _ := cmd.Bytes()
		return blob, nil
	} else if err != redis.Nil {
		log.Warning("Failed to read blob from Redis: %s", err)
	}
	// If we get here, the blob didn't exist. Download it then write back to Redis.
	blob, err := r.elan.ReadBlob(dg)
	if err != nil {
		return nil, err
	}
	if dg.SizeBytes < r.maxSize {
		ctx, cancel = context.WithTimeout(context.Background(), r.timeout)
		defer cancel()
		if cmd := r.redis.Set(ctx, dg.Hash, blob, 0); cmd.Val() != "OK" {
			log.Warning("Failed to set blob in Redis: %s", cmd.Err())
		}
	}
	return blob, nil
}

func (r *redisClient) WriteBlob(blob []byte) (*pb.Digest, error) {
	return r.elan.WriteBlob(blob)
}

func (r *redisClient) UpdateActionResult(ar *pb.UpdateActionResultRequest) (*pb.ActionResult, error) {
	// We don't currently do anything with this, because we never try to read action results from
	// Redis.
	return r.elan.UpdateActionResult(ar)
}

func (r *redisClient) UploadIfMissing(entries []*uploadinfo.Entry) error {
	// All we use Redis for is to filter down the missing set.
	// This is approximate only and assumes that Redis always has a strict subset of the total keys.
	missing := make([]*uploadinfo.Entry, 0, len(entries))
	uploads := make([]interface{}, 0, 2*len(entries))
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
			e := entries[i]
			missing = append(missing, e)
			if dg := e.Digest; dg.Size < r.maxSize && dg.Hash != emptyHash {
				if e.Contents != nil {
					uploads = append(uploads, keys[i], e.Contents)
				} else {
					b, err := os.ReadFile(e.Path)
					if err != nil {
						log.Warning("Failed to read file %s: %s", e.Path, err)
						continue
					}
					uploads = append(uploads, keys[i], b)
				}
			}
		}
	}
	numPresent := len(entries) - len(missing)
	log.Debug("UploadIfMissing: %d / %d in Redis", numPresent, len(entries))
	if len(missing) == 0 {
		return nil // Redis had everything
	}
	if err := r.elan.UploadIfMissing(missing); err != nil {
		return err
	}
	if len(uploads) == 0 {
		return nil
	}
	// Only upload Redis after we have successfully uploaded blobs to Elan, otherwise we could
	// create an inconsistent situation.
	go r.writeBlobs(uploads)
	return nil // We never propagate Redis errors regardless.
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
	log.Debug("Found %d / %d files in Redis", len(dgs)-len(missingDigests), len(dgs))
	m, err := r.elan.BatchDownload(missingDigests, missingComps)
	if err != nil {
		return nil, err
	}
	uploads := make([]interface{}, 0, 2*len(m))
	for k, v := range m {
		ret[k] = v
		uploads = append(uploads, k.Hash, v)
	}
	go r.writeBlobs(uploads)
	return ret, nil
}

// readBlobs reads a set of blobs from Redis. It returns nil on any failure.
// On success some blobs may not have been available in Redis, in which case they'll be nil.
func (r *redisClient) readBlobs(keys []string, metrics bool) [][]byte {
	if len(keys) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
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
		// Somewhat counter-intuitively they are always strings, regardless of what we set them as originally.
		s, ok := blob.(string)
		if !ok {
			log.Warning("Failed to cast Redis response to string, was a %T", blob)
			return nil
		}
		if metrics {
			redisHits.Inc()
			redisBytesRead.Add(float64(len(s)))
		}
		ret[i] = []byte(s)
	}
	return ret
}

// writeBlobs writes a series of blobs to Redis. They should be passed in interleaved key/value order.
func (r *redisClient) writeBlobs(uploads []interface{}) {
	if len(uploads) == 0 {
		return
	}
	log.Debug("Writing %d blobs to Redis...", len(uploads)/2)
	// we are not using the client timeout, as this will most likely take more than the default 10s
	// plus this is not in the critical path, so it's not a big issue
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	if cmd := r.redis.MSet(ctx, uploads...); cmd.Val() != "OK" {
		log.Warning("Failed to upload %d blobs to Redis: %s", len(uploads), cmd.Err())
	}
	log.Debug("Wrote %d blobs to Redis...", len(uploads)/2)
}

func (r *redisClient) ReadToFile(dg digest.Digest, filename string, compressed bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()
	blob, err := r.redis.Get(ctx, dg.Hash).Bytes()
	if err != nil {
		if err != redis.Nil { // Not found.
			log.Warning("Error reading blob from Redis: %s", err)
		}
		redisMisses.Inc()
		return r.elan.ReadToFile(dg, filename, compressed)
	}
	redisHits.Inc()
	return os.WriteFile(filename, blob, 0644)
}

func (r *redisClient) GetDirectoryTree(dg *pb.Digest, usePacks bool) ([]*pb.Directory, error) {
	// TODO(peterebden): Figure out how we are going to stitch Redis into this. There are a lot of       //                   little internal requests that we ideally want to be able to cache into Redis.
	return r.elan.GetDirectoryTree(dg, usePacks)
}
