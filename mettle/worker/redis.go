package worker

import (
	"context"
	"sync"
	"time"

	"github.com/redis-go/redis/v8"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	elan "github.com/thought-machine/please-servers/elan/rpc"
)

// newRedisClient augments an existing elan.Client with a Redis connection.
// All usage of Redis is best-effort only.
func newRedisClient(client elan.Client, url string) elan.Client {
	return &redisClient{
		elan:  client,
		redis: redis.NewClient(&redis.Options{
			Addr: redisURL,
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
	cmd := r.redis.Get(ctx, dg.Hash).Result()
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
	go r.updateActionResult(ar)
	return r.elan.UpdateActionResult(ar)
}

func (r *redisClient) updateActionResult(ar *pb.UpdateActionResultRequest) {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()
	if err := r.Set(ctx, "ar/" + ar.ActionDigest.Hash, ar.ActionResult, 0).Err(); err != nil {
		log.Warning("Failed to set action result in Redis: %s", err)
	}
}

func (r *redisClient) UploadIfMissing(entries []*uploadinfo.Entry) error {
	// All we use Redis for is to filter down the missing set.
	// This is approximate only and assumes that Redis always has a strict subset of the total keys.
	missing := make([]*uploadinfo.Entry, 0, len(entries))
	ch := make(chan struct{}, 10)
	var mutex sync.Mutex
	for _, entry := range entries {
		go func(entry *uploadinfo.Entry) {
			ch <- struct{}{}
			defer func() { <-ch }()
			ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
			defer cancel()

		}(entry)
	}
	return r.elan.UploadIfMissing(missing)
}

func (r *redisClient) BatchDownload([]digest.Digest, []pb.Compressor_Value) (map[digest.Digest][]byte, error)
func (r *redisClient) ReadToFile(digest.Digest, string, bool) error
func (r *redisClient) GetDirectoryTree(*pb.Digest) ([]*pb.Directory, error)
