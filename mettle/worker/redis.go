package worker

import (
	"context"
	"time"

	"github.com/redis-go/redis/v8"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	elan "github.com/thought-machine/please-servers/elan/rpc"
)

// newRedisClient augments an existing elan.Client with a Redis connection.
// All usage of Redis is best-effort only.
func newRedisClient(client elan.Client, url string, storer *storer) elan.Client {
	return &redisClient{
		elan:  client,
		redis: redis.NewClient(&redis.Options{
			Addr: redisURL,
		}),
		storer:  storer,
		timeout: 1 * time.Minute,
	}
}

type redisClient struct{
	elan   elan.Client
	redis  *redis.Client
	storer *storer
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
	b, _ := cmd.Bytes()
	return b, nil
}

func (r *redisClient) WriteBlob([]byte) (*pb.Digest, error)
func (r *redisClient) UpdateActionResult(*pb.UpdateActionResultRequest) (*pb.ActionResult, error)
func (r *redisClient) UploadIfMissing([]*uploadinfo.Entry) error
func (r *redisClient) BatchDownload([]digest.Digest, []pb.Compressor_Value) (map[digest.Digest][]byte, error)
func (r *redisClient) ReadToFile(digest.Digest, string, bool) error
func (r *redisClient) GetDirectoryTree(*pb.Digest) ([]*pb.Directory, error)
