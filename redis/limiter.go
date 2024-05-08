package redis

import (
	"context"
	"errors"

	"github.com/go-redis/redis/v8"
	"golang.org/x/time/rate"
)

// ErrRateLimitReached is returned when the rate limit of errors is reached.
var ErrRateLimitReached = errors.New("limiter error rate exceeded, skipping Redis call")

// Limiter implements redis.Limiter so we can rate limit calls to Redis if
// they fail too often.
type Limiter struct {
	limiter *rate.Limiter
}

// Allow checks whether we've exceeded the allowed rate of errors. If we have,
// we won't allow the redis call.
func (l *Limiter) Allow() error {
	if l.limiter.Tokens() < 1.0 {
		return ErrRateLimitReached
	}
	return nil
}

// ReportResult records any non-nil error against the rate limiter.
func (l *Limiter) ReportResult(err error) {
	if err != nil &&
		!errors.Is(err, redis.Nil) &&
		!errors.Is(err, ErrRateLimitReached) &&
		!errors.Is(err, context.Canceled) {
		l.limiter.Allow()
	}
}
