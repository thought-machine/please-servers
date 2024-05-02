package redis

import (
	"errors"

	"github.com/go-redis/redis/v8"
	"golang.org/x/time/rate"
)

// Limiter implements redis.Limiter so we can rate limit calls to Redis if
// they fail too often.
type Limiter struct {
	limiter *rate.Limiter
}

// Allow checks whether we've exceeded the allowed rate of errors. If we have,
// we won't allow the redis call.
func (l *Limiter) Allow() error {
	if l.limiter.Tokens() < 1.0 {
		return errors.New("limiter error rate exceeded, skipping Redis call")
	}
	return nil
}

// ReportResult records any non-nil error against the rate limiter.
func (l *Limiter) ReportResult(err error) {
	if err != nil && err != redis.Nil {
		l.limiter.Reserve()
	}
}
