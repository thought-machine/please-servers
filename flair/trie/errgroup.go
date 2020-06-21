package trie

import (
	"sync"

	"github.com/hashicorp/go-multierror"
)

// An errgroup is like a multierror.Group but returns the first response
// (if it is nil) or a multierror if they all fail.
type errgroup struct {
	ch  chan error
	err *multierror.Error
	wg  sync.WaitGroup
	key string
}

func newErrGroup(key string, replicas int) *errgroup {
	return &errgroup{
		ch: make(chan error, replicas),
	}
}

func (g *errgroup) Go(f func() error) {
	g.wg.Add(1)
	go func() {
		g.ch <- f()
		g.wg.Done()
	}()
}

func (g *errgroup) Wait() error {
	go func() {
		g.wg.Wait()
		close(g.ch)
	}()
	for err := range g.ch {
		if err == nil {
			return nil
		}
		log.Warning("Write to replica failed for %s: %s", g.key, err)
		g.err = multierror.Append(g.err, err)
	}
	return g.err.ErrorOrNil()
}
