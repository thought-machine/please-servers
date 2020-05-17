package worker

import (
	"context"
	"fmt"
	"time"

	lpb "github.com/thought-machine/please-servers/proto/lucidity"
)

// Report reports to Lucidity how this worker currently considers itself.
// If a Lucidity server hasn't been configured, calling this has no effect.
func (w *worker) Report(healthy, busy, alive bool, status string, args ...interface{}) {
	if w.lucidChan != nil {
		w.lucidChan <- &lpb.UpdateRequest{
			Name:      w.name,
			StartTime: w.startTime.Unix(),
			Healthy:   healthy,
			Busy:      busy,
			Alive:     alive,
			Status:    fmt.Sprintf(status, args...),
			LastTask:  w.lastURL,
		}
	}
}

// sendReports sends reports to Lucidity indefinitely.
func (w *worker) sendReports() {
	for report := range w.lucidChan {
		w.sendReport(report)
	}
}

func (w *worker) sendReport(report *lpb.UpdateRequest) {
	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()
	if _, err := w.lucidity.Update(ctx, report); err != nil {
		log.Warning("Failed to report status to Lucidity: %s", err)
	}
}
