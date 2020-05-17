package worker

import (
	"context"
	"fmt"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"

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

// waitForFreeSpace checks the currently available disk space and reports unhealthy until it is under a threshold.
func (w *worker) waitForFreeSpace() {
	if w.checkFreeSpace() {
		return
	}
	for range time.NewTicker(1 * time.Minute).C {
		if w.checkFreeSpace() {
			return
		}
	}
}

// checkFreeSpace returns true if the worker currently has sufficient free space.
// If not it reports unhealthy.
func (w *worker) checkFreeSpace() bool {
	statfs := syscall.Statfs_t{}
	if err := syscall.Statfs(w.rootDir, &statfs); err != nil {
		log.Error("Failed to statfs %s: %s", w.rootDir, err)
		w.Report(false, false, true, "Failed statfs: %s", err)
		return false
	} else if avail := int64(statfs.Bsize) * int64(statfs.Bavail); avail < w.diskSpace {
		log.Warning("Disk free space %d is under healthy threshold %d, will not accept new jobs until resolved", avail, w.diskSpace)
		w.Report(false, false, true, "Low disk space: %s free", humanize.Bytes(uint64(avail)))
		return false
	} else {
		log.Debug("Disk free space %d is over healthy threshold %d", avail, w.diskSpace)
		return true
	}
}
