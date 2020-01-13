// Package metrics provides some common functionality for metrics.
package metrics

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/op/go-logging.v1"
)

var log = logging.MustGetLogger("metrics")

// Serve serves metrics on the given port if it is not zero.
func Serve(port int) {
	if port != 0 {
		log.Notice("Serving metrics on :%d", port)
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
			log.Errorf("Failed to serve metrics: %s", err)
		}
	}
}
