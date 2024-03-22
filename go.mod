module github.com/thought-machine/please-servers

go 1.21

require (
	cloud.google.com/go/profiler v0.4.0
	cloud.google.com/go/pubsub v1.33.0
	cloud.google.com/go/storage v1.36.0
	github.com/bazelbuild/remote-apis v0.0.0-20230411132548-35aee1c4a425
	github.com/bazelbuild/remote-apis-sdks v0.0.0-20230419185642-269815af5db1
	github.com/dgraph-io/ristretto v0.1.1
	github.com/dustin/go-humanize v1.0.1
	github.com/go-redis/redis/v8 v8.11.5
	github.com/golang/protobuf v1.5.3
	github.com/golang/snappy v0.0.4 // This is a required dep that Go mod tidy gets rid of. This breaks Please if it gets removed though.
	github.com/google/go-cmp v0.6.0 // This is a required dep that Go mod tidy gets rid of. This breaks Please if it gets removed though.
	github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus v1.0.0
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.0.1
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hashicorp/go-retryablehttp v0.7.5
	github.com/klauspost/compress v1.17.4
	github.com/mostynb/go-grpc-compression v1.2.2
	github.com/peterebden/go-cli-init/v4 v4.0.2
	github.com/peterebden/go-copyfile v0.0.0-20200424115000-bc0baf74909c
	github.com/peterebden/go-sri v1.1.1
	github.com/prometheus/client_golang v1.18.0
	github.com/prometheus/common v0.45.0
	github.com/shirou/gopsutil v3.21.11+incompatible
	github.com/stretchr/testify v1.8.4
	github.com/thought-machine/http-admin v1.1.1
	go.uber.org/automaxprocs v1.5.3
	gocloud.dev v0.36.0
	golang.org/x/crypto v0.17.0
	golang.org/x/exp v0.0.0-20240103183307-be819d1f06fc
	golang.org/x/sync v0.6.0
	golang.org/x/time v0.5.0
	google.golang.org/api v0.155.0
	google.golang.org/genproto v0.0.0-20240102182953-50ed04b92917
	google.golang.org/genproto/googleapis/bytestream v0.0.0-20240102182953-50ed04b92917
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240102182953-50ed04b92917
	google.golang.org/grpc v1.60.1
	google.golang.org/protobuf v1.32.0
	gopkg.in/op/go-logging.v1 v1.0.0-20160211212156-b2cb9fa56473
)

require (
	cloud.google.com/go v0.111.0 // indirect
	cloud.google.com/go/compute v1.23.3 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v1.1.5 // indirect
	cloud.google.com/go/longrunning v0.5.4 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/golang/glog v1.2.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/pprof v0.0.0-20231229205709-960ae82b1e42 // indirect
	github.com/google/s2a-go v0.1.7 // indirect
	github.com/google/uuid v1.5.0 // indirect
	github.com/google/wire v0.5.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.2 // indirect
	github.com/googleapis/gax-go/v2 v2.12.0 // indirect
	github.com/gorilla/mux v1.8.1 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/matttproud/golang_protobuf_extensions/v2 v2.0.0 // indirect
	github.com/mostynb/zstdpool-syncpool v0.0.13 // indirect
	github.com/pborman/uuid v1.2.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pkg/xattr v0.4.9 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.5.0 // indirect
	github.com/prometheus/procfs v0.12.0 // indirect
	github.com/thought-machine/go-flags v1.6.3 // indirect
	github.com/yusufpapurcu/wmi v1.2.3 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.46.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.46.1 // indirect
	go.opentelemetry.io/otel v1.21.0 // indirect
	go.opentelemetry.io/otel/metric v1.21.0 // indirect
	go.opentelemetry.io/otel/trace v1.21.0 // indirect
	golang.org/x/net v0.19.0 // indirect
	golang.org/x/oauth2 v0.15.0 // indirect
	golang.org/x/sys v0.16.0 // indirect
	golang.org/x/term v0.16.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/xerrors v0.0.0-20231012003039-104605ab7028 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240102182953-50ed04b92917 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/bazelbuild/remote-apis-sdks v0.0.0-20230419185642-269815af5db1 => github.com/peterebden/remote-apis-sdks v0.0.0-20230519151942-2a9420921957
