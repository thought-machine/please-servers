module github.com/thought-machine/please-servers

go 1.18

require (
	cloud.google.com/go/pubsub v1.27.1
	cloud.google.com/go/storage v1.27.0
	github.com/bazelbuild/remote-apis v0.0.0-20230411132548-35aee1c4a425
	github.com/bazelbuild/remote-apis-sdks v0.0.0-20230419185642-269815af5db1
	github.com/dgraph-io/ristretto v0.0.3
	github.com/dustin/go-humanize v1.0.0
	github.com/go-redis/redis/v8 v8.8.3
	github.com/golang/protobuf v1.5.2
	github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus v1.0.0-rc.0
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.0.0-rc.5
	github.com/hashicorp/go-multierror v1.1.0
	github.com/hashicorp/go-retryablehttp v0.6.6
	github.com/klauspost/compress v1.12.3
	github.com/mostynb/go-grpc-compression v1.1.6
	github.com/peterebden/go-cli-init/v4 v4.0.1
	github.com/peterebden/go-copyfile v0.0.0-20200424115000-bc0baf74909c
	github.com/peterebden/go-sri v1.1.1
	github.com/prometheus/client_golang v1.14.0
	github.com/prometheus/common v0.37.0
	github.com/shirou/gopsutil v3.21.1+incompatible
	github.com/stretchr/testify v1.8.1
	github.com/thought-machine/http-admin v1.1.0
	github.com/thought-machine/please v13.4.0+incompatible
	go.uber.org/automaxprocs v1.4.0
	gocloud.dev v0.20.0
	golang.org/x/crypto v0.1.0
	golang.org/x/exp v0.0.0-20230425010034-47ecfdc1ba53
	golang.org/x/sync v0.1.0
	google.golang.org/api v0.103.0
	google.golang.org/genproto v0.0.0-20230110181048-76db0878b65f
	google.golang.org/grpc v1.53.0
	google.golang.org/protobuf v1.30.0
	gopkg.in/op/go-logging.v1 v1.0.0-20160211212156-b2cb9fa56473
)

require (
	cloud.google.com/go v0.107.0 // indirect
	cloud.google.com/go/compute v1.15.1 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v0.8.0 // indirect
	cloud.google.com/go/longrunning v0.3.0 // indirect
	github.com/StackExchange/wmi v1.2.1 // indirect
	github.com/Workiva/go-datastructures v1.0.52 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-ole/go-ole v1.2.5 // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/google/wire v0.4.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.0 // indirect
	github.com/googleapis/gax-go v2.0.2+incompatible // indirect
	github.com/googleapis/gax-go/v2 v2.7.0 // indirect
	github.com/gorilla/mux v1.7.4 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.1 // indirect
	github.com/jessevdk/go-flags v1.4.0 // indirect
	github.com/karrick/godirwalk v1.16.1 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mostynb/zstdpool-syncpool v0.0.7 // indirect
	github.com/pborman/uuid v1.2.1 // indirect
	github.com/peterebden/go-cli-init v1.3.1-0.20200329085717-d04cad1849c3 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pkg/xattr v0.4.4 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/thought-machine/go-flags v1.5.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/otel v0.20.0 // indirect
	go.opentelemetry.io/otel/metric v0.20.0 // indirect
	go.opentelemetry.io/otel/trace v0.20.0 // indirect
	golang.org/x/net v0.8.0 // indirect
	golang.org/x/oauth2 v0.4.0 // indirect
	golang.org/x/sys v0.6.0 // indirect
	golang.org/x/term v0.6.0 // indirect
	golang.org/x/text v0.8.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	gopkg.in/gcfg.v1 v1.2.3 // indirect
	gopkg.in/warnings.v0 v0.1.2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/bazelbuild/remote-apis-sdks v0.0.0-20230419185642-269815af5db1 => github.com/peterebden/remote-apis-sdks v0.0.0-20230518122452-6e5d9d252ff0
