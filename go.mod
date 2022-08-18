module github.com/thought-machine/please-servers

go 1.16

require (
	cloud.google.com/go v0.103.0 // indirect
	cloud.google.com/go/compute v1.8.0 // indirect
	cloud.google.com/go/pubsub v1.19.0
	cloud.google.com/go/storage v1.25.0
	github.com/StackExchange/wmi v1.2.1 // indirect
	github.com/Workiva/go-datastructures v1.0.52 // indirect
	github.com/bazelbuild/remote-apis v0.0.0-20220727181745-aa29b91f336b
	github.com/bazelbuild/remote-apis-sdks v0.0.0-20220518145948-4b34b65cbf91
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/dgraph-io/ristretto v0.1.0
	github.com/dustin/go-humanize v1.0.0
	github.com/go-redis/redis/v8 v8.8.3
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/protobuf v1.5.2
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/googleapis/gax-go/v2 v2.5.1 // indirect
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hashicorp/go-retryablehttp v0.6.6
	github.com/karrick/godirwalk v1.16.1 // indirect
	github.com/klauspost/compress v1.15.9
	github.com/mostynb/go-grpc-compression v1.1.17
	github.com/mostynb/zstdpool-syncpool v0.0.12 // indirect
	github.com/pborman/uuid v1.2.1 // indirect
	github.com/peterebden/go-cli-init/v4 v4.0.2
	github.com/peterebden/go-copyfile v0.0.0-20200424115000-bc0baf74909c
	github.com/peterebden/go-sri v1.1.1
	github.com/pkg/xattr v0.4.8 // indirect
	github.com/prometheus/client_golang v1.13.0
	github.com/prometheus/common v0.37.0
	github.com/shirou/gopsutil v3.21.1+incompatible
	github.com/stretchr/testify v1.7.1
	github.com/thought-machine/go-flags v1.6.2 // indirect
	github.com/thought-machine/http-admin v1.1.0
	github.com/thought-machine/please v13.4.0+incompatible
	go.uber.org/automaxprocs v1.5.1
	gocloud.dev v0.26.0
	golang.org/x/crypto v0.0.0-20220817201139-bc19a97f63c8
	golang.org/x/net v0.0.0-20220812174116-3211cb980234 // indirect
	golang.org/x/oauth2 v0.0.0-20220808172628-8227340efae7 // indirect
	golang.org/x/sync v0.0.0-20220722155255-886fb9371eb4
	golang.org/x/sys v0.0.0-20220817070843-5a390386f1f2 // indirect
	golang.org/x/term v0.0.0-20220722155259-a9ba230a4035 // indirect
	google.golang.org/api v0.93.0
	google.golang.org/genproto v0.0.0-20220817144833-d7fd3f11b9b1
	google.golang.org/grpc v1.48.0
	google.golang.org/protobuf v1.28.1
	gopkg.in/gcfg.v1 v1.2.3 // indirect
	gopkg.in/op/go-logging.v1 v1.0.0-20160211212156-b2cb9fa56473
	gopkg.in/warnings.v0 v0.1.2 // indirect
)

replace (
	github.com/bazelbuild/remote-apis v0.0.0-20201113154229-1e9ccef3705c => github.com/peterebden/remote-apis v0.0.0-20201218092846-5306a2d66a1b
	github.com/bazelbuild/remote-apis-sdks v0.0.0-20210219194604-fdb5dae38ca8 => github.com/peterebden/remote-apis-sdks v0.0.0-20210607091647-d0d55ea13e2f
)
