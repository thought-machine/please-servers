module github.com/thought-machine/please-servers

go 1.16

require (
	cloud.google.com/go/pubsub v1.3.1
	cloud.google.com/go/storage v1.25.0
	github.com/StackExchange/wmi v1.2.1 // indirect
	github.com/Workiva/go-datastructures v1.0.52 // indirect
	github.com/bazelbuild/remote-apis v0.0.0-20201113154229-1e9ccef3705c
	github.com/bazelbuild/remote-apis-sdks v0.0.0-20210219194604-fdb5dae38ca8
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/dgraph-io/ristretto v0.0.3
	github.com/dustin/go-humanize v1.0.0
	github.com/go-redis/redis/v8 v8.8.3
	github.com/golang/protobuf v1.5.2
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/go-multierror v1.1.0
	github.com/hashicorp/go-retryablehttp v0.6.6
	github.com/karrick/godirwalk v1.16.1 // indirect
	github.com/klauspost/compress v1.11.8
	github.com/mostynb/go-grpc-compression v1.1.6
	github.com/peterebden/go-cli-init/v4 v4.0.1
	github.com/peterebden/go-copyfile v0.0.0-20200424115000-bc0baf74909c
	github.com/peterebden/go-sri v1.1.1
	github.com/pkg/xattr v0.4.3 // indirect
	github.com/prometheus/client_golang v1.11.1
	github.com/prometheus/common v0.26.0
	github.com/shirou/gopsutil v3.21.1+incompatible
	github.com/stretchr/testify v1.7.0
	github.com/thought-machine/http-admin v1.1.0
	github.com/thought-machine/please v13.4.0+incompatible
	go.uber.org/automaxprocs v1.4.0
	gocloud.dev v0.20.0
	golang.org/x/crypto v0.0.0-20200709230013-948cd5f35899
	golang.org/x/sync v0.0.0-20220601150217-0de741cfad7f
	google.golang.org/api v0.88.0
	google.golang.org/genproto v0.0.0-20220720214146-176da50484ac
	google.golang.org/grpc v1.48.0
	google.golang.org/protobuf v1.28.0
	gopkg.in/gcfg.v1 v1.2.3 // indirect
	gopkg.in/op/go-logging.v1 v1.0.0-20160211212156-b2cb9fa56473
	gopkg.in/warnings.v0 v0.1.2 // indirect
)

replace (
	github.com/bazelbuild/remote-apis v0.0.0-20201113154229-1e9ccef3705c => github.com/peterebden/remote-apis v0.0.0-20201218092846-5306a2d66a1b
	github.com/bazelbuild/remote-apis-sdks v0.0.0-20210219194604-fdb5dae38ca8 => github.com/peterebden/remote-apis-sdks v0.0.0-20220818131950-94bf9a163994
)
