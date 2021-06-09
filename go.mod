module github.com/thought-machine/please-servers

go 1.16

require (
	cloud.google.com/go/pubsub v1.3.1
	cloud.google.com/go/storage v1.10.0 // indirect
	github.com/Workiva/go-datastructures v1.0.52 // indirect
	github.com/bazelbuild/remote-apis v0.0.0-20201113154229-1e9ccef3705c
	github.com/bazelbuild/remote-apis-sdks v0.0.0-20210219194604-fdb5dae38ca8
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgraph-io/ristretto v0.0.3
	github.com/dustin/go-humanize v1.0.0
	github.com/go-redis/redis/v8 v8.8.3 // indirect
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/golang/protobuf v1.4.2
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/google/wire v0.4.0
	github.com/googleapis/gax-go/v2 v2.0.5 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/go-multierror v1.1.0
	github.com/hashicorp/go-retryablehttp v0.6.6
	github.com/jessevdk/go-flags v1.4.0 // indirect
	github.com/karrick/godirwalk v1.16.1 // indirect
	github.com/kevinburke/go-bindata v3.21.0+incompatible // indirect
	github.com/klauspost/compress v1.11.8
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mostynb/go-grpc-compression v1.1.6
	github.com/op/go-logging v0.0.0-20160315200505-970db520ece7 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/peterebden/go-cli-init/v2 v2.0.1
	github.com/peterebden/go-cli-init/v3 v3.1.1
	github.com/peterebden/go-cli-init/v4 v4.0.1
	github.com/peterebden/go-copyfile v0.0.0-20200424115000-bc0baf74909c
	github.com/peterebden/go-sri v1.1.1
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pkg/xattr v0.4.3 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.10.0
	github.com/prometheus/procfs v0.1.3 // indirect
	github.com/shirou/gopsutil v3.21.1+incompatible
	github.com/sirupsen/logrus v1.6.0 // indirect
	github.com/stretchr/objx v0.3.0 // indirect
	github.com/stretchr/testify v1.7.0
	github.com/thought-machine/http-admin v1.1.0
	github.com/thought-machine/please v13.4.0+incompatible
	go.opencensus.io v0.22.4 // indirect
	go.uber.org/zap v1.15.0 // indirect
	go4.org v0.0.0-20200411211856-f5505b9728dd // indirect
	gocloud.dev v0.20.0
	golang.org/x/build v0.0.0-20200714230912-6a30d041565f // indirect
	golang.org/x/crypto v0.0.0-20200709230013-948cd5f35899
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
	golang.org/x/text v0.3.3 // indirect
	google.golang.org/api v0.30.0
	google.golang.org/appengine v1.6.6 // indirect
	google.golang.org/genproto v0.0.0-20200904004341-0bd0a958aa1d
	google.golang.org/grpc v1.31.1
	google.golang.org/protobuf v1.25.0
	gopkg.in/gcfg.v1 v1.2.3 // indirect
	gopkg.in/op/go-logging.v1 v1.0.0-20160211212156-b2cb9fa56473
	gopkg.in/warnings.v0 v0.1.2 // indirect
)

replace (
	github.com/bazelbuild/remote-apis v0.0.0-20201113154229-1e9ccef3705c => github.com/peterebden/remote-apis v0.0.0-20201218092846-5306a2d66a1b
	github.com/bazelbuild/remote-apis-sdks v0.0.0-20210219194604-fdb5dae38ca8 => github.com/peterebden/remote-apis-sdks v0.0.0-20210607091647-d0d55ea13e2f
)
