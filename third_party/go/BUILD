package(default_visibility = ["PUBLIC"])

go_toolchain(
    name = "toolchain",
    version = "1.19",
)

go_module(
    name = "logging",
    module = "gopkg.in/op/go-logging.v1",
    version = "b2cb9fa56473e98db8caba80237377e83fe44db5",
)

go_module(
    name = "terminal",
    install = ["ssh/terminal"],
    module = "golang.org/x/crypto",
    version = "7b85b097bf7527677d54d3220065e966a0e3b613",
)

go_module(
    name = "go-flags",
    module = "github.com/thought-machine/go-flags",
    version = "v1.5.1",
    deps = [
        ":unix",
    ],
)

go_module(
    name = "net",
    install = ["..."],
    module = "golang.org/x/net",
    version = "136a25c244d3019482a795d728110278d6ba09a4",
    deps = [
        ":terminal",
        ":text",
    ],
)

go_module(
    name = "tools",
    install = [
        "cover",
        "go/...",
    ],
    module = "golang.org/x/tools",
    strip = [
        "go/loader/testdata",
        "go/internal/gccgoimporter/testdata",
        "go/gcimporter15/testdata",
    ],
    version = "2ae76fd1560b622911f444c1e66b70a857e1de67",
)

go_module(
    name = "text",
    install = ["..."],
    module = "golang.org/x/text",
    version = "4e4a3210bb54bb31f6ab2cdca2edcc0b50c420c1",
    deps = [
        ":tools",
    ],
)

go_module(
    name = "grpc",
    exported_deps = [":net"],
    install = [
        ".",
        "attributes",
        "backoff",
        "balancer",
        "balancer/base",
        "balancer/grpclb",
        "balancer/grpclb/...",
        "balancer/roundrobin",
        "binarylog/...",
        "codes",
        "connectivity",
        "credentials",
        "credentials/alts",
        "credentials/alts/...",
        "credentials/google",
        "credentials/oauth",
        "encoding",
        "encoding/gzip",
        "encoding/proto",
        "grpclog",
        "health",
        "health/grpc_health_v1",
        "internal",
        "internal/...",
        "keepalive",
        "metadata",
        "peer",
        "reflection",
        "reflection/...",
        "resolver",
        "resolver/dns",
        "resolver/passthrough",
        "serviceconfig",
        "stats",
        "status",
        "tap",
    ],
    module = "google.golang.org/grpc",
    version = "v1.48.0",
    deps = [
        ":cmp",
        ":envoyproxy-control-plane",
        ":oauth2",
        ":protobuf",
        ":rpc",
        ":unix",
    ],
)

go_module(
    name = "envoyproxy-control-plane",
    install = ["envoy/type/matcher/v3"],
    module = "github.com/envoyproxy/go-control-plane",
    version = "v0.10.3",
    deps = [
        ":envoyproxy-protoc-validate",
        ":protobuf",
        ":xds",
    ],
)

go_module(
    name = "envoyproxy-protoc-validate",
    install = ["validate"],
    module = "github.com/envoyproxy/protoc-gen-validate",
    version = "v0.6.7",
    deps = [
        ":protobuf",
    ],
)

go_module(
    name = "xds",
    install = ["udpa/annotations"],
    module = "github.com/cncf/xds/go",
    version = "v0.0.0-20220314180256-7f1daf1720fc",
    deps = [
        ":envoyproxy-protoc-validate",
        ":protobuf",
    ],
)

go_mod_download(
    name = "genproto_download",
    module = "google.golang.org/genproto",
    version = "063164c882e6716797f16f5accd450506145132d",
)

go_module(
    name = "rpc",
    download = ":genproto_download",
    install = [
        "googleapis/rpc/code",
        "googleapis/rpc/status",
    ],
    module = "google.golang.org/genproto",
    deps = [":protobuf"],
)

go_module(
    name = "protoc-gen-go",
    binary = True,
    download = ":protobuf_download",
    install = ["protoc-gen-go"],
    module = "github.com/golang/protobuf",
    deps = [":golang-protobuf"],
)

go_module(
    name = "protobuf",
    download = ":protobuf_download",
    exported_deps = [
        ":golang-protobuf",
    ],
    install = ["..."],
    module = "github.com/golang/protobuf",
    strip = [
        "proto/proto3_proto",
        "conformance",
    ],
    deps = [
        ":cmp",
        ":golang-protobuf",
    ],
)

go_mod_download(
    name = "protobuf_download",
    module = "github.com/golang/protobuf",
    version = "v1.4.2",
)

go_module(
    name = "spew",
    install = ["spew"],
    module = "github.com/davecgh/go-spew",
    version = "d8f796af33cc11cb798c1aaeb27a4ebc5099927d",
)

go_module(
    name = "difflib",
    install = ["difflib"],
    module = "github.com/pmezard/go-difflib",
    version = "5d4384ee4fb2527b0a1256a821ebfc92f91efefc",
)

go_module(
    name = "testify",
    install = [
        "assert",
        "require",
    ],
    module = "github.com/stretchr/testify",
    version = "v1.3.0",
    deps = [
        ":difflib",
        ":spew",
    ],
)

go_module(
    name = "grpc-middleware",
    install = [
        ".",
        "recovery",
        "retry",
        "util/metautils",
        "util/backoffutils",
    ],
    module = "github.com/grpc-ecosystem/go-grpc-middleware",
    version = "fa8fef87dcecac0bda02d36abb3c790ab9e0030b",
    deps = [
        ":grpc",
        ":net",
        ":protobuf",
    ],
)

go_module(
    name = "errgroup",
    install = [
        "errgroup",
        "semaphore",
    ],
    module = "golang.org/x/sync",
    version = "09787c993a3ab68e3d1f5c9b2394ab9433f391be",
    deps = [":net"],
)

go_module(
    name = "unix",
    install = [
        "cpu",
        "execabs",
        "internal/unsafeheader",
        "unix",
    ],
    module = "golang.org/x/sys",
    version = "v0.0.0-20210423082822-04245dca01da",
)

go_module(
    name = "errors",
    module = "github.com/pkg/errors",
    version = "30136e27e2ac8d167177e8a583aa4c3fea5be833",
)

go_module(
    name = "cli-init",
    install = [
        "flags",
        "logging",
    ],
    module = "github.com/peterebden/go-cli-init/v4",
    version = "v4.0.2",
    deps = [
        ":go-flags",
        ":humanize",
        ":logging",
        ":terminal",
    ],
)

go_module(
    name = "remote-apis",
    download = ":remote-apis-download",
    install = [
        "build/bazel/...",
    ],
    module = "github.com/bazelbuild/remote-apis/",
    deps = [
        ":genproto",
        ":grpc",
        ":protobuf",
        ":rpc",
    ],
)

go_mod_download(
    name = "remote-apis-download",
    module = "github.com/peterebden/remote-apis",
    version = "5306a2d66a1b879350f1a474cc639d569953fb48",
)

go_module(
    name = "remote-apis-sdks",
    download = ":remote-apis-sdks_download",
    install = ["go/..."],
    module = "github.com/bazelbuild/remote-apis-sdks",
    deps = [
        ":cmp",
        ":errgroup",
        ":errors",
        ":genproto",
        ":glog",
        ":golang-protobuf",
        ":grpc",
        ":oauth2",
        ":pborman_uuid",
        ":protobuf",
        ":remote-apis",
        ":rpc",
        ":zstd",
        ":zstdpool",
    ],
)

go_mod_download(
    name = "remote-apis-sdks_download",
    module = "github.com/peterebden/remote-apis-sdks",
    version = "52a570458adc5c5c6be63b597ca60170cb199646",
)

go_module(
    name = "golang-protobuf",
    install = ["..."],
    module = "google.golang.org/protobuf",
    version = "v1.25.0",
    deps = [
        ":cmp",
    ],
)

go_module(
    name = "cmp",
    install = [
        "cmp",
        "cmp/...",
    ],
    module = "github.com/google/go-cmp",
    version = "b1c9c4891a6525d98001fea424c8926c6d77bb56",
)

go_module(
    name = "glog",
    download = ":glog_download",
    module = "github.com/golang/glog",
)

go_mod_download(
    name = "glog_download",
    module = "github.com/peterebden/glog",
    version = "78e7b285a1f26e8ca142f6f7b584511bc551988b",
)

go_module(
    name = "oauth2",
    install = [
        ".",
        "google",
        "internal",
        "jws",
        "jwt",
    ],
    module = "golang.org/x/oauth2",
    version = "bf48bf16ab8d622ce64ec6ce98d2c98f916b6303",
    deps = [
        ":cloud-metadata",
        ":net",
    ],
)

go_module(
    name = "genproto",
    download = ":genproto_download",
    install = [
        "googleapis/api/annotations",
        "googleapis/longrunning",
        "googleapis/bytestream",
        "googleapis/iam/v1",
        "googleapis/iam/credentials/v1",
        "googleapis/pubsub/v1",
        "protobuf/field_mask",
        "googleapis/type/expr",
    ],
    module = "google.golang.org/genproto",
    deps = [
        ":grpc",
        ":protobuf",
        ":rpc",
    ],
)

go_module(
    name = "uuid",
    module = "github.com/google/uuid",
    version = "v1.1.1",
)

go_module(
    name = "pborman_uuid",
    module = "github.com/pborman/uuid",
    version = "v1.2.0",
    deps = [":uuid"],
)

go_module(
    name = "ristretto",
    install = [
        ".",
        "...",
    ],
    module = "github.com/dgraph-io/ristretto",
    version = "v0.0.3",
    deps = [":xxhash"],
)

go_module(
    name = "xxhash",
    module = "github.com/cespare/xxhash",
    version = "v1.1.0",
)

go_module(
    name = "humanize",
    module = "github.com/dustin/go-humanize",
    version = "v1.0.0",
)

go_module(
    name = "retryablehttp",
    module = "github.com/hashicorp/go-retryablehttp",
    version = "v0.6.1",
    deps = [
        ":cleanhttp",
        ":hclog",
    ],
)

go_module(
    name = "sri",
    module = "github.com/peterebden/go-sri",
    version = "v1.1.1",
)

go_module(
    name = "go-multierror",
    module = "github.com/hashicorp/go-multierror",
    version = "v1.1.1",
    deps = [":errwrap"],
)

go_module(
    name = "errwrap",
    module = "github.com/hashicorp/errwrap",
    version = "v1.0.0",
)

go_module(
    name = "cleanhttp",
    module = "github.com/hashicorp/go-cleanhttp",
    version = "v0.5.1",
)

go_module(
    name = "hclog",
    module = "github.com/hashicorp/go-hclog",
    version = "v0.12.0",
    deps = [
        ":color",
        ":isatty",
    ],
)

go_module(
    name = "isatty",
    module = "github.com/mattn/go-isatty",
    version = "v0.0.12",
    deps = [":unix"],
)

go_module(
    name = "colorable",
    module = "github.com/mattn/go-colorable",
    version = "v0.1.8",
    deps = [
        ":isatty",
    ],
)

go_module(
    name = "color",
    module = "github.com/fatih/color",
    version = "v1.9.0",
    deps = [
        ":colorable",
        ":isatty",
        ":unix",
    ],
)

go_module(
    name = "copyfile",
    module = "github.com/peterebden/go-copyfile",
    version = "bc0baf74909cb55565cf9536cb003f04da55372d",
)

go_module(
    name = "godirwalk",
    module = "github.com/karrick/godirwalk",
    strip = [
        "examples",
        "testdata",
        "vendor",
    ],
    version = "v1.7.0",
    deps = [":errors"],
)

go_module(
    name = "http-admin",
    module = "github.com/thought-machine/http-admin",
    version = "v1.1.0",
    deps = [
        ":logging",
        ":mux",
        ":net",
        "//third_party/go/prometheus",
    ],
)

go_module(
    name = "mux",
    module = "github.com/gorilla/mux",
    version = "v1.7.4",
)

go_module(
    name = "xattr",
    module = "github.com/pkg/xattr",
    version = "v0.4.1",
    deps = [":unix"],
)

go_module(
    name = "psutil",
    install = [
        "internal/common",
        "mem",
    ],
    module = "github.com/shirou/gopsutil",
    version = "v2.17.09",
    deps = [":unix"],
)

go_module(
    name = "zstd",
    install = [
        ".",
        "fse",
        "huff0",
        "snappy",
        "zstd",
        "zstd/...",
    ],
    module = "github.com/klauspost/compress",
    version = "v1.11.0",
)

go_module(
    name = "go-grpc-compression",
    install = ["zstd"],
    module = "github.com/mostynb/go-grpc-compression",
    version = "v1.1.2",
    deps = [
        ":grpc",
        ":zstd",
    ],
)

go_mod_download(
    name = "cloud_download",
    module = "cloud.google.com/go",
    version = "v0.78.0",
)

go_module(
    name = "cloud-metadata",
    download = ":cloud_download",
    install = ["compute/metadata"],
    module = "cloud.google.com/go",
)

go_module(
    name = "google-cloud-go",
    download = ":cloud_download",
    install = [
        "iam",
        "iam/credentials/...",
        "internal",
        "internal/optional",
        "internal/pubsub",
        "internal/trace",
        "internal/version",
    ],
    module = "cloud.google.com/go",
    deps = [
        ":api",
        ":errgroup",
        ":gax-go",
        ":genproto",
        ":go.opencensus.io",
    ],
)

go_module(
    name = "cloud_pubsub",
    install = [
        ".",
        "apiv1",
    ],
    module = "cloud.google.com/go/pubsub",
    version = "v1.10.0",
    deps = [
        ":api",
        ":gax-go",
        ":genproto",
        ":go.opencensus.io",
        ":google-cloud-go",
    ],
)

go_module(
    name = "cloud-storage",
    module = "cloud.google.com/go/storage",
    version = "v1.11.0",
    deps = [
        ":api",
        ":errgroup",
        ":gax-go",
        ":genproto",
        ":go.opencensus.io",
        ":google-cloud-go",
    ],
)

go_module(
    name = "gax-go",
    module = "github.com/googleapis/gax-go/v2",
    version = "v2.0.5",
    deps = [
        ":grpc",
    ],
)

go_module(
    name = "go.opencensus.io",
    install = [
        ".",
        "...",
    ],
    module = "go.opencensus.io",
    version = "v0.22.1",
    deps = [
        ":groupcache",
        ":grpc",
        ":net",
    ],
)

go_module(
    name = "groupcache",
    install = [
        "lru",
    ],
    module = "github.com/golang/groupcache",
    version = "404acd9df4cc9859d64fb9eed42e5c026187287a",
)

go_module(
    name = "api",
    install = [
        "calendar/v3",
        "googleapi",
        "googleapi/transport",
        "idtoken",
        "internal",
        "internal/gensupport",
        "internal/impersonate",
        "internal/third_party/uritemplates",
        "iterator",
        "option",
        "option/internaloption",
        "storage/v1",
        "sheets/v4",
        "support/bundler",
        "transport",
        "transport/bytestream",
        "transport/cert",
        "transport/grpc",
        "transport/http",
        "transport/http/internal/propagation",
        "transport/internal/dca",
        "dataflow/...",
        "compute/v1",
        "container/v1",
        "drive/v3",
    ],
    module = "google.golang.org/api",
    version = "v0.40.0",
    deps = [
        ":errgroup",
        ":gax-go",
        ":genproto",
        ":go.opencensus.io",
        ":grpc",
        ":oauth2",
    ],
)

go_module(
    name = "redis",
    install = [
        "...",
    ],
    module = "github.com/go-redis/redis/v8",
    version = "v8.3.3",
    deps = [
        ":opentelemetry",
        ":rendezvous",
        ":xxhash_v2",
    ],
)

go_module(
    name = "opentelemetry",
    install = [
        ".",
        "api/global",
        "api/global/internal",
        "api/metric",
        "api/metric/registry",
        "api/trace",
        "codes",
        "internal/...",
        "label",
        "unit",
    ],
    licences = ["apache-2.0"],
    module = "go.opentelemetry.io/otel",
    version = "v0.13.0",
)

go_module(
    name = "xxhash_v2",
    module = "github.com/cespare/xxhash/v2",
    version = "v2.1.1",
)

go_module(
    name = "rendezvous",
    licences = ["mit"],
    module = "github.com/dgryski/go-rendezvous",
    version = "9f7001d12a5f0021fd3283525f888b5814ccee27",
)

go_module(
    name = "automaxprocs",
    install = [
        "...",
    ],
    licences = ["MIT"],
    module = "go.uber.org/automaxprocs",
    version = "v1.4.0",
)

go_module(
    name = "json-iterator",
    install = ["..."],
    licences = ["MIT"],
    module = "github.com/json-iterator/go",
    version = "v1.1.6",
    deps = [
        ":concurrent",
        ":reflect2",
    ],
)

go_module(
    name = "concurrent",
    install = ["..."],
    licences = ["Apache-2.0"],
    module = "github.com/modern-go/concurrent",
    version = "1.0.3",
)

go_module(
    name = "reflect2",
    install = ["..."],
    licences = ["Apache-2.0"],
    module = "github.com/modern-go/reflect2",
    version = "v1.0.1",
    deps = [":concurrent"],
)

go_module(
    name = "kingpin",
    licences = ["MIT"],
    module = "gopkg.in/alecthomas/kingpin.v2",
    version = "v2.2.6",
    deps = [
        ":template",
        ":units",
    ],
)

go_module(
    name = "template",
    install = [
        "",
        "parse",
    ],
    licences = ["BSD-3-Clause"],
    module = "github.com/alecthomas/template",
    version = "a0175ee3bccc567396460bf5acd36800cb10c49c",
)

go_module(
    name = "units",
    licences = ["MIT"],
    module = "github.com/alecthomas/units",
    version = "2efee857e7cfd4f3d0138cc3cbb1b4966962b93a",
)

go_module(
    name = "logrus",
    licences = ["MIT"],
    module = "github.com/sirupsen/logrus",
    version = "v1.6.0",
    deps = [
        ":terminal",
        ":unix",
    ],
)

go_module(
    name = "x_term",
    install = ["..."],
    licences = ["BSD-3-Clause"],
    module = "golang.org/x/term",
    version = "v0.0.0-20210615171337-6886f2dfbf5b",
    deps = [":unix"],
)

go_module(
    name = "zstdpool",
    licences = ["Apache-2.0"],
    module = "github.com/mostynb/zstdpool-syncpool",
    version = "v0.0.7",
    visibility = ["PUBLIC"],
    deps = [":zstd"],
)
