package(default_visibility = ["PUBLIC"])

go_module(
    name = "logging",
    module = "gopkg.in/op/go-logging.v1",
    version = "b2cb9fa56473e98db8caba80237377e83fe44db5",
)

go_module(
    name = "terminal",
    module = "golang.org/x/crypto",
    install = ["ssh/terminal"],
    version = "7b85b097bf7527677d54d3220065e966a0e3b613",
)

go_module(
    name = "go-bindata",
    binary = True,
    module = "github.com/kevinburke/go-bindata",
    install = ["..."],
    version = "46eb4c183bfc1ebb527d9d19bcded39476302eb8",
    strip = ["testdata"],
)

go_module(
    name = "go-flags",
    module = "github.com/thought-machine/go-flags",
    version = "v1.5.0",
    deps = [
        ":unix",
    ],
)

go_module(
    name = "net",
    module = "golang.org/x/net",
    install = ["..."],
    version = "136a25c244d3019482a795d728110278d6ba09a4",
    deps = [
        ":terminal",
        ":text",
    ],
)

go_module(
    name = "tools",
    module = "golang.org/x/tools",
    install = [
        "cover",
        "go/...",
    ],
    strip = [
        "go/loader/testdata",
        "go/internal/gccgoimporter/testdata",
        "go/gcimporter15/testdata",
    ],
    version = "2ae76fd1560b622911f444c1e66b70a857e1de67",
)

go_module(
    name = "text",
    module = "golang.org/x/text",
    install = ["..."],
    version = "4e4a3210bb54bb31f6ab2cdca2edcc0b50c420c1",
    deps = [
        ":tools",
    ],
)

go_module(
    name = "grpc",
    exported_deps = [":net"],
    module = "google.golang.org/grpc",
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
    version = "v1.36.0",
    deps = [
        ":cmp",
        ":oauth2",
        ":protobuf",
        ":rpc",
        ":unix",
    ],
)

go_mod_download(
    name = "genproto_download",
    module = "google.golang.org/genproto",
    version = "063164c882e6716797f16f5accd450506145132d",
)

go_module(
    name = "rpc",
    module = "google.golang.org/genproto",
    install = [
        "googleapis/rpc/code",
        "googleapis/rpc/status",
    ],
    download = ":genproto_download",
    deps = [":protobuf"],
)

go_module(
    name = "protoc-gen-go",
    binary = True,
    download = ":protobuf_download",
    install = ["protoc-gen-go"],
    module = "github.com/golang/protobuf",
    deps = [":protobuf"],
)

go_module(
    name = "protobuf",
    exported_deps = [
        ":golang-protobuf",
    ],
    module = "github.com/golang/protobuf",
    install = ["..."],
    download = ":protobuf_download",
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
    module = "github.com/davecgh/go-spew",
    install = ["spew"],
    version = "d8f796af33cc11cb798c1aaeb27a4ebc5099927d",
)

go_module(
    name = "testify",
    module = "github.com/stretchr/testify",
    install = [
        "assert",
        "require",
        "vendor/github.com/davecgh/go-spew/spew",
        "vendor/github.com/pmezard/go-difflib/difflib",
    ],
    version = "f390dcf405f7b83c997eac1b06768bb9f44dec18",
    deps = [":spew"],
)

go_module(
    name = "grpc-middleware",
    module = "github.com/grpc-ecosystem/go-grpc-middleware",
    install = [
        ".",
        "recovery",
        "retry",
        "util/metautils",
        "util/backoffutils",
    ],
    version = "fa8fef87dcecac0bda02d36abb3c790ab9e0030b",
    deps = [
        ":grpc",
        ":net",
        ":protobuf",
    ],
)

go_module(
    name = "errgroup",
    module = "golang.org/x/sync",
    install = [
        "errgroup",
        "semaphore",
    ],
    version = "09787c993a3ab68e3d1f5c9b2394ab9433f391be",
    deps = [":net"],
)

go_module(
    name = "unix",
    module = "golang.org/x/sys",
    install = [
        "internal/unsafeheader",
        "unix",
    ],
    version = "ddb9806d33aed8dbaac1cd6f1cba58952e87f933",
)

go_module(
    name = "errors",
    module = "github.com/pkg/errors",
    version = "30136e27e2ac8d167177e8a583aa4c3fea5be833",
)

go_module(
    name = "cli-init",
    module = "github.com/peterebden/go-cli-init/v3",
    version = "v3.1.1",
    deps = [
        ":go-flags",
        ":humanize",
        ":logging",
        ":terminal",
    ],
)

go_module(
    name = "remote-apis",
    module = "github.com/bazelbuild/remote-apis/",
    install = [
        "build/bazel/...",
    ],
    download = ":remote-apis-download",
    deps = [
        ":annotations",
        ":grpc",
        ":longrunning",
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
    module = "github.com/bazelbuild/remote-apis-sdks",
    install = ["go/..."],
    download = ":remote-apis-sdks_download",
    deps = [
        ":annotations",
        ":bytestream",
        ":cmp",
        ":errgroup",
        ":errors",
        ":glog",
        ":golang-protobuf",
        ":grpc",
        ":longrunning",
        ":oauth2",
        ":pborman_uuid",
        ":protobuf",
        ":remote-apis",
        ":rpc",
        ":zstd",
    ],
)

go_mod_download(
    name = "remote-apis-sdks_download",
    module = "github.com/peterebden/remote-apis-sdks",
    version = "5e7b88ce7342f4b3a53c29b6e6962f29e0522d12",
)

go_module(
    name = "golang-protobuf",
    module = "google.golang.org/protobuf",
    install = ["..."],
    version = "v1.25.0",
    deps = [
        ":cmp",
    ],
)

go_module(
    name = "cmp",
    module = "github.com/google/go-cmp",
    install = [
        "cmp",
        "cmp/...",
    ],
    version = "b1c9c4891a6525d98001fea424c8926c6d77bb56",
)

go_module(
    name = "glog",
    module = "github.com/golang/glog",
    download = ":glog_download",
)

go_mod_download(
    name = "glog_download",
    module = "github.com/peterebden/glog",
    version = "78e7b285a1f26e8ca142f6f7b584511bc551988b",
)

go_module(
    name = "oauth2",
    module = "golang.org/x/oauth2",
    install = [
        ".",
        "google",
        "internal",
        "jws",
        "jwt",
    ],
    version = "bf48bf16ab8d622ce64ec6ce98d2c98f916b6303",
    visibility = ["//third_party/go/..."],
    deps = [
        ":cloud-metadata",
        ":net",
    ],
)

# TODO(peterebden): Splitting up this & the other bits of genproto is a bit of
#                   a hack. Replace once we have a better way of describing Go modules.
go_module(
    name = "annotations",
    module = "google.golang.org/genproto",
    install = ["googleapis/api/annotations"],
    download = ":genproto_download",
    deps = [
        ":grpc",
        ":protobuf",
    ],
)

go_module(
    name = "longrunning",
    module = "google.golang.org/genproto",
    install = ["googleapis/longrunning"],
    download = ":genproto_download",
    deps = [
        ":annotations",
        ":grpc",
        ":protobuf",
    ],
)

go_module(
    name = "bytestream",
    module = "google.golang.org/genproto",
    install = ["googleapis/bytestream"],
    download = ":genproto_download",
    deps = [
        ":annotations",
        ":grpc",
        ":protobuf",
    ],
)

go_module(
    name = "iam",
    module = "google.golang.org/genproto",
    install = [
        "googleapis/iam/v1",
        "googleapis/iam/credentials/v1",
    ],
    download = ":genproto_download",
    deps = [
        ":annotations",
        ":grpc",
        ":protobuf",
    ],
)

go_module(
    name = "pubsub",
    module = "google.golang.org/genproto",
    install = [
        "googleapis/pubsub/v1",
        "protobuf/field_mask",
    ],
    download = ":genproto_download",
    deps = [
        ":annotations",
        ":grpc",
        ":protobuf",
    ],
)

go_module(
    name = "type_expr",
    module = "google.golang.org/genproto",
    install = ["googleapis/type/expr"],
    download = ":genproto_download",
    deps = [
        ":annotations",
        ":grpc",
        ":protobuf",
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
    module = "github.com/dgraph-io/ristretto",
    install = ["..."],
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
    version = "v1.1.0",
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
    deps = [
        ":colorable",
        ":isatty",
        ":unix",
    ],
    version = "v1.9.0",
)

go_module(
    name = "copyfile",
    module = "github.com/peterebden/go-copyfile",
    version = "bc0baf74909cb55565cf9536cb003f04da55372d",
)

go_module(
    name = "godirwalk",
    module = "github.com/karrick/godirwalk",
    version = "v1.7.0",
    strip = [
        "examples",
        "testdata",
        "vendor",
    ],
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
    module = "github.com/shirou/gopsutil",
    install = [
        "internal/common",
        "mem",
    ],
    version = "v2.17.09",
    deps = [":unix"],
)

go_module(
    name = "zstd",
    module = "github.com/klauspost/compress",
    install = [
        ".",
        "fse",
        "huff0",
        "snappy",
        "zstd",
        "zstd/...",
    ],
    version = "v1.11.0",
)

go_module(
    name = "go-grpc-compression",
    module = "github.com/mostynb/go-grpc-compression",
    install = ["zstd"],
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
    module = "cloud.google.com/go",
    install = ["compute/metadata"],
    download = ":cloud_download",
)

go_module(
    name = "google-cloud-go",
    module = "cloud.google.com/go",
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
    deps = [
        ":api",
        ":errgroup",
        ":iam",
        ":gax-go",
        ":go.opencensus.io",
    ],
)

go_module(
    name = "cloud_pubsub",
    module = "cloud.google.com/go/pubsub",
    version = "v1.10.0",
    install = [
        ".",
        "apiv1",
    ],
    deps = [
        ":api",
        ":google-cloud-go",
        ":iam",
        ":gax-go",
        ":go.opencensus.io",
        ":pubsub",
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
        ":go.opencensus.io",
        ":google-cloud-go",
        ":type_expr",
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
    module = "go.opencensus.io",
    install = ["..."],
    version = "v0.22.1",
    deps = [
        ":groupcache",
        ":grpc",
        ":net",
    ],
)

go_module(
    name = "groupcache",
    module = "github.com/golang/groupcache",
    install = [
        "lru",
    ],
    version = "404acd9df4cc9859d64fb9eed42e5c026187287a",
)

go_module(
    name = "api",
    module = "google.golang.org/api",
    install = [
        "calendar/v3",
        "googleapi",
        "googleapi/transport",
        "idtoken",
        "internal",
        "internal/gensupport",
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
        "dataflow/...",
        "compute/v1",
        "container/v1",
        "drive/v3",
    ],
    version = "v0.40.0",
    deps = [
        ":gax-go",
        ":go.opencensus.io",
        ":bytestream",
        ":errgroup",
        ":grpc",
        ":oauth2",
    ],
)
