package(default_visibility = ["PUBLIC"])

go_get(
    name = "logging",
    get = "gopkg.in/op/go-logging.v1",
    revision = "b2cb9fa56473e98db8caba80237377e83fe44db5",
)

go_get(
    name = "terminal",
    get = "golang.org/x/crypto/ssh/terminal",
    revision = "7b85b097bf7527677d54d3220065e966a0e3b613",
)

go_get(
    name = "go-bindata",
    binary = True,
    get = "github.com/kevinburke/go-bindata/...",
    revision = "46eb4c183bfc1ebb527d9d19bcded39476302eb8",
    strip = ["testdata"],
)

go_get(
    name = "go-flags",
    get = "github.com/jessevdk/go-flags",
    revision = "v1.4.0",
)

go_get(
    name = "net",
    get = "golang.org/x/net/...",
    revision = "136a25c244d3019482a795d728110278d6ba09a4",
    deps = [
        ":terminal",
        ":text",
    ],
)

go_get(
    name = "tools",
    get = "golang.org/x/tools",
    install = [
        "cover",
    ],
    revision = "2ae76fd1560b622911f444c1e66b70a857e1de67",
)

go_get(
    name = "text",
    get = "golang.org/x/text/...",
    revision = "4e4a3210bb54bb31f6ab2cdca2edcc0b50c420c1",
    deps = [
        ":tools",
    ],
)

go_get(
    name = "grpc",
    exported_deps = [":net"],
    get = "google.golang.org/grpc",
    install = [
        "",
        "balancer",
        "balancer/base",
        "balancer/roundrobin",
        "binarylog/...",
        "codes",
        "connectivity",
        "credentials",
        "credentials/internal",
        "credentials/oauth",
        "encoding",
        "encoding/gzip",
        "encoding/proto",
        "grpclog",
        "health",
        "health/grpc_health_v1",
        "internal/...",
        "keepalive",
        "metadata",
        "naming",
        "peer",
        "resolver",
        "resolver/dns",
        "resolver/passthrough",
        "serviceconfig",
        "stats",
        "status",
        "tap",
    ],
    repo = "github.com/grpc/grpc-go",
    revision = "v1.22.0",
    deps = [
        ":oauth2",
        ":protobuf",
        ":rpcstatus",
        ":unix",
    ],
)

#TODO: build from the actual proto.
go_get(
    name = "rpcstatus",
    get = "google.golang.org/genproto/googleapis/rpc/status",
    revision = "2b5a72b8730b0b16380010cfe5286c42108d88e7",
    deps = [":protobuf"],
)

go_get(
    name = "protobuf",
    get = "github.com/golang/protobuf/...",
    revision = "v1.3.2",
    strip = [
        "proto/proto3_proto",
        "conformance",
    ],
)

go_get(
    name = "protoc-gen-go",
    binary = True,
    get = [],
    install = ["github.com/golang/protobuf/protoc-gen-go"],
    deps = [
        ":protobuf",
    ],
)

go_get(
    name = "spew",
    get = "github.com/davecgh/go-spew/spew",
    revision = "ecdeabc65495df2dec95d7c4a4c3e021903035e5",
)

go_get(
    name = "testify",
    get = "github.com/stretchr/testify",
    install = [
        "assert",
        "require",
        "vendor/github.com/davecgh/go-spew/spew",
        "vendor/github.com/pmezard/go-difflib/difflib",
    ],
    revision = "f390dcf405f7b83c997eac1b06768bb9f44dec18",
    deps = [":spew"],
)

go_get(
    name = "grpc-middleware",
    get = "github.com/grpc-ecosystem/go-grpc-middleware",
    install = [
        "",
        "recovery",
        "retry",
        "util/metautils",
        "util/backoffutils",
    ],
    revision = "fa8fef87dcecac0bda02d36abb3c790ab9e0030b",
    deps = [
        ":grpc",
        ":net",
        ":protobuf",
    ],
)

go_get(
    name = "errgroup",
    get = "golang.org/x/sync/errgroup",
    revision = "457c5828408160d6a47e17645169cf8fa20218c4",
    deps = [":net"],
)

go_get(
    name = "unix",
    get = "golang.org/x/sys/unix",
    revision = "1b2967e3c290b7c545b3db0deeda16e9be4f98a2",
)

go_get(
    name = "errors",
    get = "github.com/pkg/errors",
    revision = "30136e27e2ac8d167177e8a583aa4c3fea5be833",
)

go_get(
    name = "skylark",
    get = "github.com/google/skylark/syntax",
    revision = "a5f7082aabed29c0e429c722292c66ec8ecf9591",
)

go_get(
    name = "buildtools",
    get = "github.com/bazelbuild/buildtools/...",
    revision = "0.19.2",
    deps = [
        ":protobuf",
        ":skylark",
    ],
)

go_get(
    name = "buildifier",
    binary = True,
    get = [],
    install = ["github.com/bazelbuild/buildtools/buildifier"],
    deps = [
        ":buildtools",
    ],
)

go_get(
    name = "cli-init",
    get = "github.com/peterebden/go-cli-init",
    revision = "v1.1.1",
    deps = [
        ":go-flags",
        ":logging",
        ":terminal",
    ],
)

go_get(
    name = "remote-apis",
    get = "github.com/bazelbuild/remote-apis/build/...",
    revision = "b5123b1bb2853393c7b9aa43236db924d7e32d61",
    deps = [
        ":annotations",
        ":grpc",
        ":longrunning",
        ":protobuf",
        ":rpcstatus",
    ],
)

go_get(
    name = "remote-apis-sdks",
    get = "github.com/bazelbuild/remote-apis-sdks/go/...",
    repo = "github.com/peterebden/remote-apis-sdks",
    revision = "de86b298638b7ac24b09ac6bedfdee0c6384b5f6",
    deps = [
        ":annotations",
        ":bytestream",
        ":cmp",
        ":errgroup",
        ":errors",
        ":glog",
        ":grpc",
        ":longrunning",
        ":oauth2",
        ":pborman_uuid",
        ":protobuf",
        ":remote-apis",
        ":rpcstatus",
    ],
)

go_get(
    name = "cmp",
    get = "github.com/google/go-cmp/cmp",
    revision = "b1c9c4891a6525d98001fea424c8926c6d77bb56",
)

go_get(
    name = "glog",
    get = "github.com/golang/glog",
    patch = "glog_disable.patch",
    revision = "23def4e6c14b4da8ac2ed8007337bc5eb5007998",
)

go_get(
    name = "cloud-metadata",
    get = "cloud.google.com/go/compute/metadata",
    revision = "v0.40.0",
)

go_get(
    name = "oauth2",
    get = "golang.org/x/oauth2",
    install = [
        "",
        "google",
        "internal",
        "jws",
        "jwt",
    ],
    revision = "d668ce993890a79bda886613ee587a69dd5da7a6",
    visibility = ["//third_party/go/..."],
    deps = [
        ":cloud-metadata",
        "//third_party/go:net",
    ],
)

# TODO(peterebden): Splitting up this & the other bits of genproto is a bit of
#                   a hack. Replace once we have a better way of describing Go modules.
go_get(
    name = "annotations",
    get = "google.golang.org/genproto/googleapis/api/annotations",
    revision = "3bdd9d9f5532d75d09efb230bd767d265245cfe5",
    deps = [
        ":grpc",
        ":protobuf",
    ],
)

go_get(
    name = "longrunning",
    get = "google.golang.org/genproto/googleapis/longrunning",
    revision = "3bdd9d9f5532d75d09efb230bd767d265245cfe5",
    deps = [
        ":annotations",
        ":grpc",
        ":protobuf",
    ],
)

go_get(
    name = "bytestream",
    get = "google.golang.org/genproto/googleapis/bytestream",
    revision = "3bdd9d9f5532d75d09efb230bd767d265245cfe5",
    deps = [
        ":annotations",
        ":grpc",
        ":protobuf",
    ],
)

go_get(
    name = "uuid",
    get = "github.com/google/uuid",
    revision = "v1.1.1",
)

go_get(
    name = "pborman_uuid",
    get = "github.com/pborman/uuid",
    revision = "v1.2.0",
    deps = [":uuid"],
)
