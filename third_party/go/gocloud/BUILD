package(default_visibility = ["PUBLIC"])

go_get(
    name = "gocloud",
    get = "gocloud.dev",
    install = [
        # This is an awkward lot but necessary so we don't have to pull in transitive
        # dependencies for the providers for aws, azure etc.
        "blob",
        "blob/driver",
        "blob/fileblob",
        "blob/gcsblob",
        "blob/memblob",
        "gcerrors",
        "gcp",
        "internal/batcher",
        "internal/escape",
        "internal/gcerr",
        "internal/oc",
        "internal/openurl",
        "internal/retry",
        "internal/useragent",
        "pubsub",
        "pubsub/driver",
        "pubsub/gcppubsub",
        "pubsub/mempubsub",
    ],
    repo = "github.com/google/go-cloud",
    revision = "v0.17.0",
    deps = [
        ":cloud-pubsub",
        ":cloud-storage",
        ":gax-go",
        ":go.opencensus.io",
        "//third_party/go:grpc",
        ":oauth2",
        ":wire",
        ":x_errors",
    ],
    visibility = ["PUBLIC"],
)

go_get(
    name = "cloud-storage",
    get = "cloud.google.com/go/storage",
    revision = "v0.40.0",
    deps = [
        ":cloud-iam",
        ":cloud-internal",
    ],
)

go_get(
    name = "cloud-iam",
    get = "cloud.google.com/go/iam",
    revision = "v0.40.0",
    deps = [
        ":api",
        ":cloud-metadata",
        ":gax-go",
        "//third_party/go:grpc",
        ":genproto",
    ],
)

go_get(
    name = "cloud-internal",
    get = "cloud.google.com/go/internal",
    install = [
        "",
        "optional",
        "trace",
        "version",
    ],
    revision = "v0.40.0",
    deps = [
        ":genproto",
        ":api",
        ":gax-go",
        ":go.opencensus.io",
    ],
)

go_get(
    name = "cloud-metadata",
    get = "cloud.google.com/go/compute/metadata",
    revision = "v0.40.0",
)

go_get(
    name = "cloud-pubsub",
    get = "cloud.google.com/go/pubsub",
    install = [
        "",
        "apiv1",
    ],
    revision = "v0.40.0",
    deps = [
        ":cloud-iam",
        ":cloud-internal",
        ":semaphore",
        "//third_party/go:errgroup",
    ],
)

go_get(
    name = "semaphore",
    get = "golang.org/x/sync/semaphore",
    revision = "112230192c580c3556b8cee6403af37a4fc5f28c",
)

go_get(
    name = "api",
    get = "google.golang.org/api/...",
    install = [
        "gensupport",
        "googleapi",
        "googleapi/internal/uritemplates",
        "googleapi/transport",
        "internal",
        "iterator",
        "option",
        "storage/v1",
        "sheets/v4",
        "transport",
        "transport/grpc",
        "transport/http",
        "transport/http/internal/propagation",
        "dataflow/...",
    ],
    revision = "583d854617af4d2080b5d2a24d72f7fc5a128ab2",
    deps = [
        ":go.opencensus.io",
        "//third_party/go:grpc",
        ":oauth2",
    ],
)

go_get(
    name = "gax-go",
    get = "github.com/googleapis/gax-go",
    install = [
        "",
        "v2",
    ],
    revision = "4f559a1e5fd0a1cac6a74c886f1c87917e23ac94",
    deps = [
        "//third_party/go:grpc",
    ],
)

go_get(
    name = "go.opencensus.io",
    get = "go.opencensus.io",
    install = [
        "",
        "exemplar",
        "internal",
        "internal/tagencoding",
        "plugin/ocgrpc",
        "plugin/ochttp",
        "plugin/ochttp/propagation/b3",
        "stats",
        "stats/internal",
        "stats/view",
        "tag",
        "trace",
        "trace/internal",
        "trace/propagation",
        "trace/tracestate",
    ],
    revision = "8b95f1c9dfabe8d924e11cf1c30c71c19a7ac35c",
    deps = [
        "//third_party/go:grpc",
        "//third_party/go:net",
    ],
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
    deps = [
        ":cloud-metadata",
        "//third_party/go:net",
    ],
    visibility = ["//third_party/go/..."],
)

go_get(
    name = "genproto",
    get = "google.golang.org/genproto/...",
    revision = "bd9b4fb69e2ffd37621a6caa54dcbead29b546f2",
    deps = [
        "//third_party/go:protobuf",
        "//third_party/go:net",
        "//third_party/go:grpc",
    ],
)

go_get(
    name = "wire",
    get = "github.com/google/wire",
    revision = "v0.3.0",
)

go_get(
    name = "x_errors",
    get = "golang.org/x/xerrors/...",
    revision = "a5947ffaace3e882f334c1750858b4a6a7e52422",
    deps = ["//third_party/go:unix"],
)
