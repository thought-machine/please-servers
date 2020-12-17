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
    patch = "batch_size.patch",
    deps = [
        ":google-cloud-go",
        ":gax-go",
        ":go.opencensus.io",
        "//third_party/go:grpc",
        "//third_party/go:oauth2",
        ":wire",
        ":x_errors",
    ],
    visibility = ["PUBLIC"],
)

go_get(
    name = "google-cloud-go",
    get = "cloud.google.com/go",
    repo = "github.com/googleapis/google-cloud-go",
    revision = "v0.46.3",
    install = [
        "iam",
        "internal",
        "internal/optional",
        "internal/trace",
        "internal/version",
        "pubsub",
        "pubsub/apiv1",
        "storage",
    ],
    deps = [
        ":genproto",
        ":api",
        ":gax-go",
        ":go.opencensus.io",
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
        "transport/cert",
        "transport/grpc",
        "transport/http",
        "transport/http/internal/propagation",
        "dataflow/...",
        "compute/v1",
        "container/v1",
        "drive/v3",
    ],
    repo = "https://github.com/googleapis/google-api-go-client",
    revision = "v0.27.0",
    deps = [
        ":gax-go",
        ":go.opencensus.io",
        ":semaphore",
        "//third_party/go:grpc",
        "//third_party/go:oauth2",
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
    get = "go.opencensus.io/...",
    revision = "v0.22.1",
    deps = [
        ":groupcache",
        "//third_party/go:grpc",
        "//third_party/go:net",
    ],
)

go_get(
    name = "groupcache",
    get = "github.com/golang/groupcache",
    install = [
        "lru",
    ],
    revision = "404acd9df4cc9859d64fb9eed42e5c026187287a",
)

go_get(
    name = "genproto",
    get = "google.golang.org/genproto/...",
    revision = "a023cd5227bd25fd1f5c5633743ff3eacc93d169",
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
