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
    revision = "v0.46.3",
    deps = [
        ":cloud-iam",
        ":cloud-internal",
    ],
)

go_get(
    name = "cloud-iam",
    get = "cloud.google.com/go/iam",
    revision = "v0.46.3",
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
    revision = "v0.46.3",
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
    revision = "v0.46.3",
)

go_get(
    name = "cloud-pubsub",
    get = "cloud.google.com/go/pubsub",
    install = [
        "",
        "apiv1",
    ],
    revision = "v0.46.3",
    deps = [
        ":cloud-iam",
        ":cloud-internal",
        ":genproto",
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
        "calendar/v3",
        "googleapi",
        "googleapi/internal/uritemplates",
        "googleapi/transport",
        "internal",
        "internal/gensupport",
        "iterator",
        "option",
        "storage/v1",
        "sheets/v4",
        "support/bundler",
        "transport",
        "transport/grpc",
        "transport/http",
        "transport/http/internal/propagation",
        "dataflow/...",
        "compute/v1",
        "container/v1",
        "drive/v3",
    ],
    repo = "https://github.com/googleapis/google-api-go-client",
    revision = "v0.11.0",
    deps = [
        ":gax-go",
        ":go.opencensus.io",
        ":semaphore",
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
