package(default_visibility = ["PUBLIC"])

go_module(
    name = "gocloud",
    module = "gocloud.dev",
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
    please_go_install = True,
    download = ":gocloud_download",
    visibility = ["PUBLIC"],
    deps = [
        ":gax-go",
        ":go.opencensus.io",
        ":google-cloud-go",
        ":wire",
        ":x_errors",
        "//third_party/go:grpc",
        "//third_party/go:oauth2",
    ],
)

go_mod_download(
    name = "gocloud_download",
    module = "github.com/peterebden/go-cloud",
    version = "6e9f62655df4f037f0007f5d4f1bd4efbb869d96",
)

go_module(
    name = "google-cloud-go",
    module = "cloud.google.com/go",
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
    version = "v0.46.3",
    deps = [
        ":api",
        ":gax-go",
        ":genproto",
        ":go.opencensus.io",
        "//third_party/go:errgroup",
    ],
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
    version = "v0.27.0",
    deps = [
        ":gax-go",
        ":go.opencensus.io",
        "//third_party/go:bytestream",
        "//third_party/go:errgroup",
        "//third_party/go:grpc",
        "//third_party/go:oauth2",
    ],
)

go_module(
    name = "gax-go",
    module = "github.com/googleapis/gax-go",
    install = [
        "",
        "v2",
    ],
    version = "4f559a1e5fd0a1cac6a74c886f1c87917e23ac94",
    deps = [
        "//third_party/go:grpc",
    ],
)

go_module(
    name = "go.opencensus.io",
    module = "go.opencensus.io",
    install = ["..."],
    version = "v0.22.1",
    deps = [
        ":groupcache",
        "//third_party/go:grpc",
        "//third_party/go:net",
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
    name = "genproto",
    module = "google.golang.org/genproto/...",
    version = "a023cd5227bd25fd1f5c5633743ff3eacc93d169",
    deps = [
        "//third_party/go:grpc",
        "//third_party/go:net",
        "//third_party/go:protobuf",
    ],
)

go_module(
    name = "wire",
    module = "github.com/google/wire",
    version = "v0.3.0",
)

go_module(
    name = "x_errors",
    module = "golang.org/x/xerrors",
    install = ["..."],
    version = "a5947ffaace3e882f334c1750858b4a6a7e52422",
    deps = ["//third_party/go:unix"],
)
