package(default_visibility = ["PUBLIC"])

go_module(
    name = "gocloud",
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
        "internal/escape",
        "internal/gcerr",
        "internal/oc",
        "internal/openurl",
        "internal/retry",
        "internal/useragent",
        "pubsub",
        "pubsub/batcher",
        "pubsub/driver",
        "pubsub/gcppubsub",
        "pubsub/mempubsub",
    ],
    module = "gocloud.dev",
    version = "v0.26.0",
    visibility = ["PUBLIC"],
    deps = [
        ":wire",
        ":x_errors",
        "//third_party/go:cloud-storage",
        "//third_party/go:cloud_pubsub",
        "//third_party/go:gax-go",
        "//third_party/go:go.opencensus.io",
        "//third_party/go:google-cloud-go",
        "//third_party/go:grpc",
        "//third_party/go:oauth2",
    ],
)

go_module(
    name = "wire",
    module = "github.com/google/wire",
    version = "v0.3.0",
)

go_module(
    name = "x_errors",
    install = [
        ".",
        "...",
    ],
    module = "golang.org/x/xerrors",
    version = "a5947ffaace3e882f334c1750858b4a6a7e52422",
    deps = ["//third_party/go:unix"],
)
