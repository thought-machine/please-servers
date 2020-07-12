go_library(
    name = "gc",
    srcs = glob(["*.go"]),
    deps = [
        "//grpcutil",
        "//proto:purity",
        "//third_party/go:cli-init",
        "//third_party/go:go-multierror",
        "//third_party/go:grpc",
        "//third_party/go:grpc-middleware",
        "//third_party/go:remote-apis",
        "//third_party/go:remote-apis-sdks",
        "//third_party/go:terminal",
    ],
    visibility = [
        "//purity/...",
        "//discern/...",
    ],
)
