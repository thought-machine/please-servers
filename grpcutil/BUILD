go_library(
    name = "grpcutil",
    srcs = glob(["*.go"]),
    deps = [
        "//third_party/go:cli-init",
        "//third_party/go:grpc",
        "//third_party/go:grpc-middleware",
        "//third_party/go/prometheus:grpc-prometheus",
        "//third_party/go:logging",
        "//third_party/go:go-grpc-compression",
    ],
    visibility = ["PUBLIC"],
)
