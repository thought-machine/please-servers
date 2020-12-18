go_library(
    name = "rpc",
    visibility = ["//elan/..."],
    srcs = glob(
        ["*.go"],
        exclude = ["*_test.go"],
    ),
    deps = [
        "//grpcutil",
        "//proto:purity",
        "//third_party/go:bytestream",
        "//third_party/go:cli-init",
        "//third_party/go:go-multierror",
        "//third_party/go:grpc",
        "//third_party/go:longrunning",
        "//third_party/go:protobuf",
        "//third_party/go:remote-apis",
        "//third_party/go:remote-apis-sdks",
        "//third_party/go:ristretto",
        "//third_party/go:rpcstatus",
        "//third_party/go/gocloud",
        "//third_party/go/gocloud:api",
        "//third_party/go/prometheus",
        "//third_party/go:zstd",
    ],
)

go_test(
    name = "storage_test",
    srcs = ["storage_test.go"],
    deps = [
        ":rpc",
        "//third_party/go:testify",
    ],
)
