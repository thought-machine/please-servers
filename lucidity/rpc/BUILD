go_library(
    name = "rpc",
    srcs = [
        "auth.go",
        "rpc.go",
    ],
    resources = glob(["static/*.*"]),
    visibility = ["//lucidity/..."],
    deps = [
        "//grpcutil",
        "//proto/lucidity",
        "//third_party/go:api",
        "//third_party/go:cli-init",
        "//third_party/go:protobuf",
        "//third_party/go/prometheus",
    ],
)

go_test(
    name = "rpc_test",
    srcs = ["rpc_test.go"],
    resources = glob(["static/*.*"]),
    deps = [
        ":rpc",
        "//proto/lucidity",
        "//third_party/go:testify",
    ],
)
