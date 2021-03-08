go_library(
    name = "api",
    srcs = ["api.go"],
    visibility = ["//mettle/..."],
    deps = [
        "//grpcutil",
        "//mettle/common",
        "//proto:mettle",
        "//third_party/go:cli-init",
        "//third_party/go:genproto",
        "//third_party/go:grpc",
        "//third_party/go:logging",
        "//third_party/go:protobuf",
        "//third_party/go:remote-apis",
        "//third_party/go/gocloud",
        "//third_party/go/prometheus",
    ],
)

go_test(
    name = "api_test",
    srcs = ["api_test.go"],
    deps = [
        ":api",
        "//grpcutil",
        "//proto:mettle",
        "//third_party/go:genproto",
        "//third_party/go:grpc",
        "//third_party/go:protobuf",
        "//third_party/go:remote-apis",
        "//third_party/go:testify",
        "//third_party/go/gocloud",
    ],
)
