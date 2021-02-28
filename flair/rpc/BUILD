go_library(
    name = "rpc",
    srcs = glob(["*.go"]),
    visibility = ["//flair/..."],
    deps = [
        "//flair/trie",
        "//grpcutil",
        "//proto:purity",
        "//third_party/go:errgroup",
        "//third_party/go:genproto",
        "//third_party/go:go-multierror",
        "//third_party/go:grpc",
        "//third_party/go:protobuf",
        "//third_party/go:remote-apis",
        "//third_party/go:remote-apis-sdks",
        "//third_party/go:sri",
    ],
)
