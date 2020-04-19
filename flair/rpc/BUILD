go_library(
    name = "rpc",
    srcs = glob(["*.go"]),
    deps = [
        "//creds",
        "//proto:record",
        "//flair/trie",
        "//third_party/go:bytestream",
        "//third_party/go:cli-init",
        "//third_party/go:errgroup",
        "//third_party/go:go-multierror",
        "//third_party/go:grpc",
        "//third_party/go:grpc-middleware",
        "//third_party/go:protobuf",
        "//third_party/go:remote-apis",
        "//third_party/go:sri",
    ],
    visibility = ["//flair/..."],
)
