go_library(
    name = "trie",
    srcs = [
        "replication.go",
        "trie.go",
    ],
    visibility = ["//flair/..."],
    deps = [
        "//proto:purity",
        "//third_party/go:bytestream",
        "//third_party/go:cli-init",
        "//third_party/go:go-multierror",
        "//third_party/go:grpc",
        "//third_party/go:remote-apis",
    ],
)

go_test(
    name = "trie_test",
    srcs = glob(["*_test.go"]),
    deps = [
        ":trie",
        "//third_party/go:grpc",
        "//third_party/go:testify",
    ],
)
