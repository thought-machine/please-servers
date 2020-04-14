go_library(
    name = "trie",
    srcs = ["trie.go"],
    visibility = ["//sanguine/..."],
    deps = [
        "//proto:record",
        "//third_party/go:bytestream",
        "//third_party/go:grpc",
        "//third_party/go:remote-apis",
    ],
)

go_test(
    name = "trie_test",
    srcs = ["trie_test.go"],
    deps = [
        ":trie",
        "//third_party/go:testify",
    ],
)
