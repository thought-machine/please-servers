go_library(
    name = "rexclient",
    srcs = [
        "client.go",
        "packs.go",
    ],
    visibility = ["PUBLIC"],
    deps = [
        "//grpcutil",
        "//third_party/go:cli-init",
        "//third_party/go:remote-apis",
        "//third_party/go:remote-apis-sdks",
    ],
)

go_test(
    name = "packs_test",
    srcs = ["packs_test.go"],
    deps = [
        ":rexclient",
        "//third_party/go:remote-apis",
        "//third_party/go:remote-apis-sdks",
        "//third_party/go:testify",    ],
)
