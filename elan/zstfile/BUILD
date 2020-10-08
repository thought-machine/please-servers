go_library(
    name = "zstfile",
    srcs = glob(
        ["*.go"],
        exclude = ["*_test.go"],
    ),
    visibility = ["//elan/..."],
    deps = [
        "//grpcutil",
        "//third_party/go:cli-init",
        "//third_party/go:xattr",
        "//third_party/go:zstd",
        "//third_party/go/gocloud",
        "//third_party/go/prometheus",
    ],
)

go_test(
    name = "zstfile_test",
    srcs = glob(["*_test.go"]),
    deps = [
        ":zstfile",
        "//third_party/go:testify",
        "//third_party/go:zstd",
        "//third_party/go/gocloud",
    ],
)
