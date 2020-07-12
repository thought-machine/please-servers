go_library(
    name = "gzfile",
    srcs = glob(["*.go"], exclude=["*_test.go"]),
    deps = [
        "//grpcutil",
        "//third_party/go:xattr",
        "//third_party/go/gocloud",
    ],
    visibility = ["//elan/..."],
)

go_test(
    name = "gzfile_test",
    srcs = glob(["*_test.go"]),
    deps = [
        ":gzfile",
        "//third_party/go/gocloud",
        "//third_party/go:testify",
    ],
)
