go_library(
    name = "common",
    srcs = ["common.go"],
    visibility = ["//mettle/..."],
    deps = [
        "//mettle/gcprpubsub",
        "//mettle/omempubsub",
        "//third_party/go:cli-init",
        "//third_party/go:genproto",
        "//third_party/go:grpc",
        "//third_party/go:remote-apis",
        "//third_party/go/gocloud",
    ],
)

go_test(
    name = "common_test",
    srcs = ["common_test.go"],
    deps = [
        ":common",
        "//third_party/go:testify",
    ],
)
