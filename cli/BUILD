go_library(
    name = "cli",
    srcs = ["cli.go"],
    visibility = ["PUBLIC"],
    deps = [
        "//third_party/go:automaxprocs",
        "//third_party/go:cli-init",
        "//third_party/go:http-admin",
        "//third_party/go:remote-apis",
    ],
)

go_test(
    name = "cli_test",
    srcs = ["cli_test.go"],
    deps = [
        ":cli",
        "//third_party/go:testify",
    ],
)
