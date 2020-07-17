go_library(
    name = "cli",
    srcs = ["cli.go"],
    deps = [
        "//third_party/go:cli-init",
        "//third_party/go:remote-apis",
    ],
    visibility = ["PUBLIC"],
)

go_test(
    name = "cli_test",
    srcs = ["cli_test.go"],
    deps = [
        ":cli",
        "//third_party/go:testify",
    ],
)
