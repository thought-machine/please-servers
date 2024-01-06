go_library(
    name = "cli",
    srcs = ["cli.go"],
    visibility = ["PUBLIC"],
    deps = [
        "///third_party/go/github.com_bazelbuild_remote-apis//build/bazel/remote/execution/v2",
        "///third_party/go/github.com_peterebden_go-cli-init_v4//flags",
        "///third_party/go/github.com_peterebden_go-cli-init_v4//logging",
        "///third_party/go/github.com_thought-machine_http-admin//:http-admin",
        "///third_party/go/go.uber.org_automaxprocs//maxprocs",
        "///third_party/go/cloud.google.com_go_profiler//:profiler",
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
