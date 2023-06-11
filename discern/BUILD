go_binary(
    name = "discern",
    srcs = ["main.go"],
    visibility = ["//package:all"],
    deps = [
        "///third_party/go/github.com_bazelbuild_remote-apis-sdks//go/pkg/client",
        "///third_party/go/github.com_bazelbuild_remote-apis-sdks//go/pkg/digest",
        "///third_party/go/github.com_bazelbuild_remote-apis//build/bazel/remote/execution/v2",
        "///third_party/go/github.com_dustin_go-humanize//:go-humanize",
        "///third_party/go/github.com_peterebden_go-cli-init_v4//logging",
        "//cli",
        "//purity/gc",
        "//rexclient",
        "//third_party/go:golang-protobuf",
    ],
)
