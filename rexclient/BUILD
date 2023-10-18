go_library(
    name = "rexclient",
    srcs = glob(
        ["*.go"],
        exclude = ["*_test.go"],
    ),
    visibility = ["PUBLIC"],
    deps = [
        "///third_party/go/github.com_bazelbuild_remote-apis-sdks//go/pkg/client",
        "///third_party/go/github.com_bazelbuild_remote-apis-sdks//go/pkg/digest",
        "///third_party/go/github.com_bazelbuild_remote-apis-sdks//go/pkg/uploadinfo",
        "///third_party/go/github.com_bazelbuild_remote-apis//build/bazel/remote/execution/v2",
        "///third_party/go/github.com_peterebden_go-cli-init_v4//logging",
        "///third_party/go/google.golang.org_grpc//metadata",
        "//grpcutil",
    ],
)

go_test(
    name = "packs_test",
    srcs = ["packs_test.go"],
    deps = [
        ":rexclient",
        "///third_party/go/github.com_bazelbuild_remote-apis-sdks//go/pkg/digest",
        "///third_party/go/github.com_bazelbuild_remote-apis//build/bazel/remote/execution/v2",
    ],
)
