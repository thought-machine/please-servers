go_library(
    name = "rpc",
    srcs = [
        "auth.go",
        "rpc.go",
    ],
    resources = glob(["static/*.*"]),
    visibility = ["//lucidity/..."],
    deps = [
        "///third_party/go/github.com_golang_protobuf//jsonpb",
        "///third_party/go/github.com_peterebden_go-cli-init_v4//logging",
        "///third_party/go/github.com_prometheus_client_golang//prometheus",
        "///third_party/go/google.golang.org_api//idtoken",
        "//grpcutil",
        "//proto/lucidity",
    ],
)

go_test(
    name = "rpc_test",
    srcs = ["rpc_test.go"],
    resources = glob(["static/*.*"]),
    deps = [
        ":rpc",
        "//proto/lucidity",
        "//third_party/go:testify",
    ],
)
