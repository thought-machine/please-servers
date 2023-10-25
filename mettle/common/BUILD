go_library(
    name = "common",
    srcs = ["common.go"],
    visibility = ["//mettle/..."],
    deps = [
        "///third_party/go/github.com_bazelbuild_remote-apis//build/bazel/remote/execution/v2",
        "///third_party/go/github.com_golang_protobuf//proto",
        "///third_party/go/github.com_golang_protobuf//ptypes",
        "///third_party/go/github.com_peterebden_go-cli-init_v4//logging",
        "///third_party/go/gocloud.dev//pubsub",
        "///third_party/go/gocloud.dev//pubsub/gcppubsub",
        "///third_party/go/google.golang.org_genproto//googleapis/longrunning",
        "///third_party/go/google.golang.org_genproto//googleapis/pubsub/v1",
        "///third_party/go/google.golang.org_grpc//codes",
        "///third_party/go/google.golang.org_grpc//status",
        "//mettle/mempubsub",
    ],
)

go_test(
    name = "common_test",
    srcs = ["common_test.go"],
    deps = [
        ":common",
        "///third_party/go/github.com_stretchr_testify//assert",
    ],
)
