go_library(
    name = "trie",
    srcs = [
        "replication.go",
        "trie.go",
    ],
    visibility = ["//flair/..."],
    deps = [
        "///third_party/go/github.com_bazelbuild_remote-apis//build/bazel/remote/asset/v1",
        "///third_party/go/github.com_bazelbuild_remote-apis//build/bazel/remote/execution/v2",
        "///third_party/go/github.com_hashicorp_go-multierror//:go-multierror",
        "///third_party/go/github.com_peterebden_go-cli-init_v4//logging",
        "///third_party/go/google.golang.org_genproto_googleapis_bytestream//:bytestream",
        "///third_party/go/google.golang.org_grpc//:grpc",
        "///third_party/go/google.golang.org_grpc//codes",
        "///third_party/go/google.golang.org_grpc//health/grpc_health_v1",
        "///third_party/go/google.golang.org_grpc//status",
        "//proto/purity",
    ],
)

go_test(
    name = "trie_test",
    srcs = glob(["*_test.go"]),
    deps = [
        ":trie",
        "///third_party/go/github.com_hashicorp_go-multierror//:go-multierror",
        "///third_party/go/github.com_stretchr_testify//assert",
        "///third_party/go/google.golang.org_grpc//:grpc",
        "///third_party/go/google.golang.org_grpc//codes",
        "///third_party/go/google.golang.org_grpc//status",
    ],
)
