go_binary(
    name = "flair",
    srcs = ["main.go"],
    visibility = ["//package:all"],
    deps = [
        "///third_party/go/github.com_peterebden_go-cli-init_v4//flags",
        "///third_party/go/github.com_peterebden_go-cli-init_v4//logging",
        "///third_party/go/google.golang.org_grpc//:grpc",
        "//cli",
        "//flair/rpc",
        "//flair/trie",
        "//grpcutil",
    ],
)

sh_cmd(
    name = "run_local",
    srcs = [":flair"],
    cmd = "exec $(out_location :flair) --host 127.0.0.1 --port 7772 -g 0-7:127.0.0.1:7777 -g 8-f:127.0.0.1:7777 -e 0-f:127.0.0.1:7778 -a 0-f:127.0.0.1:7776 --admin_disabled --token_file grpcutil/token.txt",
)
