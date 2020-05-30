go_binary(
    name = "flair",
    srcs = ["main.go"],
    deps = [
        "//flair/rpc",
        "//flair/trie",
        "//grpcutil",
        "//third_party/go:cli-init",
        "//third_party/go:grpc",
        "//third_party/go:http-admin",
    ],
)

sh_cmd(
    name = "run_local",
    cmd = "exec $(out_location :flair) --host 127.0.0.1 --port 7772 -g 0-7:127.0.0.1:7777 -g 8-f:127.0.0.1:7777 -e 0-f:127.0.0.1:7778 -a 0-f:127.0.0.1:7776 --admin_disabled --token_file grpcutil/token.txt",
    srcs = [":flair"],
)
