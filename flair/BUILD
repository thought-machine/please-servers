go_binary(
    name = "flair",
    srcs = ["main.go"],
    deps = [
        "//flair/rpc",
        "//flair/trie",
        "//third_party/go:cli-init",
        "//third_party/go:http-admin",
    ],
)

sh_cmd(
    name = "run_local",
    cmd = "exec $(out_location :flair) -g 0-7:127.0.0.1:7777 -g 8-f:127.0.0.1:7777 --admin_disabled",
    srcs = [":flair"],
)
