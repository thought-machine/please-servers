go_binary(
    name = "sanguine",
    srcs = ["main.go"],
    deps = [
        "//sanguine/rpc",
        "//sanguine/trie",
        "//third_party/go:cli-init",
        "//third_party/go:http-admin",
    ],
)

sh_cmd(
    name = "run_local",
    cmd = "exec $(out_location :sanguine) -g 0-7:127.0.0.1:7777 -g 8-f:127.0.0.1:7777 --admin_disabled",
    srcs = [":sanguine"],
)
