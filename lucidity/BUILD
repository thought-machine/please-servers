go_binary(
    name = "lucidity",
    srcs = ["main.go"],
    deps = [
        "//lucidity/rpc",
        "//grpcutil",
        "//third_party/go:cli-init",
        "//third_party/go:http-admin",
    ],
)

sh_cmd(
    name = "run_local",
    cmd = "exec $(out_location :lucidity) --host 127.0.0.1 --port 7774 --admin_disabled",
    srcs = [":lucidity"],
)
