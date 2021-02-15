go_binary(
    name = "lucidity",
    srcs = ["main.go"],
    deps = [
        "//cli",
        "//grpcutil",
        "//lucidity/rpc",
        "//third_party/go:cli-init",
    ],
)

sh_cmd(
    name = "run_local",
    srcs = [":lucidity"],
    cmd = "exec $(out_location :lucidity) --host 127.0.0.1 --port 7774 --http_port 7773 --admin_disabled -v info",
)
