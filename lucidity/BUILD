go_binary(
    name = "lucidity",
    srcs = ["main.go"],
    visibility = ["//package:all"],
    deps = [
        "///third_party/go/github.com_peterebden_go-cli-init_v4//flags",
        "//cli",
        "//grpcutil",
        "//lucidity/rpc",
    ],
)

sh_cmd(
    name = "run_local",
    srcs = [":lucidity"],
    cmd = "exec $(out_location :lucidity) --host 127.0.0.1 --port 7774 --http_port 7773 --admin_disabled -v info --min_proportion 0.5",
)
