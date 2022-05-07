go_binary(
    name = "zeal",
    srcs = ["main.go"],
    deps = [
        "//cli",
        "//grpcutil",
        "//zeal/rpc",
    ],
    visibility = ["//package:all"],
)

sh_cmd(
    name = "run_local",
    srcs = [":zeal"],
    cmd = "exec $(out_location :zeal) --host 127.0.0.1 --port 7776 -s 127.0.0.1:7777 -v 4 --log_file plz-out/log/zeal.log --admin_disabled --token_file grpcutil/token.txt",
)
