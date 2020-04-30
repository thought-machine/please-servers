go_binary(
    name = "zeal",
    srcs = ["main.go"],
    deps = [
        "//grpcutil",
        "//third_party/go:cli-init",
        "//third_party/go:http-admin",
        "//zeal/rpc",
    ],
)

sh_cmd(
    name = "run_local",
    cmd = "exec $(out_location :zeal) --host 127.0.0.1 --port 7776 -s 127.0.0.1:7777 -v 4 --log_file plz-out/log/zeal.log --admin_disabled",
    srcs = [":zeal"],
)
