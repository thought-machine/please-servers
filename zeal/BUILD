go_binary(
    name = "zeal",
    srcs = ["main.go"],
    deps = [
        "//metrics",
        "//zeal/rpc",
        "//third_party/go:cli-init",
        "//third_party/go:logging",
    ],
)

sh_cmd(
    name = "run_local",
    cmd = "exec $(out_location :zeal) -s 127.0.0.1:7777 -v 4 --log_file plz-out/log/zeal.log",
    srcs = [":zeal"],
)
