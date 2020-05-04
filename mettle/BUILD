go_binary(
    name = "mettle",
    srcs = ["main.go"],
    deps = [
        "//grpcutil",
        "//mettle/worker",
        "//mettle/api",
        "//mettle/common",
        "//third_party/go:cli-init",
        "//third_party/go:http-admin",
    ],
)

sh_cmd(
    name = "run_local",
    cmd = "exec $(out_location :mettle) dual --host 127.0.0.1 --port 7778 -s 127.0.0.1:7777 -d plz-out/mettle -v 4 --log_file plz-out/log/mettle.log --browser http://127.0.0.1:7779 --sandbox ~/.please/please_sandbox --admin_host 127.0.0.1 --token_file grpcutil/token.txt",
    srcs = [":mettle"],
)
