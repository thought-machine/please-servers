go_binary(
    name = "mettle",
    srcs = ["main.go"],
    deps = [
        "//mettle/worker",
        "//mettle/api",
        "//mettle/common",
        "//third_party/go:cli-init",
        "//third_party/go:http-admin",
    ],
)

sh_cmd(
    name = "run_local",
    cmd = "exec $(out_location :mettle) dual -s 127.0.0.1:7777 -d plz-out/mettle -v 4 --log_file plz-out/log/mettle.log --browser http://127.0.0.1:7779 --sandbox ~/.please/please_sandbox",
    srcs = [":mettle"],
)
