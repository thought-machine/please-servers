go_binary(
    name = "mettle",
    srcs = ["main.go"],
    deps = [
        "//metrics",
        "//mettle/worker",
        "//mettle/api",
        "//mettle/common",
        "//third_party/go:cli-init",
        "//third_party/go:logging",
    ],
)

sh_cmd(
    name = "run_local",
    cmd = "exec $(out_location :mettle) dual -s 127.0.0.1:7777 -q mem://requests -r mem://responses -m 13434 -d plz-out/mettle -v 4 --log_file plz-out/log/mettle.log",
    srcs = [":mettle"],
)
