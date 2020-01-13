go_binary(
    name = "elan",
    srcs = ["main.go"],
    deps = [
        "//elan/rpc",
        "//metrics",
        "//third_party/go:cli-init",
        "//third_party/go:logging",
    ],
)

sh_cmd(
    name = "run_local",
    cmd = "mkdir -p plz-out/elan && exec $(out_location :elan) -s file://\\\\$PWD/plz-out/elan --log_file plz-out/log/elan.log",
    srcs = [":elan"],
)
