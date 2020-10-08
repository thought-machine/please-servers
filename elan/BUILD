go_binary(
    name = "elan",
    srcs = ["main.go"],
    deps = [
        "//elan/rpc",
        "//grpcutil",
        "//third_party/go:cli-init",
        "//third_party/go:http-admin",
        "//third_party/go/gocloud",
    ],
)

sh_cmd(
    name = "run_local",
    srcs = [":elan"],
    cmd = "exec $(out_location :elan) --host 127.0.0.1 --port 7777 -s zstfile://plz-out/elan --log_file plz-out/log/elan.log --admin_port 9992 --token_file grpcutil/token.txt",
)
