go_binary(
    name = "purity",
    srcs = ["main.go"],
    deps = [
        "//purity/gc",
        "//grpcutil",
        "//third_party/go:cli-init",
        "//third_party/go:http-admin",
        "//third_party/go/gocloud",
    ],
)

sh_cmd(
    name = "run_local",
    cmd = "exec $(out_location :purity) --host 127.0.0.1 --log_file plz-out/log/purity.log --admin_disabled --storage 127.0.0.1:7777 --token_file grpcutil/token.txt",
    srcs = [":purity"],
)
