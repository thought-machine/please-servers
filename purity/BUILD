go_binary(
    name = "purity",
    srcs = ["main.go"],
    deps = [
        "//cli",
        "//purity/gc",
        "//grpcutil",
        "//third_party/go:cli-init",
        "//third_party/go:http-admin",
        "//third_party/go/gocloud",
    ],
)

sh_cmd(
    name = "run_local",
    cmd = "exec $(out_location :purity) one --url 127.0.0.1:7777 --log_file plz-out/log/purity.log --admin_disabled --token_file grpcutil/token.txt --min_age 15m",
    srcs = [":purity"],
)
