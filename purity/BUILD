go_binary(
    name = "purity",
    srcs = ["main.go"],
    deps = [
        "//cli",
        "//grpcutil",
        "//purity/gc",
        "//third_party/go:cli-init",
        "//third_party/go/gocloud",
    ],
)

sh_cmd(
    name = "run_local",
    srcs = [":purity"],
    cmd = "exec $(out_location :purity) one --url 127.0.0.1:7772 --log_file plz-out/log/purity.log --admin_disabled --token_file grpcutil/token.txt --min_age 15m",
)

sh_cmd(
    name = "dry_run_local",
    srcs = [":purity"],
    cmd = "exec $(out_location :purity) one --url 127.0.0.1:7772 --log_file plz-out/log/purity.log --admin_disabled --token_file grpcutil/token.txt --min_age 15m --dry_run",
)
