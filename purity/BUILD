go_binary(
    name = "purity",
    srcs = ["main.go"],
    visibility = ["//package:all"],
    deps = [
        "///third_party/go/github.com_peterebden_go-cli-init_v4//flags",
        "///third_party/go/github.com_peterebden_go-cli-init_v4//logging",
        "//cli",
        "//purity/gc",
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
