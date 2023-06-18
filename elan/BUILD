go_binary(
    name = "elan",
    srcs = ["main.go"],
    visibility = ["//package:all"],
    deps = [
        "///third_party/go/github.com_peterebden_go-cli-init_v4//flags",
        "//cli",
        "//elan/rpc",
        "//grpcutil",
    ],
)

sh_cmd(
    name = "run_local",
    srcs = [":elan"],
    cmd = f"mkdir -p plz-out/elan && exec $(out_location :elan) --host {CONFIG.LOCAL_HOST} --port 7777 -s file://\\\\$PWD/plz-out/elan --log_file plz-out/log/elan.log --admin_host 127.0.0.1 --admin_port 9992 --token_file grpcutil/token.txt --known_blob_cache_size 20M",
)
