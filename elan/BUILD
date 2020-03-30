go_binary(
    name = "elan",
    srcs = ["main.go"],
    deps = [
        "//elan/rpc",
        "//third_party/go:cli-init",
        "//third_party/go:http-admin",
        "//third_party/go/gocloud",
    ],
)

sh_cmd(
    name = "run_local",
    cmd = "mkdir -p plz-out/elan && exec $(out_location :elan) -s file://\\\\$PWD/plz-out/elan --log_file plz-out/log/elan.log --admin_disabled --cache_port 9200 --cache_peer localhost:9200 --cache_self_ip=127.0.0.1",
    srcs = [":elan"],
)
