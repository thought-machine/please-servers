go_binary(
    name = "mettle",
    srcs = ["main.go"],
    deps = [
        "//cli",
        "//grpcutil",
        "//mettle/api",
        "//mettle/common",
        "//mettle/worker",
        "//third_party/go:cli-init",
    ],
)

sh_cmd(
    name = "run_local",
    cmd = "mkdir -p plz-out/elan && exec $(out_location :mettle) dual --host 127.0.0.1 --port 7778 -s 127.0.0.1:7777 -s file://\\\\$PWD/plz-out/elan -s 127.0.0.1:7772 -d plz-out/mettle --log_file plz-out/log/mettle.log --browser http://127.0.0.1:7779 --sandbox $(out_location //sandbox:sandbox) --alt_sandbox $(out_location //sandbox:alt_sandbox) --admin_host 127.0.0.1 --token_file grpcutil/token.txt --redis_url 127.0.0.1:6379 --redis_password_file redis/auth --lucidity grpc://127.0.0.1:7774 --cache_dir plz-out/mettle-cache --cache_prefix third_party --memory_threshold 95.0 --version_file VERSION --cost cpu:£0.02 --cost mem:£0.01 --allowed_platform OSFamily:linux --allowed_platform OSFamily:macos --allowed_platform ISA:x86-64",
    data = [
        ":mettle",
        "//sandbox",
        "//sandbox:alt_sandbox",
    ],
)
