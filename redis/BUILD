github_repo(
    name = "redis_repo",
    build_file = "redis.build",
    hashes = ["96e95f55711f1598bc0080eb6d5bc52ed9e50ba3fa66442393872605293063df"],
    repo = "redis/redis",
    revision = "6.2.3",
)

sh_cmd(
    name = "run_local",
    cmd = "mkdir -p plz-out/redis && exec $(out_location ///redis/redis_repo//:redis_server) redis/redis.conf",
    data = [
        "///redis/redis_repo//:redis_server",
    ],
)

go_library(
    name = "redis",
    srcs = glob(["*.go"]),
    visibility = ["PUBLIC"],
    deps = [
        "///third_party/go/github.com_go-redis_redis_v8//:v8",
        "///third_party/go/github.com_peterebden_go-cli-init_v4//flags",
        "///third_party/go/golang.org_x_time//rate",
    ],
)
