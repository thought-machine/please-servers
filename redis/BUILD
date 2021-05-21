github_repo(
    name = "redis",
    build_file = "redis.build",
    hashes = ["96e95f55711f1598bc0080eb6d5bc52ed9e50ba3fa66442393872605293063df"],
    repo = "redis/redis",
    revision = "6.2.3",
)

sh_cmd(
    name = "run_local",
    cmd = "mkdir -p plz-out/redis && exec $(out_location ///redis/redis//:redis_server) redis/redis.conf",
    data = [
        "///redis/redis//:redis_server",
    ],
)
