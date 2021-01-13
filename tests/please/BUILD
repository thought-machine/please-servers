github_repo(
    name = "please",
    repo = "thought-machine/please",
    revision = "master",
    config = "plz.config",
)

go_test(
    name = "core_test",
    srcs = ["core_test.go"],
    deps = [
        "///tests/please/please//src/core",
        "//third_party/go:testify",
    ],
)
