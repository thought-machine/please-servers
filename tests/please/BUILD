github_repo(
    name = "please",
    config = "plz.config",
    repo = "thought-machine/please",
    revision = "master",
)

go_test(
    name = "core_test",
    srcs = ["core_test.go"],
    deps = [
        "///tests/please/please//src/core",
        "//third_party/go:testify",
    ],
)
