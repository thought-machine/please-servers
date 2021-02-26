# This is for local development only, runs a local version of bb-browser to help
# visualise some of the outputs.
remote_file(
    name = "bb_browser",
    out = "bb-browser",
    binary = True,
    hashes = [
        "35691a41fe5cb8d134ed7f0728d9696f492c346ef133cdb718397937c1d667af",
        "ae9a13454da895d4ff5b8137afe39050b8af6b67fc3ec364636312dae000b50a",
    ],
    url = "https://github.com/thought-machine/please-servers/releases/download/v7.6.1/bb_browser_${OS}_${ARCH}",
)

filegroup(
    name = "config",
    srcs = ["config.jsonnet"],
    binary = True,
)

sh_cmd(
    name = "run_local",
    srcs = [":bb_browser"],
    cmd = "cd plz-out/bin/browser && exec ./bb-browser config.jsonnet",
    deps = [
        ":config",
    ],
)
