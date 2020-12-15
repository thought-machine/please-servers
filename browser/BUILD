# This is for local development only, runs a local version of bb-browser to help
# visualise some of the outputs.
remote_file(
    name = "bb_browser",
    url = "https://github.com/thought-machine/please-servers/releases/download/v7.6.1/bb_browser_${OS}_${ARCH}",
    hashes = [
        "35691a41fe5cb8d134ed7f0728d9696f492c346ef133cdb718397937c1d667af",
        "317d967ebfddfd6eb6bc3baa0519ec4e9580d604a4edb3d759ebae81b90adb2d",
    ],
    out = "bb-browser",
    binary = True,
)

filegroup(
    name = "config",
    srcs = ["config.jsonnet"],
    binary = True,
)

sh_cmd(
    name = "run_local",
    cmd = "cd plz-out/bin/browser && exec ./bb-browser config.jsonnet",
    srcs = [":bb_browser"],
    deps = [
        ":config",
    ],
)
