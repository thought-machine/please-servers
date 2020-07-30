# This is for local development only, runs a local version of bb-browser to help
# visualise some of the outputs.
remote_file(
    name = "bb_browser",
    url = "https://get.please.build/third_party/binary/bb-browser-${OS}_${ARCH}-20200730",
    hashes = [
        "c26928e8d53e6ed9cce4a74c9f10eb9b2dbaaeed7da988440e741c3ee024c9fc",
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
