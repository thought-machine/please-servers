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

remote_file(
    name = "templates",
    url = "https://get.please.build/third_party/binary/bb-browser-templates-20200730.tar.gz",
    hashes = ["6de808dfb93fbf25c5761062caf484ca845f59921d3258eb92f0eed7681883d8"],
)

genrule(
    name = "extract_templates",
    srcs = [":templates"],
    outs = ["templates"],
    cmd = "tar -xzf $SRCS",
    binary = True,  # Not really but bb-browser doesn't allow their location to be configured.
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
        ":extract_templates",
        ":config",
    ],
)
