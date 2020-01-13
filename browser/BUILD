# This is for local development only, runs a local version of bb-browser to help
# visualise some of the outputs.
remote_file(
    name = "bb_browser",
    url = "https://get.please.build/third_party/binary/bb-browser-20200102",
    hashes = ["0ff56d6bd4449d0b0e25b1a22032817f7ac9c3b9"],
    out = "bb-browser",
    binary = True,
)

remote_file(
    name = "templates",
    url = "https://get.please.build/third_party/binary/bb-browser-templates-20200102.tar.gz",
    hashes = ["1dc3bae9d39fa2a9f59914106fc9d104e1a83e07"],
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
    deps = [":extract_templates", ":config"],
)
