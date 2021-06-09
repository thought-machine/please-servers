# This is for local development only, runs a local version of bb-browser to help
# visualise some of the outputs.
remote_file(
    name = "bb_browser",
    out = "bb-browser",
    binary = True,
    hashes = [
        "f70c6e03abec5bc453d9adddc668d5da2c54f61e5d6c6da8d2816474e1cfd16e",
        "a639f32046e3d66edbbbfa57bc0120770f81cee9c692a106f3a9a454efebd932",
    ],
    url = "https://github.com/thought-machine/please-servers/releases/download/v9.2.1/bb_browser_${OS}_${ARCH}",
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
