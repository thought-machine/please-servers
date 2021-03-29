# This is for local development only, runs a local version of bb-browser to help
# visualise some of the outputs.
remote_file(
    name = "bb_browser",
    out = "bb-browser",
    binary = True,
    hashes = [
        "4e7e3645a7ccc61562afd67b272affe09f700f91b6575c82ea3c270388f2b5e8",
        "a639f32046e3d66edbbbfa57bc0120770f81cee9c692a106f3a9a454efebd932",
    ],
    url = "https://github.com/thought-machine/please-servers/releases/download/v8.8.4/bb_browser_${OS}_${ARCH}",
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
