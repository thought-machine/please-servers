subinclude("///python//build_defs:python")

python_binary(
    name = "gen_release",
    main = "gen_release.py",
    resources = ["//:version"],
    deps = [
        "//third_party/python:absl",
        "//third_party/python:colorlog",
        "//third_party/python:requests",
    ],
)

go_binary(
    name = "compress",
    srcs = ["compress.go"],
    test_only = True,
    visibility = ["PUBLIC"],
    deps = ["///third_party/go/github.com_klauspost_compress//zstd"],
)
