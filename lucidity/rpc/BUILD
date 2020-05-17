subinclude("//build_defs:go_bindata")

go_bindata(
    name = "static_files",
    srcs = glob(["static/*.*"]),
    prefix = "$PKG/static",
)

go_library(
    name = "rpc",
    srcs = [
        "rpc.go",
        ":static_files",
    ],
    deps = [
        "//grpcutil",
        "//proto:lucidity",
        "//third_party/go:cli-init",
        "//third_party/go:protobuf",
        "//third_party/go/prometheus",
    ],
    visibility = ["//lucidity/..."],
)
