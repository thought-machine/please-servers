package(default_visibility = ["PUBLIC"])

# This is a vendorised copy of Buildbarn's uncached action result extension.
go_library(
    name = "cas",
    srcs = ["cas.pb.go"],
    deps = [
        "//third_party/go:protobuf",
        "//third_party/go:remote-apis",
    ],
)

# Similarly this is of Buildbarn's resource usage proto for display in the browser.
proto_library(
    name = "resourceusage",
    srcs = ["resourceusage.proto"],
)

protoc_binary(
    name = "protoc",
    version = "3.11.4",
)
