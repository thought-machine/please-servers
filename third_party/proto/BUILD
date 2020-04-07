# This is a vendorised copy of Buildbarn's uncached action result extension.
go_library(
    name = "cas",
    srcs = ["cas.pb.go"],
    deps = [
        "//third_party/go:protobuf",
        "//third_party/go:remote-apis",
    ],
    visibility = ["PUBLIC"],
)

protoc_binary(
    name = "protoc",
    version = "3.11.4",
    visibility = ["PUBLIC"],
)
