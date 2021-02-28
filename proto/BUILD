grpc_library(
    name = "lucidity",
    srcs = ["lucidity.proto"],
    visibility = ["PUBLIC"],
)

grpc_library(
    name = "purity",
    srcs = ["purity.proto"],
    visibility = ["PUBLIC"],
)

sh_cmd(
    name = "generate",
    cmd = "cp $(out_location :_lucidity#go) proto/lucidity.pb.go && cp $(out_location :_purity#go) proto/purity.pb.go",
    data = [
        ":_lucidity#go",
        ":_purity#go",
    ],
    labels = ["codegen"],
)
