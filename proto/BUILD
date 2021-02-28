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
    cmd = "cp $(out_location :_lucidity#protoc_go) proto/lucidity && cp $(out_location :_purity#protoc_go) proto/purity",
    data = [
        ":_lucidity#protoc_go",
        ":_purity#protoc_go",
    ],
    labels = ["codegen"],
)
