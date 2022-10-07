sh_cmd(
    name = "generate",
    cmd = "cp $(out_location //proto/lucidity:_lucidity#protoc_go) proto/lucidity && cp $(out_location //proto/purity:_purity#protoc_go) proto/purity && cp $(out_location //proto/mettle:_mettle#protoc_go) proto/mettle",
    data = [
        "//proto/lucidity:_lucidity#protoc_go",
        "//proto/purity:_purity#protoc_go",
        "//proto/mettle:_mettle#protoc_go",
    ],
    labels = ["codegen"],
)
