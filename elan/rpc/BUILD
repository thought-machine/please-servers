go_library(
    name = "rpc",
    srcs = glob(
        ["*.go"],
        exclude = ["*_test.go"],
    ),
    visibility = [
        "//elan/...",
        "//mettle/worker/...",
    ],
    deps = [
        "//grpcutil",
        "//proto/purity",
        "//rexclient",
        "//third_party/go:api",
        "//third_party/go:cli-init",
        "//third_party/go:errgroup",
        "//third_party/go:genproto",
        "//third_party/go:go-multierror",
        "//third_party/go:grpc",
        "//third_party/go:protobuf",
        "//third_party/go:remote-apis",
        "//third_party/go:remote-apis-sdks",
        "//third_party/go:ristretto",
        "//third_party/go:rpc",
        "//third_party/go:zstd",
        "//third_party/go/gocloud",
        "//third_party/go/prometheus",
    ],
)

go_test(
    name = "compression_test",
    srcs = ["compression_test.go"],
    data = [":test_data"],
    deps = [
        ":rpc",
        "//grpcutil",
        "//third_party/go:genproto",
        "//third_party/go:grpc",
        "//third_party/go:remote-apis",
        "//third_party/go:testify",
        "//third_party/go:zstd",
    ],
)

genrule(
    name = "test_data",
    cmd = [
        'for i in `seq 1 10000`; do echo "$i ----------------------------------------- $i" >> "tmp.txt"; done',
        'HASH="`sha256sum tmp.txt | cut -c -64`"',
        'PREFIX="${HASH:0:2}"',
        'mkdir -p "data/cas/${PREFIX}" "data/zstd_cas/${PREFIX}"',
        'cat tmp.txt | "$TOOL" > "data/zstd_cas/${PREFIX}/${HASH}"',
        'mv tmp.txt "data/cas/${PREFIX}/${HASH}"',
    ],
    output_dirs = ["data"],
    test_only = True,
    tools = ["//scripts:compress"],
)
