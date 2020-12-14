# This is for local development only, runs a local version of bb-browser to help
# visualise some of the outputs.
go_get(
    name = "bb_browser",
    get = "github.com/buildbarn/bb-browser/cmd/bb_browser",
    revision = "83d8601dd9c21310c772b64cb8da78bf1d39d6d8",
    binary = True,
    deps = [
        ":bb-remote-execution",
        "//third_party/go:protobuf",
        "//third_party/go:grpc",
        "//third_party/go:mux",
        "//third_party/go:remote-apis",
    ],
)

go_get(
    name = "bb-remote-execution",
    get = "github.com/buildbarn/bb-remote-execution",
    revision = "1bf59395c8ef3ae9574fdef2d6457cf01be22360",
    install = [
        "pkg/proto/configuration/bb_browser",
        "pkg/proto/resourceusage",
    ],
    deps = [
        ":bb-storage",
    ],
)

go_get(
    name = "bb-storage",
    get = "github.com/buildbarn/bb-storage",
    revision = "023d250792d9e21af77721fcbefc9e051b3316cf",
    install = [
        "pkg/blobstore",
        "pkg/digest",
        "pkg/global",
        "pkg/grpc",
        "pkg/proto/cas",
        "pkg/util",
    ],
    deps = [
        ":aws-sdk-go",
        "//third_party/go:protobuf",
        "//third_party/go:grpc",
    ],
)

go_get(
    name = "aws-sdk-go",
    get = "github.com/aws/aws-sdk-go/...",
    install = [
        "aws/...",
        "internal/...",
        "private/...",
        "service/...",
    ],
    licences = ["apache-2.0"],
    revision = "v1.33.5",
    deps = [
        ":jmespath",
        "//third_party/go:net",
    ],
)

go_get(
    name = "jmespath",
    get = "github.com/jmespath/go-jmespath",
    licences = ["apache-2.0"],
    revision = "3433f3ea46d9f8019119e7dd41274e112a2359a9",
)

filegroup(
    name = "config",
    srcs = ["config.jsonnet"],
    binary = True,
)

sh_cmd(
    name = "run_local",
    cmd = "cd plz-out/bin/browser && exec ./bb-browser config.jsonnet",
    srcs = [":bb_browser"],
    deps = [
        ":config",
    ],
)
