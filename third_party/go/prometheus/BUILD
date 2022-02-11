package(default_visibility = ["PUBLIC"])

go_module(
    name = "prometheus",
    install = ["..."],
    module = "github.com/prometheus/client_golang",
    version = "v1.11.0",
    deps = [
        ":client_model",
        ":perks",
        ":procfs",
        ":prometheus_common",
        "//third_party/go:json-iterator",
        "//third_party/go:xxhash_v2",
        "//third_party/go:net",
        "//third_party/go:protobuf",
    ],
)

go_module(
    name = "grpc-prometheus",
    install = [
        ".",
        "...",
    ],
    module = "github.com/grpc-ecosystem/go-grpc-prometheus",
    version = "v1.2.0",
    deps = [
        ":prometheus",
        "//third_party/go:grpc",
        "//third_party/go:net",
    ],
)

go_module(
    name = "procfs",
    install = [
        ".",
        "...",
    ],
    module = "github.com/prometheus/procfs",
    version = "v0.0.5",
    deps = [
        "//third_party/go:errgroup",
    ],
)

go_module(
    name = "perks",
    install = ["..."],
    module = "github.com/beorn7/perks",
    version = "v1.0.1",
)

go_module(
    name = "client_model",
    install = ["..."],
    module = "github.com/prometheus/client_model",
    version = "v0.2.0",
    deps = [
        "//third_party/go:protobuf",
    ],
)

go_module(
    name = "prometheus_common",
    install = [
        "expfmt",
        "model",
        "internal/...",
    ],
    version = "v0.30.0",
    module = "github.com/prometheus/common",
    deps = [
        ":client_model",
        ":golang_protobuf_extensions",
        "//third_party/go:kingpin",
        "//third_party/go:logrus",
        "//third_party/go:protobuf",
    ],
)

go_module(
    name = "golang_protobuf_extensions",
    install = ["..."],
    module = "github.com/matttproud/golang_protobuf_extensions",
    version = "v1.0.1",
    deps = [
        "//third_party/go:protobuf",
    ],
)
