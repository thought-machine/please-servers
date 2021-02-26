package(default_visibility = ["PUBLIC"])

go_module(
    name = "prometheus",
    install = [
        "prometheus",
        "prometheus/internal",
        "prometheus/promhttp",
    ],
    module = "github.com/prometheus/client_golang",
    version = "v1.1.0",
    deps = [
        ":client_model",
        ":perks",
        ":procfs",
        ":prometheus_common",
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
    version = "6af20e3a5340d5e6bde20c8a7a78699efe19ac0a",
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
    version = "14fe0d1b01d4d5fc031dd4bec1823bd3ebbe8016",
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
    module = "github.com/prometheus/common",
    version = "v0.7.0",
    deps = [
        ":client_model",
        ":golang_protobuf_extensions",
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
