c_library(
    name = "sandbox_lib",
    srcs = ["sandbox.c"],
    hdrs = ["sandbox.h"],
)

c_binary(
    name = "sandbox",
    srcs = ["main.c"],
    deps = [":sandbox_lib"],
    visibility = ["PUBLIC"],
)

c_binary(
    name = "alt_sandbox",
    srcs = ["alt_main.c"],
    deps = [":sandbox_lib"],
    visibility = ["PUBLIC"],
)
