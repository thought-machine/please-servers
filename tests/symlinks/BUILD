filegroup(
    name = "input",
    srcs = ["input.txt"],
)

genrule(
    name = "link_outside_output",
    srcs = [":input"],
    outs = ["out_dir"],
    cmd = "mkdir $OUTS && ln -s $SRCS $OUTS/out_file",
)
