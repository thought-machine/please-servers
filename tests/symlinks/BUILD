filegroup(
    name = "input",
    srcs = ["input.txt"],
)

genrule(
    name = "link_outside_output",
    srcs = [":input"],
    outs = ["out_dir"],
    cmd = "mkdir $OUTS && ln -s ../input.txt $OUTS/out_file",
)

sh_test(
    name = "link_outside_output_test",
    src = "link_outside_output_test.sh",
    data = [":link_outside_output"],
)
