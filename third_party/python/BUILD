subinclude("///python//build_defs:python")

package(
    default_visibility = ["PUBLIC"],
    python = {
        "wheel_repo": "https://get.please.build/third_party/python/py3",
    },
)

python_wheel(
    name = "requests",
    hashes = [
        "0e3c6b1de3156c66bae6ef74ed9c54320bcf39b98760b3356b9c9517da2c919f",
    ],
    version = "2.5.0",
)

python_wheel(
    name = "colorlog",
    hashes = ["8b234ebae1ba1237bc79c0d5f1f47b31a3f3e90c0b4c2b0ebdde63a174d3b97b"],
    version = "3.1.4",
)

python_wheel(
    name = "absl",
    package_name = "absl_py",
    hashes = ["0d8b1136d969ab9d795cf0dcd2b369ad9fc17eeccc4f8f10b253e188f5634e17"],
    version = "0.5.0",
    deps = [":six"],
)

python_wheel(
    name = "six",
    outs = ["six.py"],
    hashes = ["832dc0e10feb1aa2c68dcc57dbb658f1c7e65b9b61af69048abc87a2db00a0eb"],
    version = "1.11.0",
)
