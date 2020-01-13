package(
    default_visibility = ["PUBLIC"],
    python_wheel_repo = "https://get.please.build/third_party/python/py3",
)

python_wheel(
    name = "requests",
    hashes = [
        "sha1: e43b552c4a6063d32b8c9bf742fb3e9ce579c4d8",
    ],
    version = "2.5.0",
)

python_wheel(
    name = "colorlog",
    hashes = ["sha1: 73eef4e430c84b2235ec6be2212aad42fc0a83c7"],
    version = "3.1.4",
)

python_wheel(
    name = "absl",
    package_name = "absl_py",
    hashes = ["sha1: eb015a38bdc765b54626ca47af80bab9e3099ac2"],
    version = "0.5.0",
    deps = [":six"],
)

python_wheel(
    name = "six",
    outs = ["six.py"],
    hashes = ["sha1: a96bbdb19bd254689e8687cef85593bc6b3d9703"],
    version = "1.11.0",
)
