"""Build platform wheels that bundle the datacom binary, with no build tools
beyond Go and the Python standard library.

    python3 tools/wheels.py 0.1.0                 all platforms into dist/
    python3 tools/wheels.py 0.1.0 linux/amd64     one platform
"""

import base64
import hashlib
import os
import re
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PLATFORMS = {
    "linux/amd64": "manylinux_2_17_x86_64.manylinux2014_x86_64.musllinux_1_1_x86_64",
    "linux/arm64": "manylinux_2_17_aarch64.manylinux2014_aarch64.musllinux_1_1_aarch64",
    "darwin/amd64": "macosx_10_12_x86_64",
    "darwin/arm64": "macosx_11_0_arm64",
    "windows/amd64": "win_amd64",
    "windows/arm64": "win_arm64",
}


def pep440(v):
    v = v.lstrip("v")
    m = re.fullmatch(r"(\d+\.\d+\.\d+)(?:-(?:(alpha|beta|rc)\.?(\d+)|dev))?", v)
    if not m:
        sys.exit(f"cannot turn {v!r} into a Python version")
    base, pre, n = m.groups()
    if pre:
        return base + {"alpha": "a", "beta": "b", "rc": "rc"}[pre] + n
    if v.endswith("-dev"):
        return base + ".dev0"
    return base


def record_line(name, data):
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
    return f"{name},sha256={digest},{len(data)}"


def build(version, target, out):
    goos, goarch = target.split("/")
    exe = "datacom.exe" if goos == "windows" else "datacom"
    with tempfile.TemporaryDirectory() as tmp:
        binary = Path(tmp) / exe
        env = dict(os.environ, CGO_ENABLED="0", GOOS=goos, GOARCH=goarch)
        subprocess.run(["go", "build", "-trimpath", "-ldflags", f"-s -w -X main.version=v{version}", "-o", str(binary), "."],
                       cwd=ROOT, env=env, check=True)
        files = []
        pkg = ROOT / "python" / "datacom"
        init = (pkg / "__init__.py").read_text().replace('__version__ = "0.0.0"', f'__version__ = "{version}"')
        files.append(("datacom/__init__.py", init.encode(), 0o644))
        files.append(("datacom/__main__.py", (pkg / "__main__.py").read_bytes(), 0o644))
        files.append((f"datacom/bin/{exe}", binary.read_bytes(), 0o755))
        info = f"datacom-{version}.dist-info"
        readme = (ROOT / "python" / "README.md").read_text()
        metadata = "\n".join([
            "Metadata-Version: 2.1",
            "Name: datacom",
            f"Version: {version}",
            "Summary: Clean, profile and query messy CSV, JSON, Excel and Parquet files; mask personal data and dedupe text for AI training",
            "Home-page: https://github.com/moodykhalif23/datacom",
            "Project-URL: Source, https://github.com/moodykhalif23/datacom",
            "License: MIT",
            "Requires-Python: >=3.8",
            "Provides-Extra: pandas",
            "Requires-Dist: pandas; extra == 'pandas'",
            "Classifier: License :: OSI Approved :: MIT License",
            "Classifier: Programming Language :: Python :: 3",
            "Classifier: Topic :: Scientific/Engineering :: Information Analysis",
            "Description-Content-Type: text/markdown",
            "",
            readme,
        ])
        files.append((f"{info}/METADATA", metadata.encode(), 0o644))
        files.append((f"{info}/WHEEL", f"Wheel-Version: 1.0\nGenerator: datacom tools/wheels.py\nRoot-Is-Purelib: false\nTag: py3-none-{PLATFORMS[target]}\n".encode(), 0o644))
        files.append((f"{info}/entry_points.txt", b"[console_scripts]\ndatacom = datacom.__main__:main\n", 0o644))
        files.append((f"{info}/LICENSE", (ROOT / "LICENSE").read_bytes(), 0o644))
        record = [record_line(n, d) for n, d, _ in files] + [f"{info}/RECORD,,"]
        files.append((f"{info}/RECORD", ("\n".join(record) + "\n").encode(), 0o644))
        out.mkdir(parents=True, exist_ok=True)
        wheel = out / f"datacom-{version}-py3-none-{PLATFORMS[target]}.whl"
        with zipfile.ZipFile(wheel, "w", zipfile.ZIP_DEFLATED) as z:
            for name, data, mode in files:
                zi = zipfile.ZipInfo(name, date_time=(2026, 1, 1, 0, 0, 0))
                zi.external_attr = (0o100000 | mode) << 16
                zi.compress_type = zipfile.ZIP_DEFLATED
                z.writestr(zi, data)
        return wheel


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    version = pep440(sys.argv[1])
    targets = sys.argv[2:] or list(PLATFORMS)
    for t in targets:
        if t not in PLATFORMS:
            sys.exit(f"unknown platform {t}; choose from {', '.join(PLATFORMS)}")
        print(build(version, t, ROOT / "dist" / "wheels"))


if __name__ == "__main__":
    main()
