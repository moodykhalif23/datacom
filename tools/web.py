"""Build the browser version of datacom into dist/web: the datacom ui page,
a worker that runs the command line compiled to WebAssembly, and the module.

    python3 tools/web.py 0.2.0
"""

import gzip
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "dist" / "web"

HIDE = """<style>
.tool:has(input[value=watch]), .pathrow, .drop > .help, .more,
#joinPath, #useJoinPath, #testPath, #useTestPath { display: none !important; }
</style>
<script src="web.js"></script>
"""


def page(version):
    html = (ROOT / "ui.html").read_text()
    for old, new in [
        ("{{TOKEN}}", "local"),
        ("{{VERSION}}", f"datacom v{version} (WebAssembly)"),
        ("{{VERSION_SHORT}}", f"v{version}"),
        ("runs on this computer", "runs in your browser"),
        ('src="/logo.png"', 'src="logo.png"'),
        ("Nothing leaves your computer. This page only talks to datacom running on it.",
         "Nothing leaves your computer: datacom runs inside this page. For very large files the command line is faster: pip install datacom."),
        ("</head>", HIDE + "</head>"),
    ]:
        if old not in html:
            sys.exit(f"ui.html no longer contains {old!r}; update tools/web.py")
        html = html.replace(old, new)
    return html


def main():
    version = (sys.argv[1] if len(sys.argv) > 1 else "0.0.0-dev").lstrip("v")
    shutil.rmtree(OUT, ignore_errors=True)
    OUT.mkdir(parents=True)
    goroot = subprocess.run(["go", "env", "GOROOT"], capture_output=True, text=True, check=True).stdout.strip()
    with tempfile.TemporaryDirectory() as tmp:
        wasm = Path(tmp) / "datacom.wasm"
        env = dict(os.environ, GOOS="js", GOARCH="wasm", CGO_ENABLED="0")
        subprocess.run(["go", "build", "-trimpath", "-ldflags", f"-s -w -X main.version=v{version}", "-o", str(wasm), "."],
                       cwd=ROOT, env=env, check=True)
        with open(OUT / "datacom.wasm.gz", "wb") as f, gzip.GzipFile(fileobj=f, mode="wb", compresslevel=9, mtime=0) as gz:
            gz.write(wasm.read_bytes())
    shutil.copy(Path(goroot) / "lib" / "wasm" / "wasm_exec.js", OUT / "wasm_exec.js")
    for name in ("web.js", "worker.js", "README.md"):
        shutil.copy(ROOT / "web" / name, OUT / name)
    shutil.copy(ROOT / "ui-logo.png", OUT / "logo.png")
    (OUT / "index.html").write_text(page(version))
    for f in sorted(OUT.iterdir()):
        print(f"{f.stat().st_size:>10,}  {f.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
