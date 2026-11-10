"""Clean, profile and query messy data files from Python.

A thin wrapper around the datacom binary that ships inside this package:
every function runs the same engine as the command line, so results match.
"""

import json
import os
import subprocess
import tempfile
from pathlib import Path

__all__ = ["DatacomError", "binary", "run", "profile", "clean", "query", "convert", "clean_frame", "__version__"]
__version__ = "0.0.0"

_REPEATABLE = {"where", "derive"}


class DatacomError(RuntimeError):
    """datacom exited with an error; stderr holds its message."""

    def __init__(self, returncode, stderr):
        super().__init__(stderr or f"datacom exited with status {returncode}")
        self.returncode = returncode
        self.stderr = stderr


def binary():
    """Path of the datacom executable: DATACOM_BINARY, else the bundled one."""
    env = os.environ.get("DATACOM_BINARY")
    if env:
        return env
    exe = Path(__file__).parent / "bin" / ("datacom.exe" if os.name == "nt" else "datacom")
    if not exe.exists():
        raise FileNotFoundError(f"no datacom binary at {exe}; install a platform wheel or set DATACOM_BINARY")
    return str(exe)


def _flags(options):
    args = []
    for key, value in options.items():
        if value is None or value is False:
            continue
        flag = "-" + key.replace("_", "-")
        if value is True:
            args.append(flag)
        elif isinstance(value, (list, tuple)):
            if key in _REPEATABLE:
                for v in value:
                    args += [flag, str(v)]
            else:
                args += [flag, ",".join(str(v) for v in value)]
        else:
            args += [flag, str(value)]
    return args


def _sources(source):
    if isinstance(source, (str, os.PathLike)):
        return [os.fspath(source)]
    return [os.fspath(s) for s in source]


def run(command, *sources, **options):
    """Run any datacom command and return the CompletedProcess.

    Keyword options become flags: snake=True is -snake, near_dupes=0.8 is
    -near-dupes 0.8, text=["title", "body"] is -text title,body, and lists
    for where and derive repeat the flag.
    """
    args = [binary(), command, *(os.fspath(s) for s in sources), *_flags(options)]
    p = subprocess.run(args, capture_output=True, text=True)
    if p.returncode != 0:
        raise DatacomError(p.returncode, p.stderr.strip())
    return p


def profile(source, **options):
    """Summarize a file and return the report as a dict."""
    return json.loads(run("profile", *_sources(source), json=True, **options).stdout)


def _write(command, source, output, options):
    with tempfile.TemporaryDirectory() as d:
        manifest = options.pop("manifest", None) or os.path.join(d, "run.json")
        run(command, *_sources(source), o=os.fspath(output), manifest=manifest, q=True, **options)
        with open(manifest, encoding="utf-8") as f:
            return json.load(f)


def clean(source, output, **options):
    """Clean source into output and return the run manifest as a dict.

    The manifest lists input and output SHA-256, every rule applied and how
    many rows or values each one dropped or changed.
    """
    return _write("clean", source, output, options)


def query(source, output, **options):
    """Filter, derive, join, group or sort, and return the run manifest."""
    return _write("query", source, output, options)


def convert(source, output, **options):
    """Write source in the format output's extension names; returns the manifest."""
    return _write("convert", source, output, options)


def clean_frame(df, **options):
    """Clean a pandas DataFrame and return a new one.

    The frame goes through a temporary Parquet file when pyarrow is
    installed, and CSV otherwise.
    """
    import pandas as pd

    try:
        import pyarrow  # noqa: F401

        ext = ".parquet"
    except ImportError:
        ext = ".csv"
    with tempfile.TemporaryDirectory() as d:
        src, dst = os.path.join(d, "in" + ext), os.path.join(d, "out" + ext)
        if ext == ".parquet":
            df.to_parquet(src, index=False)
        else:
            df.to_csv(src, index=False)
        clean(src, dst, **options)
        return pd.read_parquet(dst) if ext == ".parquet" else pd.read_csv(dst, dtype=str, keep_default_na=False)
