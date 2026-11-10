"""Publish the browser version built by tools/web.py to a Hugging Face Space,
creating it if needed and sending binary files through the Hub's LFS upload.

    python3 tools/web.py 0.2.0 && python3 tools/space.py datacom dist/web

The token comes from HF_TOKEN or ~/.cache/huggingface/token and is only sent
to the endpoint and to the upload URLs it hands back.
"""
import argparse
import base64
import hashlib
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

LFS = "application/vnd.git-lfs+json"


def token():
    t = os.environ.get("HF_TOKEN") or ""
    p = Path.home() / ".cache" / "huggingface" / "token"
    if not t and p.exists():
        t = p.read_text()
    if not t.strip():
        sys.exit("no token: save a write token to ~/.cache/huggingface/token or set HF_TOKEN")
    return t.strip()


def call(method, url, body=None, headers=None):
    req = urllib.request.Request(url, data=body, method=method, headers={"User-Agent": "datacom-release", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=300) as r:
            return r.status, r.read().decode(errors="replace"), r.headers
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace"), e.headers


def must(what, res, ok=(200,)):
    code, text, headers = res
    if code not in ok:
        sys.exit(f"{what} failed: {code} {text[:400]}")
    return text, headers


def lfs_upload(endpoint, repo, tok, data):
    oid = hashlib.sha256(data).hexdigest()
    batch = {"operation": "upload", "transfers": ["basic", "multipart"], "objects": [{"oid": oid, "size": len(data)}],
             "hash_algo": "sha256", "ref": {"name": "main"}}
    text, _ = must("LFS batch", call("POST", f"{endpoint}/spaces/{repo}.git/info/lfs/objects/batch", json.dumps(batch).encode(),
                                     {"Authorization": f"Bearer {tok}", "Content-Type": LFS, "Accept": LFS}))
    obj = json.loads(text)["objects"][0]
    if obj.get("error"):
        sys.exit(f"LFS refused {oid[:12]}: {obj['error']}")
    actions = obj.get("actions") or {}
    up = actions.get("upload")
    if up:
        header = dict(up.get("header") or {})
        if "chunk_size" in header:
            size = int(header.pop("chunk_size"))
            parts = []
            for key in sorted((k for k in header if k.isdigit()), key=int):
                n = int(key)
                _, h = must(f"part {n}", call("PUT", header[key], data[(n - 1) * size:n * size]))
                parts.append({"partNumber": n, "etag": h.get("ETag", "").strip('"')})
            must("multipart completion", call("POST", up["href"], json.dumps({"oid": oid, "parts": parts}).encode(),
                                              {"Content-Type": LFS, "Accept": LFS}))
        else:
            must("LFS upload", call("PUT", up["href"], data, header))
    verify = actions.get("verify")
    if verify:
        basic = base64.b64encode(f"USER:{tok}".encode()).decode()
        must("LFS verify", call("POST", verify["href"], json.dumps({"oid": oid, "size": len(data)}).encode(),
                                {"Authorization": f"Basic {basic}", "Content-Type": LFS, "Accept": LFS, **(verify.get("header") or {})}))
    return oid


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("name")
    ap.add_argument("folder")
    ap.add_argument("--sdk", default="static")
    ap.add_argument("--endpoint", default="https://huggingface.co")
    a = ap.parse_args()
    tok = token()
    auth = {"Authorization": f"Bearer {tok}"}
    text, _ = must("token check", call("GET", f"{a.endpoint}/api/whoami-v2", headers=auth))
    repo = f"{json.loads(text)['name']}/{a.name}"
    must(f"creating {repo}", call("POST", f"{a.endpoint}/api/repos/create",
                                  json.dumps({"type": "space", "name": a.name, "sdk": a.sdk, "private": False}).encode(),
                                  {**auth, "Content-Type": "application/json"}), ok=(200, 409))
    files = [f for f in sorted(Path(a.folder).iterdir()) if f.is_file()]
    probe = {"files": [{"path": f.name, "size": f.stat().st_size, "sample": base64.b64encode(f.read_bytes()[:512]).decode()} for f in files]}
    text, _ = must("preupload", call("POST", f"{a.endpoint}/api/spaces/{repo}/preupload/main", json.dumps(probe).encode(),
                                     {**auth, "Content-Type": "application/json"}))
    mode = {x["path"]: x["uploadMode"] for x in json.loads(text)["files"]}
    lines = [{"key": "header", "value": {"summary": "Publish the datacom browser demo", "description": ""}}]
    for f in files:
        data = f.read_bytes()
        if mode.get(f.name) == "lfs":
            lines.append({"key": "lfsFile", "value": {"path": f.name, "algo": "sha256", "oid": lfs_upload(a.endpoint, repo, tok, data), "size": len(data)}})
        else:
            lines.append({"key": "file", "value": {"path": f.name, "encoding": "base64", "content": base64.b64encode(data).decode()}})
    body = "\n".join(json.dumps(x) for x in lines).encode()
    must("commit", call("POST", f"{a.endpoint}/api/spaces/{repo}/commit/main", body, {**auth, "Content-Type": "application/x-ndjson"}))
    print(f"pushed {len(files)} files ({sum(1 for f in files if mode.get(f.name) == 'lfs')} through LFS) to https://huggingface.co/spaces/{repo}")


if __name__ == "__main__":
    main()
