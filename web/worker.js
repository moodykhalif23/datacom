// Runs the datacom command line, compiled to WebAssembly, on an in-memory
// file system. Inputs are read from the visitor's files on demand, so they
// never have to fit in memory; outputs live in memory until collected.
"use strict";

const O = { O_WRONLY: 1, O_RDWR: 2, O_CREAT: 64, O_EXCL: 128, O_TRUNC: 512, O_APPEND: 1024, O_DIRECTORY: 65536 };
const S_IFDIR = 0o040000, S_IFREG = 0o100000, S_IFIFO = 0o010000;

const nodes = new Map();
const fds = new Map();
const reader = new FileReaderSync();
let ino = 1, nextFd = 3, streams = { 1: [], 2: [] };

const fail = (code) => Object.assign(new Error(code), { code });
const norm = (p) => {
  const out = [];
  for (const s of p.split("/")) {
    if (s === ".." ) out.pop();
    else if (s && s !== ".") out.push(s);
  }
  return "/" + out.join("/");
};
const parent = (p) => p.slice(0, p.lastIndexOf("/")) || "/";
const size = (n) => n.blob ? n.blob.size : n.size || 0;

function stat(n, mode) {
  const t = n.mtime || Date.now(), s = size(n);
  return {
    dev: 0, ino: n.ino || 0, mode: mode || (n.dir ? S_IFDIR | 0o755 : S_IFREG | 0o644), nlink: 1, uid: 0, gid: 0, rdev: 0,
    size: s, blksize: 4096, blocks: Math.ceil(s / 512), atimeMs: t, mtimeMs: t, ctimeMs: t,
    isDirectory() { return !!n.dir; },
  };
}

function grow(n, want) {
  if (want <= n.data.length) return;
  const next = new Uint8Array(Math.max(want, n.data.length * 2, 1 << 16));
  next.set(n.data.subarray(0, n.size));
  n.data = next;
}

function mkdirs(...paths) {
  for (const p of paths) if (!nodes.has(p)) nodes.set(p, { dir: true, ino: ino++ });
}

function reset() {
  for (const p of [...nodes.keys()]) if (p.startsWith("/out/") || p.startsWith("/tmp/")) nodes.delete(p);
  mkdirs("/", "/in", "/out", "/tmp", "/home");
  fds.clear();
  nextFd = 3;
  streams = { 1: [], 2: [] };
}

globalThis.fs = {
  constants: O,
  writeSync(fd, buf) {
    if (streams[fd]) streams[fd].push(buf.slice());
    return buf.length;
  },
  write(fd, buf, offset, length, position, cb) {
    if (fd === 1 || fd === 2) return cb(null, this.writeSync(fd, buf.subarray(offset, offset + length)));
    const f = fds.get(fd);
    if (!f || !f.node.data) return cb(fail("EBADF"));
    const n = f.node, pos = position ?? (f.append ? n.size : f.pos);
    grow(n, pos + length);
    n.data.set(buf.subarray(offset, offset + length), pos);
    n.size = Math.max(n.size, pos + length);
    n.mtime = Date.now();
    if (position === null) f.pos = pos + length;
    cb(null, length);
  },
  read(fd, buf, offset, length, position, cb) {
    if (fd === 0) return cb(null, 0);
    const f = fds.get(fd);
    if (!f) return cb(fail("EBADF"));
    const n = f.node;
    if (n.dir) return cb(fail("EISDIR"));
    const pos = position ?? f.pos, len = Math.max(0, Math.min(length, size(n) - pos));
    if (len > 0) {
      buf.set(n.blob ? new Uint8Array(reader.readAsArrayBuffer(n.blob.slice(pos, pos + len))) : n.data.subarray(pos, pos + len), offset);
    }
    if (position === null) f.pos = pos + len;
    cb(null, len);
  },
  open(path, flags, mode, cb) {
    path = norm(path);
    let n = nodes.get(path);
    if (n && flags & O.O_CREAT && flags & O.O_EXCL) return cb(fail("EEXIST"));
    if (!n) {
      if (!(flags & O.O_CREAT)) return cb(fail("ENOENT"));
      const d = nodes.get(parent(path));
      if (!d || !d.dir) return cb(fail("ENOENT"));
      n = { data: new Uint8Array(0), size: 0, ino: ino++, mtime: Date.now() };
      nodes.set(path, n);
    }
    const writing = flags & (O.O_WRONLY | O.O_RDWR);
    if (writing && n.dir) return cb(fail("EISDIR"));
    if (writing && n.blob) return cb(fail("EACCES"));
    if (flags & O.O_TRUNC && n.data) n.size = 0;
    const fd = nextFd++;
    fds.set(fd, { node: n, pos: 0, append: !!(flags & O.O_APPEND) });
    cb(null, fd);
  },
  close(fd, cb) { fds.delete(fd); cb(null); },
  fstat(fd, cb) {
    if (fd <= 2) return cb(null, stat({}, S_IFIFO | 0o600));
    const f = fds.get(fd);
    f ? cb(null, stat(f.node)) : cb(fail("EBADF"));
  },
  stat(path, cb) {
    const n = nodes.get(norm(path));
    n ? cb(null, stat(n)) : cb(fail("ENOENT"));
  },
  lstat(path, cb) { this.stat(path, cb); },
  mkdir(path, perm, cb) {
    path = norm(path);
    if (nodes.has(path)) return cb(fail("EEXIST"));
    const d = nodes.get(parent(path));
    if (!d || !d.dir) return cb(fail("ENOENT"));
    mkdirs(path);
    cb(null);
  },
  readdir(path, cb) {
    path = norm(path);
    const n = nodes.get(path);
    if (!n) return cb(fail("ENOENT"));
    if (!n.dir) return cb(fail("ENOTDIR"));
    const pre = path === "/" ? "/" : path + "/";
    cb(null, [...nodes.keys()].filter((p) => p !== path && p.startsWith(pre) && !p.slice(pre.length).includes("/")).map((p) => p.slice(pre.length)).sort());
  },
  unlink(path, cb) {
    path = norm(path);
    const n = nodes.get(path);
    if (!n) return cb(fail("ENOENT"));
    if (n.dir) return cb(fail("EISDIR"));
    nodes.delete(path);
    cb(null);
  },
  rmdir(path, cb) {
    path = norm(path);
    const n = nodes.get(path);
    if (!n) return cb(fail("ENOENT"));
    if (!n.dir) return cb(fail("ENOTDIR"));
    if ([...nodes.keys()].some((p) => p.startsWith(path + "/"))) return cb(fail("ENOTEMPTY"));
    nodes.delete(path);
    cb(null);
  },
  rename(from, to, cb) {
    from = norm(from); to = norm(to);
    const n = nodes.get(from);
    if (!n) return cb(fail("ENOENT"));
    if (n.dir) return cb(fail("ENOSYS"));
    nodes.delete(from);
    nodes.set(to, n);
    cb(null);
  },
  ftruncate(fd, length, cb) {
    const f = fds.get(fd);
    if (!f || !f.node.data) return cb(fail("EBADF"));
    grow(f.node, length);
    if (length > f.node.size) f.node.data.fill(0, f.node.size, length);
    f.node.size = length;
    cb(null);
  },
  truncate(path, length, cb) {
    const n = nodes.get(norm(path));
    if (!n || !n.data) return cb(fail("ENOENT"));
    grow(n, length);
    n.size = length;
    cb(null);
  },
  fsync(fd, cb) { cb(null); },
  chmod(path, mode, cb) { cb(null); },
  fchmod(fd, mode, cb) { cb(null); },
  chown(path, uid, gid, cb) { cb(null); },
  fchown(fd, uid, gid, cb) { cb(null); },
  lchown(path, uid, gid, cb) { cb(null); },
  utimes(path, atime, mtime, cb) { cb(null); },
  link(path, link, cb) { cb(fail("ENOSYS")); },
  symlink(path, link, cb) { cb(fail("ENOSYS")); },
  readlink(path, cb) { cb(fail("ENOSYS")); },
};

globalThis.process = {
  getuid() { return 0; }, getgid() { return 0; }, geteuid() { return 0; }, getegid() { return 0; },
  getgroups() { return []; }, pid: 1, ppid: 0, umask() { return 0o022; },
  cwd() { return "/"; }, chdir() { throw fail("ENOSYS"); },
};

importScripts("wasm_exec.js");

let module = null, current = 0;
globalThis.datacomProgress = (rows, bytes) => postMessage({ type: "progress", id: current, rows, bytes });

async function run(id, args) {
  reset();
  current = id;
  const go = new Go();
  go.argv = ["datacom", ...args];
  go.env = { TMPDIR: "/tmp", HOME: "/home" };
  let code = 0;
  go.exit = (c) => { code = c; };
  await go.run(await WebAssembly.instantiate(module, go.importObject));
  const text = (fd) => {
    const all = new Uint8Array(streams[fd].reduce((n, b) => n + b.length, 0));
    let at = 0;
    for (const b of streams[fd]) { all.set(b, at); at += b.length; }
    return new TextDecoder().decode(all);
  };
  const files = {};
  for (const [p, n] of nodes) if (p.startsWith("/out/") && n.data) files[p] = n.data.slice(0, n.size);
  return { code, stdout: text(1), stderr: text(2), files };
}

onmessage = async ({ data: m }) => {
  try {
    if (m.type === "module") {
      module = m.module;
      reset();
    } else if (m.type === "file") {
      nodes.set(m.path, { blob: m.file, ino: ino++, mtime: m.file.lastModified || Date.now() });
    } else if (m.type === "forget") {
      nodes.delete(m.path);
    } else if (m.type === "run") {
      const r = await run(m.id, m.args);
      postMessage({ type: "done", id: m.id, ...r }, Object.values(r.files).map((b) => b.buffer));
    }
  } catch (e) {
    postMessage({ type: "done", id: m.id, code: 1, stdout: "", stderr: "datacom: " + (e && e.message || e), files: {} });
  }
};
