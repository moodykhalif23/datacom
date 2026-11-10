// Stands in for the datacom ui server: the page's API calls run the command
// line in a worker instead, so the same page works with no server at all.
(() => {
  "use strict";
  const PREVIEW = 50;
  const MODULE = (async () => {
    const res = await fetch("datacom.wasm.gz");
    if (!res.ok) throw new Error("datacom could not be loaded (" + res.status + ")");
    let bytes = new Uint8Array(await res.arrayBuffer());
    if (bytes[0] === 0x1f && bytes[1] === 0x8b) {
      bytes = new Uint8Array(await new Response(new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"))).arrayBuffer());
    }
    return WebAssembly.compile(bytes);
  })();

  const files = new Map(), jobs = new Map(), waiting = new Map();
  let worker, ready, seq = 0;

  function boot() {
    worker = new Worker("worker.js");
    worker.onmessage = ({ data: m }) => {
      if (m.type === "progress") {
        const j = jobs.get(m.id);
        if (j) Object.assign(j, { rows: m.rows, bytes: m.bytes });
      } else if (waiting.has(m.id)) {
        waiting.get(m.id)(m);
        waiting.delete(m.id);
      }
    };
    ready = MODULE.then((module) => {
      worker.postMessage({ type: "module", module });
      for (const [path, file] of files) worker.postMessage({ type: "file", path, file });
    });
  }
  boot();

  async function exec(args, id = ++seq) {
    await ready;
    return new Promise((resolve) => {
      waiting.set(id, resolve);
      worker.postMessage({ type: "run", id, args });
    });
  }

  const decode = (b) => new TextDecoder().decode(b || new Uint8Array(0));
  const json = (status, body) => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
  const failure = (r) => {
    const lines = r.stderr.trim().split("\n").filter((l) => l.startsWith("datacom: "));
    return (lines.pop() || r.stderr.trim() || "datacom stopped unexpectedly").replace(/^datacom: /, "");
  };
  const sources = (q) => [...(q.sources || []), q.source || ""].map((s) => s.trim()).filter(Boolean);
  const reading = (q) => [...(q.input && q.input !== "auto" ? ["-i", q.input] : []), ...(q.sheet ? ["-sheet", q.sheet] : [])];
  const ext = (name) => { const base = name.split("/").pop(), dot = base.lastIndexOf("."); return dot >= 0 ? base.slice(dot) : ""; };
  const stem = (name) => name.slice(0, name.length - ext(name).length);

  function downloadName(name, source, cmd, extension) {
    let s = stem((name || source.replace(/\\/g, "/").split("?")[0].split("/").pop()).replace(/\.gz$/, ""));
    if (!s || s === ".") s = "datacom";
    return (cmd === "convert" ? s : s + "-" + cmd) + extension;
  }

  function safeName(name) {
    const s = name.split(/[\\/]/).pop().replace(/[^A-Za-z0-9._-]/g, "_");
    return /^[._]*$/.test(s) ? "upload" : s;
  }

  function csvRows(bytes, max, sep) {
    const cut = bytes.length > 4 << 20, s = decode(cut ? bytes.subarray(0, 4 << 20) : bytes);
    const rows = [];
    let row = [], f = "", quoted = false;
    for (let i = 0; i < s.length && rows.length < max; i++) {
      const c = s[i];
      if (quoted) {
        if (c !== '"') f += c;
        else if (s[i + 1] === '"') { f += '"'; i++; }
        else quoted = false;
      } else if (c === '"' && f === "") quoted = true;
      else if (c === sep) { row.push(f); f = ""; }
      else if (c === "\n") { row.push(f.endsWith("\r") ? f.slice(0, -1) : f); rows.push(row); row = []; f = ""; }
      else f += c;
    }
    if (!cut && rows.length < max && (f || row.length)) rows.push([...row, f]);
    return rows;
  }

  function tail(b, n) {
    if (b.length <= n) return decode(b);
    const t = decode(b.subarray(b.length - n)), i = t.indexOf("\n");
    return i >= 0 ? t.slice(i + 1) : t;
  }

  async function preview(job, out) {
    if (job.cmd === "profile" || ["md", "html", "sql"].includes(job.format)) return { text: tail(out, 1 << 20) };
    let csv = out, sep = job.format === "tsv" ? "\t" : ",";
    if (job.format !== "csv" && job.format !== "tsv") {
      const path = "/in/preview-" + job.id + job.ext;
      await ready;
      worker.postMessage({ type: "file", path, file: new Blob([out]) });
      const r = await exec(["convert", path, "-i", job.format, "-f", "csv", "-q", "-o", "/out/preview.csv"]);
      worker.postMessage({ type: "forget", path });
      if (r.code) throw new Error(failure(r));
      csv = r.files["/out/preview.csv"];
      sep = ",";
    }
    const rows = csvRows(csv, PREVIEW + 1, sep);
    return { header: rows[0] || [], rows: rows.slice(1) };
  }

  async function columns(q) {
    const srcs = sources(q);
    if (!srcs.length) return json(400, { error: "choose a file first" });
    const r = await exec(["query", ...srcs, ...reading(q), ...(q.snake ? ["-snake"] : []), "-limit", "1", "-f", "csv", "-q", "-o", "/out/columns.csv"]);
    if (r.code) return json(400, { error: failure(r) });
    return json(200, { columns: csvRows(r.files["/out/columns.csv"], 1, ",")[0] || [], files: srcs.length });
  }

  async function run(q) {
    const srcs = sources(q);
    if (!["profile", "clean", "query", "convert"].includes(q.cmd)) return json(400, { error: `unknown action "${q.cmd}"` });
    if (!srcs.length) return json(400, { error: "choose a file first" });
    const id = ++seq, format = q.cmd === "profile" ? "" : q.format || "csv";
    const extension = q.cmd === "profile" ? (q.json ? ".json" : ".txt") : "." + format;
    const a = [q.cmd, ...srcs, ...reading(q), "-o", "/out/result" + extension, "-q", "-manifest", "/out/run.json", "-log", "/out/run.log"];
    const flag = (name, on) => { if (on) a.push("-" + name); };
    const val = (name, v) => { if (v !== undefined && v !== null && v !== "" && v !== 0) a.push("-" + name, String(v)); };
    if (format) a.push("-f", format);
    flag("snake", q.snake); flag("dedupe", q.dedupe); flag("keep-empty", q.keep_empty); flag("no-types", q.no_types); flag("fold-case", q.fold_case);
    val("phone", q.phone); val("redact", (q.redact || []).join(","));
    val("join", q.join); val("on", q.on);
    for (const d of q.derive || []) a.push("-derive", d);
    for (const w of q.where || []) a.push("-where", w);
    val("select", q.select); val("group", q.group); val("agg", q.agg); val("sort", q.sort); val("limit", q.limit);
    if (q.cmd === "profile") { val("top", q.top); flag("json", q.json); }
    val("text", (q.text || []).join(",")); val("quality", q.quality); val("decontam", (q.decontam || []).join(","));
    val("decontam-n", q.decontam_n); val("near-dupes", q.near_dupes); val("near-mem", q.near_mem);

    const job = { id, cmd: q.cmd, format, ext: extension, filename: downloadName(q.name, srcs[0], q.cmd, extension), start: Date.now(), rows: 0, bytes: 0 };
    jobs.set(id, job);
    exec(a, id).then(async (r) => {
      if (job.done) return;
      const log = decode(r.files["/out/run.log"]).trim().split("\n").pop();
      job.report = log ? JSON.parse(log) : { rows_in: job.rows, seconds: (Date.now() - job.start) / 1000 };
      if (r.files["/out/run.json"]) job.record = URL.createObjectURL(new Blob([r.files["/out/run.json"]], { type: "application/json" }));
      if (r.code) job.error = failure(r);
      else {
        const out = r.files["/out/result" + extension] || new Uint8Array(0);
        job.download = URL.createObjectURL(new Blob([out]));
        try { job.preview = await preview(job, out); } catch (e) { job.error = e.message; }
      }
      job.done = true;
    }).catch((e) => Object.assign(job, { done: true, error: (e && e.message) || String(e) }));
    return json(200, { id: String(id) });
  }

  function status(id) {
    const j = jobs.get(+id);
    if (!j) return json(404, { error: "that run is no longer available" });
    const secs = (Date.now() - j.start) / 1000;
    if (!j.done) return json(200, { done: false, rows_in: j.rows, bytes: j.bytes, seconds: secs });
    const resp = { done: true, seconds: (j.report && j.report.seconds) || secs, report: j.report || {}, preview: j.preview || {} };
    if (j.record) Object.assign(resp, { record: j.record, record_name: stem(j.filename) + "-record.json" });
    if (j.error) resp.error = j.error;
    else Object.assign(resp, { download: j.download, filename: j.filename });
    return json(200, resp);
  }

  function cancel(id) {
    const j = jobs.get(+id);
    if (j && !j.done) {
      worker.terminate();
      waiting.clear();
      boot();
      Object.assign(j, { done: true, error: "interrupted", report: { rows_in: j.rows, seconds: (Date.now() - j.start) / 1000 } });
    }
    return json(200, { ok: true });
  }

  const realFetch = window.fetch.bind(window);
  window.fetch = async (url, init = {}) => {
    const path = typeof url === "string" ? url : url.url;
    if (!path.startsWith("/api/")) return realFetch(url, init);
    try {
      const body = init.body ? JSON.parse(init.body) : {};
      if (path === "/api/columns") return await columns(body);
      if (path === "/api/run") return await run(body);
      let m = path.match(/^\/api\/job\/(\d+)$/);
      if (m) return status(m[1]);
      m = path.match(/^\/api\/job\/(\d+)\/cancel$/);
      if (m) return cancel(m[1]);
      return json(404, { error: "not found" });
    } catch (e) {
      return json(500, { error: (e && e.message) || String(e) });
    }
  };

  window.XMLHttpRequest = class {
    constructor() { this.upload = {}; this.status = 0; this.responseText = ""; }
    open() {}
    setRequestHeader() {}
    send(form) {
      setTimeout(() => {
        const file = form.get("file");
        if (!file) {
          this.status = 400;
          this.responseText = JSON.stringify({ error: "no file was sent" });
        } else {
          const name = safeName(file.name), path = "/in/" + ++seq + "-" + name;
          files.set(path, file);
          ready.then(() => worker.postMessage({ type: "file", path, file }));
          if (this.upload.onprogress) this.upload.onprogress({ lengthComputable: true, loaded: file.size, total: file.size });
          this.status = 200;
          this.responseText = JSON.stringify({ path, name });
        }
        if (this.onload) this.onload();
      });
    }
  };
})();
