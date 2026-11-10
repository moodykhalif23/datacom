"""A public demo of datacom: upload a file, summarize it or clean it.

Only uploaded files are read, so nothing on the server is reachable.
"""
import tempfile
from pathlib import Path

import datacom

MASKS = {"Emails": "emails", "Phone numbers": "phones", "ID numbers": "ids", "Card numbers": "cards"}
FORMATS = ["csv", "xlsx", "parquet", "ndjson", "json"]


def summarize(path):
    if not path:
        raise ValueError("Upload a file first.")
    rep = datacom.profile(path)
    lines = [f"**{rep['rows']:,} rows, {len(rep['columns'])} columns.**"]
    lines += [f"- {issue}" for issue in rep["issues"]] or ["- Nothing looks wrong."]
    return "\n".join(lines), rep


COUNTS = ("rows_dropped", "values_changed", "values_masked", "documents_changed", "values_left_as_written", "rows_padded", "rows_trimmed")


def describe(m):
    lines = [f"**{m['rows_in']:,} rows in, {m['rows_out']:,} rows out** in {m['seconds']:.1f} s."]
    for r in m["rules"]:
        bits = []
        for k in COUNTS:
            v, label = r.get(k), k.replace("_", " ")
            if isinstance(v, dict):
                parts = [f"{kk} {vv:,}" for kk, vv in v.items() if isinstance(vv, (int, float)) and vv]
                if parts:
                    bits.append(f"{label}: " + ", ".join(parts))
            elif isinstance(v, (int, float)) and not isinstance(v, bool) and v:
                bits.append(f"{label} {v:,}")
        for t in r.get("test_sets", []):
            if t.get("rows_dropped"):
                bits.append(f"rows dropped for {Path(t['file']).name} {t['rows_dropped']:,}")
        if bits:
            lines.append(f"- **{r['rule']}**: " + "; ".join(bits))
    return "\n".join(lines)


def clean(path, tidy, dedupe, respell, phone, masks, text, quality, near, fmt):
    if not path:
        raise ValueError("Upload a file first.")
    phone = phone.strip().lstrip("+")
    if phone and not phone.isdigit():
        raise ValueError("The country code for phone numbers should be digits, like 254.")
    out = Path(tempfile.mkdtemp()) / f"{Path(path).name.split('.')[0]}-clean.{fmt}"
    m = datacom.clean(
        path, out,
        snake=tidy, dedupe=dedupe, fold_case=respell, phone=phone or None,
        redact=[MASKS[x] for x in masks] or None,
        text=[c.strip() for c in text.split(",") if c.strip()] or None,
        quality="default" if quality else None, near_dupes=0.8 if near else None,
    )
    return str(out), describe(m), m


def build():
    import gradio as gr

    def guard(fn):
        def run(*args):
            try:
                return fn(*args)
            except (ValueError, datacom.DatacomError) as e:
                raise gr.Error(str(e))
        return run

    with gr.Blocks(title="datacom") as demo:
        gr.Markdown(
            "# datacom\n"
            "Clean, profile and query data files too big or too messy for a spreadsheet. "
            "This demo runs the same engine as `pip install datacom` and the "
            "[command line](https://github.com/moodykhalif23/datacom). Your file is deleted when the Space restarts."
        )
        src = gr.File(label="Data file: CSV, TSV, JSON, NDJSON, Excel, ODS, Parquet, Avro or XML, also gzipped", type="filepath")
        with gr.Tab("Summarize"):
            go = gr.Button("Summarize", variant="primary")
            found = gr.Markdown()
            report = gr.JSON(label="Full report")
            go.click(guard(summarize), inputs=src, outputs=[found, report])
        with gr.Tab("Clean"):
            with gr.Row():
                tidy = gr.Checkbox(True, label="Tidy column names")
                dedupe = gr.Checkbox(True, label="Remove duplicate rows")
                respell = gr.Checkbox(False, label="Respell values to their most common form")
            phone = gr.Textbox(label="Country code for phone numbers", placeholder="none, or 254")
            masks = gr.CheckboxGroup(list(MASKS), label="Mask personal data")
            text = gr.Textbox(label="Clean document text in these columns", placeholder="body, title")
            with gr.Row():
                quality = gr.Checkbox(False, label="Drop low-quality documents")
                near = gr.Checkbox(False, label="Drop near-duplicates")
            fmt = gr.Dropdown(FORMATS, value="csv", label="Save as")
            go = gr.Button("Clean", variant="primary")
            result = gr.File(label="Cleaned file")
            changes = gr.Markdown()
            record = gr.JSON(label="Run record")
            go.click(guard(clean), inputs=[src, tidy, dedupe, respell, phone, masks, text, quality, near, fmt],
                     outputs=[result, changes, record])
    return demo


if __name__ == "__main__":
    build().launch(max_file_size="200mb")
