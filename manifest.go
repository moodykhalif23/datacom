package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/moodykhalif23/datacom/pipeline"
)

// runRecord gathers what a run's manifest needs while the run is going.
type runRecord struct {
	ledger    *pipeline.Ledger
	outputs   []*outputRecord
	rawHeader []string
	header    []string
}

type outputRecord struct {
	Path   string `json:"path"`
	Format string `json:"format"`
	Rows   int64  `json:"rows"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
	hw     *pipeline.HashWriter
}

type manifest struct {
	Version  int                    `json:"manifest_version"`
	Tool     string                 `json:"tool"`
	Command  []string               `json:"command"`
	Started  string                 `json:"started"`
	Finished string                 `json:"finished"`
	Seconds  float64                `json:"seconds"`
	Status   string                 `json:"status"`
	Error    string                 `json:"error,omitempty"`
	Inputs   []pipeline.InputDigest `json:"inputs"`
	Outputs  []*outputRecord        `json:"outputs"`
	RowsIn   int64                  `json:"rows_in"`
	RowsOut  int64                  `json:"rows_out"`
	Rules    []rule                 `json:"rules"`
}

// rule is one step of the run as ordered key and value pairs, so the
// manifest reads in the order the step was described.
type rule []any

func (r rule) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	for i := 0; i+1 < len(r); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		k, err := marshal(r[i])
		if err != nil {
			return nil, err
		}
		v, err := marshal(r[i+1])
		if err != nil {
			return nil, err
		}
		b.Write(k)
		b.WriteByte(':')
		b.Write(v)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

func marshal(v any) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(b.Bytes(), "\n"), nil
}

func writeManifest(path, cmd string, o opts, rep pipeline.RunReport, rec *runRecord, start time.Time, runErr error) error {
	m := manifest{
		Version: 1, Tool: rep.Version, Command: rep.Command,
		Started: start.Format(time.RFC3339), Finished: time.Now().Format(time.RFC3339), Seconds: rep.Seconds,
		Status: "ok", RowsIn: rep.RowsIn, RowsOut: rep.RowsOut, Outputs: rec.outputs, Inputs: []pipeline.InputDigest{},
	}
	if m.Outputs == nil {
		m.Outputs = []*outputRecord{}
	}
	errText := rep.Error
	if runErr != nil {
		errText = runErr.Error()
	}
	switch {
	case errText == "":
	case errText == "interrupted" || strings.Contains(errText, "context canceled"):
		m.Status, m.Error = "stopped", errText
	default:
		m.Status, m.Error = "error", errText
	}
	if rec.ledger != nil {
		m.Inputs = rec.ledger.Inputs()
		for i := range m.Inputs {
			switch m.Inputs[i].Source {
			case o.join:
				m.Inputs[i].Role = "join"
			default:
				m.Inputs[i].Role = "input"
			}
		}
	}
	for _, out := range rec.outputs {
		out.SHA256, out.Bytes = out.hw.Sum()
	}
	m.Rules = rules(cmd, o, rep, rec)
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(m); err != nil {
		return err
	}
	return os.WriteFile(path, b.Bytes(), 0o644)
}

// rules lists what the run did to the data, in pipeline order, with how
// many rows or values each step dropped or changed.
func rules(cmd string, o opts, rep pipeline.RunReport, rec *runRecord) []rule {
	rs := []rule{}
	add := func(name string, kv ...any) { rs = append(rs, append(rule{"rule", name}, kv...)) }
	c := rep.Counts
	if cmd == "convert" {
		add("convert", "values_changed", 0)
		return rs
	}
	if cmd == "profile" || !o.noTypes {
		var untyped int64
		for _, b := range rep.Bad {
			untyped += b.Count
		}
		add("types", "values_changed", c["changed:types"], "values_left_as_written", untyped)
	}
	if o.snake {
		renamed := map[string]string{}
		for i, h := range rec.rawHeader {
			if i < len(rec.header) && rec.header[i] != h {
				renamed[h] = rec.header[i]
			}
		}
		add("snake-headers", "columns_renamed", renamed)
	}
	add("row-width", "rows_padded", rep.Padded, "rows_trimmed", rep.Trimmed)
	if cmd == "profile" || !o.keepEmpty {
		add("drop-empty", "rows_dropped", rep.Empty)
	}
	if o.dedupe {
		add("dedupe", "rows_dropped", rep.Dupes)
	}
	if o.foldCase {
		add("fold-case", "values_changed", c["changed:fold-case"])
	}
	if o.phone != "" {
		add("phone", "country_code", strings.TrimLeft(o.phone, "+"))
	}
	if o.text != "" {
		fixed := map[string]int64{}
		for _, k := range []string{"encoding", "mojibake", "html", "unicode", "control", "whitespace"} {
			fixed[k] = c["text:"+k]
		}
		add("text", "columns", splitList(o.text), "documents_changed", fixed)
	}
	if cmd == "profile" {
		return rs
	}
	if o.join != "" {
		add("join", "file", o.join, "on", o.on, "rows_unmatched", rep.Unmatched)
	}
	for _, d := range o.derive {
		add("derive", "expression", d)
	}
	for i, w := range o.where {
		add("where", "expression", w, "rows_dropped", c[fmt.Sprintf("dropped:where:%d", i+1)])
	}
	if o.selectCols != "" {
		add("select", "columns", splitList(o.selectCols))
	}
	if o.redact != "" {
		masked := map[string]int64{}
		for _, k := range []string{"email", "phone", "id", "card"} {
			masked[k] = c["masked:"+k]
		}
		add("redact", "kinds", splitList(o.redact), "values_masked", masked)
	}
	if o.group != "" || o.agg != "" {
		agg := o.agg
		if agg == "" {
			agg = "count"
		}
		add("group", "by", splitList(o.group), "totals", agg)
	}
	if o.sortKey != "" {
		add("sort", "by", o.sortKey)
	}
	if o.limit > 0 {
		add("limit", "rows", o.limit)
	}
	return rs
}
