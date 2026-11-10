package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// runCLI runs a command and returns the contents of its output file.
func runCLI(t *testing.T, cmd string, o opts) string {
	t.Helper()
	if o.out == "" {
		o.out = filepath.Join(t.TempDir(), "out.csv")
	}
	o.quiet = true
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := run(ctx, cmd, o); err != nil {
		t.Fatalf("%s: %v", cmd, err)
	}
	b, err := os.ReadFile(o.out)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestCommands(t *testing.T) {
	dir := t.TempDir()
	regions := filepath.Join(dir, "regions.csv")
	os.WriteFile(regions, []byte("Region,manager\nNairobi,Amina\nMombasa,Otieno\n"), 0o644)
	src := "testdata/messy.csv"

	out := runCLI(t, "query", opts{in: src, snake: true, dedupe: true, join: regions, on: "region",
		derive: multi{"net = round(amount_kes * 0.84, 2)", "band = if(net > 1000, high, low)"},
		where:  multi{"net not null", "manager not null"}, selectCols: "customer_name,manager,net,band", sortKey: "-net", limit: 2})
	if out != "customer_name,manager,net,band\nTullow Oil,Amina,10080,high\nKenya Airways,Otieno,7350,high\n" {
		t.Fatalf("query:\n%s", out)
	}
	out = runCLI(t, "query", opts{in: src, snake: true, group: "region", agg: "count,sum(amount_kes):total", sortKey: "-total", format: "md"})
	if !strings.HasPrefix(out, "| region  | count | total |") || !strings.Contains(out, "| Nairobi |     4 | 14599 |") {
		t.Fatalf("group:\n%s", out)
	}
	for _, f := range []string{"out.ndjson", "out.json", "out.xlsx", "out.parquet", "out.tsv"} {
		p := filepath.Join(dir, f)
		runCLI(t, "clean", opts{in: src, snake: true, out: p})
		back := runCLI(t, "convert", opts{in: p, format: "csv"})
		if !strings.HasPrefix(back, "invoice_no,customer_name,region,amount_kes,invoice_date,paid,email\n1001,Acme Ltd,Nairobi,1250,2025-01-15,true,ops@acme.co.ke\n") {
			t.Fatalf("%s round trip:\n%s", f, back)
		}
	}
	out = runCLI(t, "profile", opts{in: src, jsonReport: true, top: 2})
	if !strings.Contains(out, `"rows": 8`) || !strings.Contains(out, `"duplicate_rows": 1`) {
		t.Fatalf("profile json:\n%s", out)
	}
	log := filepath.Join(dir, "runs.jsonl")
	runCLI(t, "clean", opts{in: src, dedupe: true, logFile: log, monitor: true})
	if b, _ := os.ReadFile(log); !strings.Contains(string(b), `"rows_duplicate_dropped":1`) ||
		!strings.Contains(string(b), `"version":"datacom `) || !strings.Contains(string(b), `"time":"`) {
		t.Fatalf("log: %s", b)
	}
}

func TestWatchCommand(t *testing.T) {
	live := filepath.Join(t.TempDir(), "live.ndjson")
	os.WriteFile(live, []byte("{\"s\":\"a\",\"n\":1}\n{\"s\":\"b\",\"n\":2}\n"), 0o644)
	out := filepath.Join(t.TempDir(), "watch.txt")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(600 * time.Millisecond)
		f, _ := os.OpenFile(live, os.O_APPEND|os.O_WRONLY, 0o644)
		f.WriteString("{\"s\":\"a\",\"n\":5}\n")
		f.Close()
		time.Sleep(600 * time.Millisecond)
		cancel()
	}()
	err := run(ctx, "watch", opts{in: live, every: 400 * time.Millisecond, group: "s", agg: "count,sum(n)", sortKey: "s", out: out, quiet: true, format: "csv"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(out)
	got := string(b)
	if !strings.Contains(got, "a,1,1,new,new\nb,1,2,new,new\n") || !strings.Contains(got, "a,1,5,0,4\n") {
		t.Fatalf("watch output:\n%s", got)
	}
}

func TestBadInputsReturnErrors(t *testing.T) {
	for name, o := range map[string]opts{
		"missing file":   {in: "nope.csv"},
		"bad where":      {in: "testdata/messy.csv", where: multi{"amount >"}},
		"bad column":     {in: "testdata/messy.csv", selectCols: "zzz"},
		"bad join":       {in: "testdata/messy.csv", join: "nope.csv", on: "x"},
		"bad out format": {in: "testdata/messy.csv", format: "yaml"},
	} {
		o.out = filepath.Join(t.TempDir(), "x")
		o.quiet = true
		if err := run(context.Background(), "query", o); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	if err := run(context.Background(), "bogus", opts{}); err == nil {
		t.Error("unknown command should fail")
	}
}

func TestPhoneAndFoldCaseFlags(t *testing.T) {
	src := filepath.Join(t.TempDir(), "in.csv")
	os.WriteFile(src, []byte("Phone,Status\n0712 345 678,PAID\n+254 733 000 111,paid\n733000112,paid\n0700 000 000,Paid\n"), 0o644)
	out := runCLI(t, "clean", opts{in: src, snake: true, foldCase: true, phone: "+254", format: "csv"})
	want := "phone,status\n+254712345678,paid\n+254733000111,paid\n+254733000112,paid\n+254700000000,paid\n"
	if out != want {
		t.Fatalf("got %q", out)
	}
}

// TestClosedPipeStillLogs checks that a run whose reader goes away (| head)
// ends with EPIPE and still writes its -log line.
func TestClosedPipeStillLogs(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	stdout := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = stdout; w.Close() }()
	log := filepath.Join(t.TempDir(), "runs.jsonl")
	err = run(context.Background(), "query", opts{in: "testdata/messy.csv", logFile: log, quiet: true, format: "csv"})
	if !errors.Is(err, syscall.EPIPE) {
		t.Fatalf("err = %v, want EPIPE", err)
	}
	if b, _ := os.ReadFile(log); !strings.Contains(string(b), `"error":"`) || !strings.Contains(string(b), "broken pipe") {
		t.Fatalf("log: %s", b)
	}
}

func TestSQLTableName(t *testing.T) {
	for in, want := range map[string]string{
		"sales.csv": "sales", "/tmp/Sales Q1-2025.csv.gz": "sales_q1_2025", "2025.csv": "data_2025",
		"https://x.test/exports/tx.ndjson?token=1": "tx", "": "data", "-": "data",
	} {
		if got := sqlTable(opts{in: in}); got != want {
			t.Errorf("sqlTable(%q) = %q, want %q", in, got, want)
		}
	}
	if got := sqlTable(opts{in: "a.csv", out: "report.sql"}); got != "report" {
		t.Errorf("out name wins: got %q", got)
	}
}

func TestCompressedOutput(t *testing.T) {
	dir := t.TempDir()
	for _, ext := range []string{".gz", ".zst"} {
		out := filepath.Join(dir, "clean.csv"+ext)
		runCLI(t, "clean", opts{in: "testdata/messy.csv", out: out, snake: true, limit: 2})
		got := runCLI(t, "convert", opts{in: out, format: "csv"})
		if !strings.HasPrefix(got, "invoice_no,customer_name,region,amount_kes,invoice_date,paid,email\n1001,") || strings.Count(got, "\n") != 3 {
			t.Fatalf("%s round trip:\n%s", ext, got)
		}
	}
}

// TestWatchGzip checks that a followed .gz file is decompressed rather than
// parsed as rows of compressed bytes.
func TestWatchGzip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "live.ndjson.gz")
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Write([]byte("{\"s\":\"a\",\"n\":1}\n{\"s\":\"b\",\"n\":2}\n"))
	zw.Close()
	os.WriteFile(path, buf.Bytes(), 0o644)
	out := filepath.Join(t.TempDir(), "watch.txt")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, "watch", opts{in: path, every: 200 * time.Millisecond, group: "s", agg: "sum(n)", sortKey: "s", out: out, quiet: true, format: "csv"})
	}()
	// The rows must appear while the file is still being followed, not only
	// once the run is cancelled and the reader gives up its buffered tail.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if b, _ := os.ReadFile(out); strings.Contains(string(b), "a,1,new\nb,2,new\n") {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			b, _ := os.ReadFile(out)
			t.Fatalf("rows were not delivered while following:\n%s", b)
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRedactCommand(t *testing.T) {
	log := filepath.Join(t.TempDir(), "runs.jsonl")
	out := runCLI(t, "clean", opts{in: "testdata/messy.csv", snake: true, redact: "all", logFile: log})
	if strings.Contains(out, "@") || strings.Count(out, "[EMAIL]") != 7 {
		t.Fatalf("clean -redact all:\n%s", out)
	}
	if b, _ := os.ReadFile(log); !strings.Contains(string(b), `"masked:email":7`) {
		t.Fatalf("log: %s", b)
	}
	prof := runCLI(t, "profile", opts{in: "testdata/messy.csv", jsonReport: true})
	if !strings.Contains(prof, `"personal_data": {`) || !strings.Contains(prof, "clean -redact emails masks it") {
		t.Fatalf("profile:\n%s", prof)
	}
	o := opts{in: "testdata/messy.csv", redact: "names", out: filepath.Join(t.TempDir(), "x.csv"), quiet: true}
	if err := run(context.Background(), "clean", o); err == nil || !strings.Contains(err.Error(), `"names"`) {
		t.Fatalf("bad -redact: %v", err)
	}
}

func TestManifest(t *testing.T) {
	dir := t.TempDir()
	read := func(path string) map[string]any {
		t.Helper()
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("%v\n%s", err, b)
		}
		return m
	}
	sum := func(path string) string {
		b, _ := os.ReadFile(path)
		h := sha256.Sum256(b)
		return hex.EncodeToString(h[:])
	}
	out, man := filepath.Join(dir, "out.csv.gz"), filepath.Join(dir, "run.json")
	runCLI(t, "query", opts{in: "testdata/messy.csv", snake: true, dedupe: true, where: multi{"amount_kes > 1000", "paid == true"},
		redact: "emails", out: out, manifest: man, limit: 1})
	m := read(man)
	in := m["inputs"].([]any)[0].(map[string]any)
	if m["status"] != "ok" || in["sha256"] != sum("testdata/messy.csv") || in["complete"] != true || in["role"] != "input" {
		t.Fatalf("inputs: %v", m)
	}
	o := m["outputs"].([]any)[0].(map[string]any)
	if o["sha256"] != sum(out) || o["rows"] != 1.0 || o["format"] != "csv" {
		t.Fatalf("outputs: %v (file %s)", o, sum(out))
	}
	b, _ := os.ReadFile(man)
	for _, want := range []string{`"rule": "dedupe",`, `"rule": "where",
      "expression": "amount_kes > 1000",
      "rows_dropped": 3`, `"rule": "limit",`, `"email": `} {
		if !strings.Contains(string(b), want) {
			t.Errorf("manifest lacks %q:\n%s", want, b)
		}
	}
	bad := filepath.Join(dir, "bad.json")
	err := run(context.Background(), "query", opts{in: "testdata/messy.csv", selectCols: "zzz", out: filepath.Join(dir, "x.csv"), manifest: bad, quiet: true})
	if m := read(bad); err == nil || m["status"] != "error" || !strings.Contains(m["error"].(string), "zzz") {
		t.Fatalf("error manifest: %v (err %v)", m, err)
	}
	missing := filepath.Join(dir, "missing.json")
	run(context.Background(), "clean", opts{in: "nope.csv", out: filepath.Join(dir, "y.csv"), manifest: missing, quiet: true})
	if m := read(missing); m["status"] != "error" {
		t.Fatalf("missing input manifest: %v", m)
	}
}

func TestWatchCSVGrowingSlowly(t *testing.T) {
	live := filepath.Join(t.TempDir(), "live.csv")
	os.WriteFile(live, []byte("status,n\na,1\n"), 0o644)
	out := filepath.Join(t.TempDir(), "watch.txt")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, "watch", opts{in: live, every: 200 * time.Millisecond, group: "status", agg: "count", out: out, quiet: true, format: "csv"})
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if b, _ := os.ReadFile(out); strings.Contains(string(b), "a,1") {
			break
		}
		if time.Now().After(deadline) {
			b, _ := os.ReadFile(out)
			t.Fatalf("a short live CSV was not reported:\n%s", b)
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
