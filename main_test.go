package main

import (
	"context"
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
