package main

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

type uiClient struct {
	t     *testing.T
	url   string
	token string
}

func (c *uiClient) do(method, path string, body io.Reader, contentType string) (int, map[string]any) {
	c.t.Helper()
	req, _ := http.NewRequest(method, c.url+path, body)
	req.Header.Set("X-Token", c.token)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer res.Body.Close()
	var out map[string]any
	json.NewDecoder(res.Body).Decode(&out)
	return res.StatusCode, out
}

func (c *uiClient) post(path string, v any) map[string]any {
	c.t.Helper()
	b, _ := json.Marshal(v)
	code, out := c.do("POST", path, bytes.NewReader(b), "application/json")
	if code != 200 {
		c.t.Fatalf("%s: %d %v", path, code, out["error"])
	}
	return out
}

// wait polls a job until it is done and returns its final status.
func (c *uiClient) wait(id string) map[string]any {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		code, st := c.do("GET", "/api/job/"+id, nil, "")
		if code != 200 {
			c.t.Fatalf("job %s: %d %v", id, code, st["error"])
		}
		if st["done"] == true {
			return st
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal("job did not finish")
	return nil
}

func TestUI(t *testing.T) {
	s, err := newUIServer()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	srv := httptest.NewServer(s.handler())
	defer srv.Close()
	c := &uiClient{t, srv.URL, s.token}

	res, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	page, _ := io.ReadAll(res.Body)
	if !strings.Contains(string(page), s.token) || strings.Contains(string(page), "{{") {
		t.Fatal("page is missing the token or still has placeholders")
	}
	if res, err := http.Get(srv.URL + "/logo.png"); err != nil || res.StatusCode != 200 || res.Header.Get("Content-Type") != "image/png" {
		t.Fatalf("logo: %v %v", err, res)
	}
	if code, _ := (&uiClient{t, srv.URL, "wrong"}).do("POST", "/api/columns", strings.NewReader("{}"), "application/json"); code != 403 {
		t.Fatalf("bad token: status %d, want 403", code)
	}

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, _ := mw.CreateFormFile("file", "messy.csv")
	data, _ := os.ReadFile("testdata/messy.csv")
	fw.Write(data)
	mw.Close()
	code, up := c.do("POST", "/api/upload", &body, mw.FormDataContentType())
	if code != 200 || up["name"] != "messy.csv" || !strings.HasPrefix(up["path"].(string), s.dir) {
		t.Fatalf("upload: %d %v", code, up)
	}
	src := up["path"].(string)

	cols := c.post("/api/columns", map[string]any{"source": src, "snake": true})
	if got := toStrings(cols["columns"]); len(got) != 7 || got[1] != "customer_name" {
		t.Fatalf("columns = %v", got)
	}

	id := c.post("/api/run", map[string]any{"cmd": "clean", "source": src, "name": "messy.csv", "snake": true, "dedupe": true, "format": "csv"})["id"].(string)
	st := c.wait(id)
	rep := st["report"].(map[string]any)
	if st["error"] != nil || rep["rows_in"] != 9.0 || rep["rows_duplicate_dropped"] != 1.0 || st["filename"] != "messy-clean.csv" {
		t.Fatalf("clean: %v", st)
	}
	pv := st["preview"].(map[string]any)
	if hdr := toStrings(pv["header"]); len(hdr) != 7 || hdr[0] != "invoice_no" || len(pv["rows"].([]any)) != 7 {
		t.Fatalf("preview: %v", pv)
	}
	dl, err := http.Get(srv.URL + st["download"].(string))
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(dl.Body)
	if !strings.HasPrefix(string(out), "invoice_no,customer_name,region,amount_kes,invoice_date,paid,email\n1001,Acme Ltd,Nairobi,1250,2025-01-15,true,") ||
		!strings.Contains(dl.Header.Get("Content-Disposition"), "messy-clean.csv") {
		t.Fatalf("download:\n%s\n%v", out, dl.Header)
	}

	id = c.post("/api/run", map[string]any{"cmd": "profile", "source": src, "name": "messy.csv", "top": 2})["id"].(string)
	st = c.wait(id)
	if text, _ := st["preview"].(map[string]any)["text"].(string); !strings.HasPrefix(text, "rows: 8") || st["filename"] != "messy-profile.txt" {
		t.Fatalf("profile: %v", st)
	}

	id = c.post("/api/run", map[string]any{
		"cmd": "query", "source": src, "name": "messy.csv", "format": "json",
		"where": []string{"[Amount (KES)] >= '1000'"}, "group": "Region", "agg": "count, sum(Amount (KES))", "sort": "-sum_amount_kes",
	})["id"].(string)
	st = c.wait(id)
	pv = st["preview"].(map[string]any)
	if st["error"] != nil || strings.Join(toStrings(pv["header"]), ",") != "Region,count,sum_amount_kes" || len(pv["rows"].([]any)) == 0 {
		t.Fatalf("query: %v", st)
	}

	id = c.post("/api/run", map[string]any{"cmd": "query", "source": src, "select": "zzz"})["id"].(string)
	if st = c.wait(id); st["error"] == nil || !strings.Contains(st["error"].(string), `"zzz"`) || st["download"] != nil {
		t.Fatalf("bad column: %v", st)
	}

	id = c.post("/api/run", map[string]any{"cmd": "watch", "source": "testdata/events.ndjson", "name": "events.ndjson", "every": "50ms", "agg": "count"})["id"].(string)
	time.Sleep(200 * time.Millisecond)
	if code, st := c.do("GET", "/api/job/"+id, nil, ""); code != 200 || st["done"] != false || st["text"] == nil {
		t.Fatalf("watch running: %d %v", code, st)
	}
	c.post("/api/job/"+id+"/cancel", map[string]any{})
	if st = c.wait(id); st["error"] != nil || st["filename"] != "events-watch.txt" {
		t.Fatalf("watch stopped: %v", st)
	}
}

func toStrings(v any) []string {
	list, _ := v.([]any)
	out := make([]string, len(list))
	for i, x := range list {
		out[i], _ = x.(string)
	}
	return out
}

func TestDownloadName(t *testing.T) {
	for in, want := range map[[4]string]string{
		{"sales.csv.gz", "", "clean", ".csv"}:                 "sales-clean.csv",
		{"", "https://x.org/a/b.json?x=1", "convert", ".csv"}: "b.csv",
		{"", `C:\data\book.xlsx`, "profile", ".txt"}:          "book-profile.txt",
		{"", "", "query", ".parquet"}:                         "datacom-query.parquet",
	} {
		if got := downloadName(in[0], in[1], in[2], in[3]); got != want {
			t.Errorf("downloadName(%q) = %q, want %q", in, got, want)
		}
	}
}
