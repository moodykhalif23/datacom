package pipeline

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestSigV4 checks the signer against the worked example in the AWS docs
// ("Signature Calculations for the Authorization Header", GET Object).
func TestSigV4(t *testing.T) {
	req, _ := http.NewRequest("GET", "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	req.Header.Set("Range", "bytes=0-9")
	signS3(req, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "", "us-east-1",
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC))
	want := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
	if got := req.Header.Get("Authorization"); got != want {
		t.Fatalf("Authorization =\n%s\nwant\n%s", got, want)
	}
}

func TestGlobMatch(t *testing.T) {
	for _, c := range []struct {
		p, name string
		want    bool
	}{
		{"data/*.csv", "data/a.csv", true}, {"data/*.csv", "data/sub/a.csv", false},
		{"data/**/*.csv", "data/a.csv", true}, {"data/**/*.csv", "data/x/y/a.csv", true},
		{"data/**", "data/x/y", true}, {"**/*.gz", "a/b.jsonl.gz", true}, {"data/**/*.csv", "other/a.csv", false},
		{"part-[0-9]*.parquet", "part-7.parquet", true},
	} {
		if globMatch(c.p, c.name) != c.want {
			t.Errorf("globMatch(%q, %q) = %v", c.p, c.name, !c.want)
		}
	}
}

func TestLocalGlob(t *testing.T) {
	dir := t.TempDir()
	for _, f := range []string{"a.csv", "b.csv", "sub/c.csv", "sub/deep/d.csv", "sub/e.txt"} {
		os.MkdirAll(filepath.Join(dir, filepath.Dir(f)), 0o755)
		os.WriteFile(filepath.Join(dir, f), []byte("x\n1\n"), 0o644)
	}
	got, err := ExpandSources(context.Background(), []string{filepath.Join(dir, "**", "*.csv"), filepath.Join(dir, "a.csv")})
	if err != nil {
		t.Fatal(err)
	}
	var rel []string
	for _, g := range got {
		r, _ := filepath.Rel(dir, g)
		rel = append(rel, filepath.ToSlash(r))
	}
	if strings.Join(rel, ",") != "a.csv,b.csv,sub/c.csv,sub/deep/d.csv,a.csv" {
		t.Fatalf("expanded %v", rel)
	}
	if _, err := ExpandSources(context.Background(), []string{filepath.Join(dir, "*.parquet")}); err == nil || !strings.Contains(err.Error(), "no files match") {
		t.Fatalf("err = %v", err)
	}
}

// fakeS3 serves objects path-style, paginating listings two keys at a time
// and recording whether each request was signed.
type fakeS3 struct {
	objects map[string]string
	mu      sync.Mutex
	auth    []string
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.auth = append(f.auth, r.Header.Get("Authorization"))
	f.mu.Unlock()
	bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if r.URL.Query().Get("list-type") == "2" {
		var keys []string
		for k := range f.objects {
			if strings.HasPrefix(k, bucket+"/"+r.URL.Query().Get("prefix")) {
				keys = append(keys, strings.TrimPrefix(k, bucket+"/"))
			}
		}
		sort.Strings(keys)
		start, _ := strconv.Atoi(r.URL.Query().Get("continuation-token"))
		end := min(start+2, len(keys))
		fmt.Fprint(w, "<ListBucketResult>")
		for _, k := range keys[start:end] {
			fmt.Fprintf(w, "<Contents><Key>%s</Key><Size>%d</Size></Contents>", k, len(f.objects[bucket+"/"+k]))
		}
		if end < len(keys) {
			fmt.Fprintf(w, "<IsTruncated>true</IsTruncated><NextContinuationToken>%d</NextContinuationToken>", end)
		}
		fmt.Fprint(w, "</ListBucketResult>")
		return
	}
	body, ok := f.objects[bucket+"/"+key]
	if !ok {
		http.NotFound(w, r)
		return
	}
	http.ServeContent(w, r, key, time.Time{}, strings.NewReader(body))
}

func readAll(t *testing.T, src string) []string {
	t.Helper()
	r, f, closeFn, err := Open(context.Background(), src, FormatAuto)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()
	tb := Read(context.Background(), r, f)
	rows := collect(tb)
	if err := tb.Err(); err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestS3(t *testing.T) {
	s3 := &fakeS3{objects: map[string]string{
		"lake/data/a.csv": "id,v\n1,x\n", "lake/data/b.csv": "id,v\n2,y\n", "lake/data/sub/c.csv": "id,v\n3,z\n",
		"lake/data/notes.txt": "skip", "lake/data/d e.csv": "id,v\n4,w\n",
	}}
	srv := httptest.NewServer(s3)
	defer srv.Close()
	t.Setenv("AWS_ENDPOINT_URL", srv.URL)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDTEST")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	got, err := ExpandSources(context.Background(), []string{"s3://lake/data/*.csv"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "s3://lake/data/a.csv,s3://lake/data/b.csv,s3://lake/data/d e.csv" {
		t.Fatalf("expanded %v", got)
	}
	var rows []string
	for _, src := range got {
		rows = append(rows, readAll(t, src)...)
	}
	if strings.Join(rows, ";") != "1,x;2,y;4,w" {
		t.Fatalf("rows %v", rows)
	}
	for _, a := range s3.auth {
		if !strings.HasPrefix(a, "AWS4-HMAC-SHA256 Credential=AKIDTEST/") || !strings.Contains(a, "Signature=") {
			t.Fatalf("unsigned or malformed request: %q", a)
		}
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "none"))
	s3.auth = nil
	readAll(t, "s3://lake/data/a.csv")
	if s3.auth[0] != "" {
		t.Fatalf("anonymous request was signed: %q", s3.auth[0])
	}
	creds := filepath.Join(t.TempDir(), "credentials")
	os.WriteFile(creds, []byte("[default]\naws_access_key_id = WRONG\n[ci]\naws_access_key_id = AKIDFILE\naws_secret_access_key = s\n"), 0o600)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", creds)
	t.Setenv("AWS_PROFILE", "ci")
	s3.auth = nil
	readAll(t, "s3://lake/data/b.csv")
	if !strings.Contains(s3.auth[0], "Credential=AKIDFILE/") {
		t.Fatalf("credentials file not used: %q", s3.auth[0])
	}
	if _, err := ExpandSources(context.Background(), []string{"s3://lake/data/*.parquet"}); err == nil {
		t.Fatal("empty match must fail")
	}
}

func TestGCS(t *testing.T) {
	objects := map[string]string{"logs/2025/a.jsonl": `{"n":1}` + "\n", "logs/2025/b.jsonl": `{"n":2}` + "\n", "logs/2024/c.jsonl": `{"n":3}` + "\n"}
	var mu sync.Mutex
	var auth []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		auth = append(auth, r.Header.Get("Authorization"))
		mu.Unlock()
		rest := strings.TrimPrefix(r.URL.EscapedPath(), "/storage/v1/b/bkt/o")
		if rest == "" {
			var names []string
			for k := range objects {
				if strings.HasPrefix(k, r.URL.Query().Get("prefix")) {
					names = append(names, k)
				}
			}
			sort.Strings(names)
			page := r.URL.Query().Get("pageToken")
			if page == "" && len(names) > 1 {
				fmt.Fprintf(w, `{"items":[{"name":%q,"size":"8"}],"nextPageToken":"p2"}`, names[0])
				return
			}
			if page == "p2" {
				names = names[1:]
			}
			fmt.Fprint(w, `{"items":[`)
			for i, n := range names {
				if i > 0 {
					fmt.Fprint(w, ",")
				}
				fmt.Fprintf(w, `{"name":%q,"size":"8"}`, n)
			}
			fmt.Fprint(w, `]}`)
			return
		}
		name, _ := url.PathUnescape(strings.TrimPrefix(rest, "/"))
		if r.URL.Query().Get("alt") != "media" || objects[name] == "" {
			http.NotFound(w, r)
			return
		}
		io.WriteString(w, objects[name])
	}))
	defer srv.Close()
	t.Setenv("STORAGE_EMULATOR_HOST", strings.TrimPrefix(srv.URL, "http://"))
	t.Setenv("GOOGLE_OAUTH_ACCESS_TOKEN", "ya29.test")
	got, err := ExpandSources(context.Background(), []string{"gs://bkt/logs/**/*.jsonl"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "gs://bkt/logs/2024/c.jsonl,gs://bkt/logs/2025/a.jsonl,gs://bkt/logs/2025/b.jsonl" {
		t.Fatalf("expanded %v", got)
	}
	if rows := readAll(t, "gs://bkt/logs/2025/b.jsonl"); strings.Join(rows, ";") != "2" {
		t.Fatalf("rows %v", rows)
	}
	for _, a := range auth {
		if a != "Bearer ya29.test" {
			t.Fatalf("auth %q", a)
		}
	}
}

func TestHuggingFace(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.URL.Path+" "+r.Header.Get("Authorization"))
		mu.Unlock()
		switch {
		case r.URL.Path == "/api/datasets/org/corpus/tree/main/data" && r.URL.Query().Get("cursor") == "":
			w.Header().Set("Link", fmt.Sprintf(`<http://%s/api/datasets/org/corpus/tree/main/data?recursive=true&cursor=2>; rel="next"`, r.Host))
			fmt.Fprint(w, `[{"type":"directory","path":"data/train"},{"type":"file","path":"data/train/a.jsonl","size":9}]`)
		case r.URL.Path == "/api/datasets/org/corpus/tree/main/data":
			fmt.Fprint(w, `[{"type":"file","path":"data/train/b.jsonl","size":9},{"type":"file","path":"data/README.md","size":3}]`)
		case strings.HasPrefix(r.URL.Path, "/datasets/org/corpus/resolve/main/"):
			http.Redirect(w, r, "/cdn/"+strings.TrimPrefix(r.URL.Path, "/datasets/org/corpus/resolve/main/"), http.StatusFound)
		case r.URL.Path == "/cdn/data/train/a.jsonl":
			io.WriteString(w, `{"t":"a"}`+"\n")
		case r.URL.Path == "/cdn/data/train/b.jsonl":
			io.WriteString(w, `{"t":"b"}`+"\n")
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	t.Setenv("HF_ENDPOINT", srv.URL)
	t.Setenv("HF_TOKEN", "hf_test")
	got, err := ExpandSources(context.Background(), []string{"hf://datasets/org/corpus/data/**/*.jsonl"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "hf://datasets/org/corpus/data/train/a.jsonl,hf://datasets/org/corpus/data/train/b.jsonl" {
		t.Fatalf("expanded %v", got)
	}
	if rows := readAll(t, got[1]); strings.Join(rows, ";") != "b" {
		t.Fatalf("rows %v", rows)
	}
	for _, s := range seen {
		if strings.HasPrefix(s, "/api/") && !strings.HasSuffix(s, "Bearer hf_test") {
			t.Fatalf("listing without token: %s", s)
		}
	}
	if r, err := parseHF("hf://datasets/org/corpus@v2/x/y.parquet"); err != nil || r.rev != "v2" || r.id != "org/corpus" || r.path != "x/y.parquet" {
		t.Fatalf("parseHF = %+v %v", r, err)
	}
	if _, err := parseHF("hf://org/corpus/x"); err == nil {
		t.Fatal("paths without datasets/ or models/ must fail")
	}
}
