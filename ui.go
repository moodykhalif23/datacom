package main

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/moodykhalif23/datacom/pipeline"
)

//go:embed ui.html
var uiHTML string

//go:embed ui-logo.png
var uiLogo []byte

const (
	uiPort      = "7878"
	previewRows = 50
)

// serveUI runs the point-and-click interface on addr until ctx is done.

func serveUI(ctx context.Context, addr string, openBrowser bool) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil && strings.HasSuffix(addr, ":"+uiPort) {
		ln, err = net.Listen("tcp", "127.0.0.1:0")
	}
	if err != nil {
		return err
	}
	s, err := newUIServer()
	if err != nil {
		ln.Close()
		return err
	}
	defer s.close()
	url := "http://" + ln.Addr().String()
	fmt.Fprintf(os.Stderr, "datacom: open %s in your browser (Ctrl-C stops it)\n", url)
	if openBrowser {
		browse(url)
	}
	srv := &http.Server{Handler: s.handler()}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func browse(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	cmd.Start()
}

type uiServer struct {
	dir   string // uploads and outputs; removed on close
	token string
	mu    sync.Mutex
	jobs  map[string]*job
}

func newUIServer() (*uiServer, error) {
	dir, err := os.MkdirTemp("", "datacom-ui-*")
	if err != nil {
		return nil, err
	}
	return &uiServer{dir: dir, token: randomID(16), jobs: map[string]*job{}}, nil
}

// close stops every job, waits for its files to be released, then removes
// the working folder.
func (s *uiServer) close() {
	s.mu.Lock()
	jobs := make([]*job, 0, len(s.jobs))
	for _, j := range s.jobs {
		j.cancel()
		jobs = append(jobs, j)
	}
	s.mu.Unlock()
	deadline := time.After(3 * time.Second)
	for _, j := range jobs {
		select {
		case <-j.done:
		case <-deadline:
		}
	}
	os.RemoveAll(s.dir)
}

func (s *uiServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.index)
	mux.HandleFunc("GET /logo.png", s.logo)
	mux.HandleFunc("POST /api/upload", s.upload)
	mux.HandleFunc("POST /api/columns", s.columns)
	mux.HandleFunc("POST /api/run", s.start)
	mux.HandleFunc("GET /api/job/{id}", s.status)
	mux.HandleFunc("POST /api/job/{id}/cancel", s.cancel)
	mux.HandleFunc("GET /api/job/{id}/download", s.download)
	mux.HandleFunc("GET /api/job/{id}/record", s.record)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if strings.HasPrefix(r.URL.Path, "/api/") &&
			r.Header.Get("X-Token") != s.token && r.URL.Query().Get("token") != s.token {
			fail(w, errors.New("this page is out of date; reload it"), http.StatusForbidden)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *uiServer) index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	full := versionString()
	short := strings.Fields(full)[1]
	if i := strings.Index(short, "-0."); i > 0 { // a pseudo-version from an untagged build
		short = short[:i] + "-dev"
	}
	page := strings.NewReplacer("{{TOKEN}}", s.token, "{{VERSION}}", full, "{{VERSION_SHORT}}", short).Replace(uiHTML)
	io.WriteString(w, page)
}

func (s *uiServer) logo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Cache-Control", "max-age=86400")
	w.Write(uiLogo)
}

// upload copies the browser's file into the working folder and returns its
// path. The name is kept because format detection reads it.
func (s *uiServer) upload(w http.ResponseWriter, r *http.Request) {
	mr, err := r.MultipartReader()
	if err != nil {
		fail(w, err, http.StatusBadRequest)
		return
	}
	for {
		part, err := mr.NextPart()
		if err != nil {
			fail(w, errors.New("no file was sent"), http.StatusBadRequest)
			return
		}
		if part.FormName() != "file" {
			continue
		}
		name := safeName(part.FileName())
		dst := filepath.Join(s.dir, "in-"+randomID(4)+"-"+name)
		f, err := os.Create(dst)
		if err != nil {
			fail(w, err, http.StatusInternalServerError)
			return
		}
		_, err = io.Copy(f, part)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			os.Remove(dst)
			fail(w, err, http.StatusInternalServerError)
			return
		}
		reply(w, map[string]string{"path": dst, "name": name})
		return
	}
}

// safeName reduces a browser-supplied file name to its base name with only
// portable characters, keeping the extension.
func safeName(name string) string {
	name = path.Base(strings.ReplaceAll(name, "\\", "/"))
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	name = b.String()
	if strings.Trim(name, "._") == "" {
		return "upload"
	}
	return name
}

type sourceSpec struct {
	Source string `json:"source"`
	Name   string `json:"name"`
	Input  string `json:"input"`
	Sheet  string `json:"sheet"`
}

// columns reads only the header of a source, named as Clean would name it,
// for the page's menus.
func (s *uiServer) columns(w http.ResponseWriter, r *http.Request) {
	var q struct {
		sourceSpec
		Snake bool `json:"snake"`
	}
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		fail(w, err, http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(q.Source) == "" {
		fail(w, errors.New("choose a file first"), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	t, closeIn, err := open(ctx, opts{inFormat: q.Input, sheet: q.Sheet}, nil, q.Source, false)
	if err != nil {
		fail(w, err, http.StatusBadRequest)
		return
	}
	defer closeIn()
	t, _ = pipeline.Clean(t, pipeline.CleanOptions{SnakeHeaders: q.Snake})
	if err := t.Err(); err != nil {
		fail(w, err, http.StatusBadRequest)
		return
	}
	hdr := t.Header
	cancel()
	for range t.Rows { // let every stage unwind before the source closes
	}
	reply(w, map[string]any{"columns": hdr})
}

type runRequest struct {
	sourceSpec
	Cmd       string   `json:"cmd"`
	Format    string   `json:"format"`
	Snake     bool     `json:"snake"`
	Dedupe    bool     `json:"dedupe"`
	KeepEmpty bool     `json:"keep_empty"`
	NoTypes   bool     `json:"no_types"`
	FoldCase  bool     `json:"fold_case"`
	Phone     string   `json:"phone"`
	Redact    []string `json:"redact"`
	Join      string   `json:"join"`
	On        string   `json:"on"`
	Derive    []string `json:"derive"`
	Where     []string `json:"where"`
	Select    string   `json:"select"`
	Group     string   `json:"group"`
	Agg       string   `json:"agg"`
	Sort      string   `json:"sort"`
	Limit     int      `json:"limit"`
	Top       int      `json:"top"`
	JSON      bool     `json:"json"`
	Every     string   `json:"every"`
	Idle      string   `json:"idle"`
}

// job is one run started from the page. The page polls it for progress and,
// once done, for the report, a preview and the file to download.
type job struct {
	manifest string
	id       string
	cmd      string
	out      string
	filename string
	format   pipeline.Format
	mon      *pipeline.Monitor
	start    time.Time
	cancel   context.CancelFunc
	done     chan struct{}

	mu      sync.Mutex
	err     error
	report  pipeline.RunReport
	preview preview
}

type preview struct {
	Header []string   `json:"header,omitempty"`
	Rows   [][]string `json:"rows,omitempty"`
	Text   string     `json:"text,omitempty"`
}

func (s *uiServer) start(w http.ResponseWriter, r *http.Request) {
	var q runRequest
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		fail(w, err, http.StatusBadRequest)
		return
	}
	switch q.Cmd {
	case "profile", "clean", "query", "convert", "watch":
	default:
		fail(w, fmt.Errorf("unknown action %q", q.Cmd), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(q.Source) == "" {
		fail(w, errors.New("choose a file first"), http.StatusBadRequest)
		return
	}
	every := 10 * time.Second
	if q.Every != "" {
		d, err := time.ParseDuration(q.Every)
		if err != nil || d <= 0 {
			fail(w, errors.New("the window length should look like 10s, 1m or 2m30s"), http.StatusBadRequest)
			return
		}
		every = d
	}
	var idle time.Duration
	if q.Idle != "" {
		d, err := time.ParseDuration(q.Idle)
		if err != nil || d < 0 {
			fail(w, errors.New("the finish-after time should look like 30s or 2m, or be left blank"), http.StatusBadRequest)
			return
		}
		idle = d
	}
	format, ext := pipeline.Format(q.Format), ""
	switch q.Cmd {
	case "profile":
		format, ext = "", ".txt"
		if q.JSON {
			ext = ".json"
		}
	case "watch":
		format, ext = pipeline.FormatTable, ".txt"
	default:
		if format == "" {
			format = pipeline.FormatCSV
		}
		ext = "." + string(format)
	}
	id := randomID(8)
	j := &job{
		id: id, cmd: q.Cmd, format: format, mon: pipeline.NewMonitor(), start: time.Now(),
		out:      filepath.Join(s.dir, "out-"+id+ext),
		manifest: filepath.Join(s.dir, "run-"+id+".json"),
		filename: downloadName(q.Name, q.Source, q.Cmd, ext),
		done:     make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	j.cancel = cancel
	o := opts{
		in: q.Source, out: j.out, format: string(format), inFormat: q.Input, sheet: q.Sheet,
		quiet: true, snake: q.Snake, dedupe: q.Dedupe, keepEmpty: q.KeepEmpty, noTypes: q.NoTypes,
		foldCase: q.FoldCase, phone: q.Phone, redact: strings.Join(q.Redact, ","), manifest: j.manifest,
		join: q.Join, on: q.On, derive: q.Derive, where: q.Where, selectCols: q.Select,
		group: q.Group, agg: q.Agg, sortKey: q.Sort, limit: q.Limit, top: q.Top, jsonReport: q.JSON,
		every: every, idle: idle, mon: j.mon,
		onReport: func(rep pipeline.RunReport) {
			j.mu.Lock()
			j.report = rep
			j.mu.Unlock()
		},
	}
	s.mu.Lock()
	s.jobs[id] = j
	s.mu.Unlock()
	go j.run(ctx, o)
	reply(w, map[string]string{"id": id})
}

func (j *job) run(ctx context.Context, o opts) {
	defer close(j.done)
	err := run(ctx, j.cmd, o)
	var pv preview
	if err == nil {
		pv, err = j.makePreview()
	}
	j.mu.Lock()
	j.err, j.preview = err, pv
	j.mu.Unlock()
}

// makePreview reads the output back through the same readers the tool uses,
// so Excel and Parquet results preview as faithfully as CSV. Formats with no
// reader (Markdown, HTML, SQL) are shown as the text that was written.
func (j *job) makePreview() (preview, error) {
	switch {
	case j.cmd == "profile", j.cmd == "watch",
		j.format == pipeline.FormatMarkdown, j.format == pipeline.FormatHTML, j.format == pipeline.FormatSQL:
		b, err := readTail(j.out, 1<<20)
		return preview{Text: string(b)}, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, f, closeFn, err := pipeline.Open(ctx, j.out, j.format)
	if err != nil {
		return preview{}, err
	}
	defer closeFn()
	t := pipeline.Read(ctx, r, f)
	pv := preview{Header: t.Header}
	for row := range t.Rows {
		pv.Rows = append(pv.Rows, row)
		if len(pv.Rows) == previewRows {
			cancel()
			break
		}
	}
	for range t.Rows {
	}
	return pv, t.Err()
}

func (s *uiServer) lookup(w http.ResponseWriter, r *http.Request) *job {
	s.mu.Lock()
	j := s.jobs[r.PathValue("id")]
	s.mu.Unlock()
	if j == nil {
		fail(w, errors.New("that run is no longer available"), http.StatusNotFound)
	}
	return j
}

func (s *uiServer) status(w http.ResponseWriter, r *http.Request) {
	j := s.lookup(w, r)
	if j == nil {
		return
	}
	done := false
	select {
	case <-j.done:
		done = true
	default:
	}
	resp := map[string]any{
		"done":    done,
		"rows_in": j.mon.RowsIn.Load(),
		"bytes":   j.mon.Bytes.Load(),
		"seconds": time.Since(j.start).Seconds(),
	}
	if !done {
		if j.cmd == "watch" {
			b, _ := readTail(j.out, 64<<10)
			resp["text"] = string(b)
		}
		reply(w, resp)
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	resp["seconds"] = j.report.Seconds
	resp["report"] = j.report
	resp["preview"] = j.preview
	if _, err := os.Stat(j.manifest); err == nil {
		resp["record"] = "/api/job/" + j.id + "/record?token=" + s.token
		resp["record_name"] = strings.TrimSuffix(j.filename, filepath.Ext(j.filename)) + "-record.json"
	}
	if j.err != nil {
		resp["error"] = j.err.Error()
	} else {
		resp["download"] = "/api/job/" + j.id + "/download?token=" + s.token
		resp["filename"] = j.filename
	}
	reply(w, resp)
}

func (s *uiServer) cancel(w http.ResponseWriter, r *http.Request) {
	if j := s.lookup(w, r); j != nil {
		j.cancel()
		reply(w, map[string]bool{"ok": true})
	}
}

func (s *uiServer) download(w http.ResponseWriter, r *http.Request) {
	j := s.lookup(w, r)
	if j == nil {
		return
	}
	select {
	case <-j.done:
	default:
		fail(w, errors.New("still running"), http.StatusConflict)
		return
	}
	j.mu.Lock()
	err := j.err
	j.mu.Unlock()
	if err != nil {
		fail(w, err, http.StatusConflict)
		return
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", j.filename))
	http.ServeFile(w, r, j.out)
}

// record serves the run manifest: checksums, rules applied and counts.
func (s *uiServer) record(w http.ResponseWriter, r *http.Request) {
	j := s.lookup(w, r)
	if j == nil {
		return
	}
	select {
	case <-j.done:
	default:
		fail(w, errors.New("still running"), http.StatusConflict)
		return
	}
	name := strings.TrimSuffix(j.filename, filepath.Ext(j.filename)) + "-record.json"
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name))
	w.Header().Set("Content-Type", "application/json")
	http.ServeFile(w, r, j.manifest)
}

// downloadName turns sales.csv.gz plus the action into sales-clean.csv.
func downloadName(name, source, cmd, ext string) string {
	if name == "" {
		name = path.Base(strings.SplitN(strings.ReplaceAll(source, "\\", "/"), "?", 2)[0])
	}
	stem := strings.TrimSuffix(name, ".gz")
	stem = strings.TrimSuffix(stem, path.Ext(stem))
	if stem == "" || stem == "." {
		stem = "datacom"
	}
	if cmd != "convert" {
		stem += "-" + cmd
	}
	return stem + ext
}

// readTail reads the last n bytes of path, cut at a line boundary when the
// file is longer.
func readTail(path string, n int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	off := max(st.Size()-n, 0)
	b := make([]byte, st.Size()-off)
	if _, err := f.ReadAt(b, off); err != nil && err != io.EOF {
		return nil, err
	}
	if off > 0 {
		if i := strings.IndexByte(string(b), '\n'); i >= 0 {
			b = b[i+1:]
		}
	}
	return b, nil
}

func randomID(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func reply(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, err error, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}
