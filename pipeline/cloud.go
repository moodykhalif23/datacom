package pipeline

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// IsCloud reports whether src names an object in S3, Google Cloud Storage
// or the Hugging Face Hub.
func IsCloud(src string) bool {
	return strings.HasPrefix(src, "s3://") || strings.HasPrefix(src, "gs://") || strings.HasPrefix(src, "hf://")
}

// object is one listed file: its full source path and size.
type object struct {
	src  string
	size int64
}

// cloudURL turns a cloud path into the HTTPS URL that serves it and a
// function that authorizes requests to it.
func cloudURL(src string) (string, func(*http.Request), error) {
	switch {
	case strings.HasPrefix(src, "s3://"):
		bucket, key, _ := strings.Cut(strings.TrimPrefix(src, "s3://"), "/")
		c := s3Config()
		return c.objectURL(bucket, key), c.sign, nil
	case strings.HasPrefix(src, "gs://"):
		bucket, key, _ := strings.Cut(strings.TrimPrefix(src, "gs://"), "/")
		return gcsEndpoint() + "/storage/v1/b/" + url.PathEscape(bucket) + "/o/" + url.PathEscape(key) + "?alt=media", bearer(gcsToken()), nil
	case strings.HasPrefix(src, "hf://"):
		repo, err := parseHF(src)
		if err != nil {
			return "", nil, err
		}
		return repo.fileURL(repo.path), bearer(hfToken()), nil
	}
	return "", nil, fmt.Errorf("not a cloud path: %s", src)
}

func bearer(token string) func(*http.Request) {
	if token == "" {
		return nil
	}
	return func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) }
}

func escapePath(p string) string {
	parts := strings.Split(p, "/")
	for i, s := range parts {
		parts[i] = url.PathEscape(s)
	}
	return strings.Join(parts, "/")
}

// S3.

type s3 struct {
	endpoint, region   string
	key, secret, token string
	pathStyle          bool
	now                func() time.Time
}

// s3Config reads the standard AWS settings: AWS_ENDPOINT_URL_S3 or
// AWS_ENDPOINT_URL for MinIO and other S3-compatible stores, AWS_REGION,
// and credentials from the environment or ~/.aws/credentials. Without
// credentials requests go unsigned, which public buckets accept.
func s3Config() *s3 {
	c := &s3{region: firstEnv("AWS_REGION", "AWS_DEFAULT_REGION"), now: time.Now}
	if c.region == "" {
		c.region = "us-east-1"
	}
	if e := firstEnv("AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL"); e != "" {
		c.endpoint, c.pathStyle = strings.TrimRight(e, "/"), true
	}
	c.key, c.secret, c.token = os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), os.Getenv("AWS_SESSION_TOKEN")
	if c.key == "" {
		c.key, c.secret, c.token = sharedCredentials()
	}
	return c
}

func firstEnv(names ...string) string {
	for _, n := range names {
		if v := os.Getenv(n); v != "" {
			return v
		}
	}
	return ""
}

func sharedCredentials() (key, secret, token string) {
	file := os.Getenv("AWS_SHARED_CREDENTIALS_FILE")
	if file == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return
		}
		file = filepath.Join(home, ".aws", "credentials")
	}
	f, err := os.Open(file)
	if err != nil {
		return
	}
	defer f.Close()
	profile := firstEnv("AWS_PROFILE")
	if profile == "" {
		profile = "default"
	}
	in := false
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(line, "[") {
			in = strings.Trim(line, "[] ") == profile
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !in || !ok {
			continue
		}
		switch strings.TrimSpace(k) {
		case "aws_access_key_id":
			key = strings.TrimSpace(v)
		case "aws_secret_access_key":
			secret = strings.TrimSpace(v)
		case "aws_session_token":
			token = strings.TrimSpace(v)
		}
	}
	if sc.Err() != nil {
		return "", "", ""
	}
	return
}

func (c *s3) bucketURL(bucket string) string {
	if c.pathStyle {
		return c.endpoint + "/" + url.PathEscape(bucket)
	}
	return "https://" + bucket + ".s3." + c.region + ".amazonaws.com"
}

func (c *s3) objectURL(bucket, key string) string {
	return c.bucketURL(bucket) + "/" + escapePath(key)
}

func (c *s3) sign(r *http.Request) {
	if c.key == "" {
		return
	}
	signS3(r, c.key, c.secret, c.token, c.region, "UNSIGNED-PAYLOAD", c.now())
}

// signS3 adds an AWS Signature Version 4 Authorization header to r.
func signS3(r *http.Request, key, secret, token, region, payload string, t time.Time) {
	amzDate := t.UTC().Format("20060102T150405Z")
	day := amzDate[:8]
	r.Header.Set("x-amz-date", amzDate)
	r.Header.Set("x-amz-content-sha256", payload)
	if token != "" {
		r.Header.Set("x-amz-security-token", token)
	}
	headers := map[string]string{"host": r.URL.Host}
	for k, v := range r.Header {
		lk := strings.ToLower(k)
		if lk == "range" || strings.HasPrefix(lk, "x-amz-") {
			headers[lk] = strings.TrimSpace(strings.Join(v, ","))
		}
	}
	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, k)
	}
	sort.Strings(names)
	var canon strings.Builder
	for _, k := range names {
		canon.WriteString(k)
		canon.WriteByte(':')
		canon.WriteString(headers[k])
		canon.WriteByte('\n')
	}
	signed := strings.Join(names, ";")
	q := r.URL.Query()
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var query []string
	for _, k := range keys {
		for _, v := range q[k] {
			query = append(query, awsEscape(k)+"="+awsEscape(v))
		}
	}
	request := strings.Join([]string{r.Method, r.URL.EscapedPath(), strings.Join(query, "&"), canon.String(), signed, payload}, "\n")
	scope := day + "/" + region + "/s3/aws4_request"
	sum := sha256.Sum256([]byte(request))
	toSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(sum[:])
	k := hmacSHA256([]byte("AWS4"+secret), day)
	for _, part := range []string{region, "s3", "aws4_request"} {
		k = hmacSHA256(k, part)
	}
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+key+"/"+scope+", SignedHeaders="+signed+", Signature="+hex.EncodeToString(hmacSHA256(k, toSign)))
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

func awsEscape(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}

func (c *s3) list(ctx context.Context, bucket, prefix string) ([]object, error) {
	var out []object
	token := ""
	for {
		q := url.Values{"list-type": {"2"}, "prefix": {prefix}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		var page struct {
			Contents []struct {
				Key  string
				Size int64
			}
			IsTruncated           bool
			NextContinuationToken string
		}
		if err := getDoc(ctx, c.bucketURL(bucket)+"/?"+q.Encode(), c.sign, func(r io.Reader) error { return xml.NewDecoder(r).Decode(&page) }); err != nil {
			return nil, err
		}
		for _, o := range page.Contents {
			out = append(out, object{"s3://" + bucket + "/" + o.Key, o.Size})
		}
		if !page.IsTruncated || page.NextContinuationToken == "" {
			return out, nil
		}
		token = page.NextContinuationToken
	}
}

// Google Cloud Storage.

// gcsEndpoint honours STORAGE_EMULATOR_HOST, as Google's own clients do.
func gcsEndpoint() string {
	e := os.Getenv("STORAGE_EMULATOR_HOST")
	if e == "" {
		return "https://storage.googleapis.com"
	}
	if !strings.Contains(e, "://") {
		e = "http://" + e
	}
	return strings.TrimRight(e, "/")
}

// gcsToken is an OAuth access token, for example from
// "gcloud auth print-access-token". Public buckets need none.
func gcsToken() string { return firstEnv("GOOGLE_OAUTH_ACCESS_TOKEN", "GCS_ACCESS_TOKEN") }

func gcsList(ctx context.Context, bucket, prefix string) ([]object, error) {
	var out []object
	token := ""
	for {
		q := url.Values{"prefix": {prefix}, "fields": {"items(name,size),nextPageToken"}}
		if token != "" {
			q.Set("pageToken", token)
		}
		var page struct {
			Items []struct {
				Name string
				Size json.Number
			}
			NextPageToken string
		}
		u := gcsEndpoint() + "/storage/v1/b/" + url.PathEscape(bucket) + "/o?" + q.Encode()
		if err := getDoc(ctx, u, bearer(gcsToken()), func(r io.Reader) error { return json.NewDecoder(r).Decode(&page) }); err != nil {
			return nil, err
		}
		for _, o := range page.Items {
			n, _ := o.Size.Int64()
			out = append(out, object{"gs://" + bucket + "/" + o.Name, n})
		}
		if page.NextPageToken == "" {
			return out, nil
		}
		token = page.NextPageToken
	}
}

// Hugging Face Hub.

type hfRepo struct {
	kind, id, rev, path string
}

// parseHF reads hf://datasets/owner/name[@revision]/path, and the same with
// models/ or spaces/.
func parseHF(src string) (hfRepo, error) {
	parts := strings.SplitN(strings.TrimPrefix(src, "hf://"), "/", 4)
	if len(parts) < 3 || parts[0] != "datasets" && parts[0] != "models" && parts[0] != "spaces" {
		return hfRepo{}, fmt.Errorf("hugging face paths look like hf://datasets/owner/name/path, got %s", src)
	}
	r := hfRepo{kind: parts[0], id: parts[1] + "/" + parts[2], rev: "main"}
	if name, rev, ok := strings.Cut(parts[2], "@"); ok {
		r.id, r.rev = parts[1]+"/"+name, rev
	}
	if len(parts) == 4 {
		r.path = parts[3]
	}
	return r, nil
}

func hfEndpoint() string {
	if e := os.Getenv("HF_ENDPOINT"); e != "" {
		return strings.TrimRight(e, "/")
	}
	return "https://huggingface.co"
}

func hfToken() string { return firstEnv("HF_TOKEN", "HUGGING_FACE_HUB_TOKEN") }

func (r hfRepo) fileURL(p string) string {
	prefix := hfEndpoint() + "/"
	if r.kind != "models" {
		prefix += r.kind + "/"
	}
	return prefix + r.id + "/resolve/" + url.PathEscape(r.rev) + "/" + escapePath(p)
}

func (r hfRepo) src(p string) string {
	id := r.id
	if r.rev != "main" {
		id += "@" + r.rev
	}
	return "hf://" + r.kind + "/" + id + "/" + p
}

func hfList(ctx context.Context, r hfRepo, dir string) ([]object, error) {
	next := hfEndpoint() + "/api/" + r.kind + "/" + r.id + "/tree/" + url.PathEscape(r.rev)
	if dir != "" {
		next += "/" + escapePath(dir)
	}
	next += "?recursive=true"
	var out []object
	for next != "" {
		var page []struct {
			Type string
			Path string
			Size int64
		}
		link := ""
		err := getDoc(ctx, next, bearer(hfToken()), func(body io.Reader) error { return json.NewDecoder(body).Decode(&page) }, func(h http.Header) { link = h.Get("Link") })
		if err != nil {
			return nil, err
		}
		for _, f := range page {
			if f.Type == "file" {
				out = append(out, object{r.src(f.Path), f.Size})
			}
		}
		next = nextLink(link)
	}
	return out, nil
}

func nextLink(h string) string {
	for _, part := range strings.Split(h, ",") {
		if strings.Contains(part, `rel="next"`) {
			if i, j := strings.Index(part, "<"), strings.Index(part, ">"); i >= 0 && j > i {
				return part[i+1 : j]
			}
		}
	}
	return ""
}

func getDoc(ctx context.Context, u string, sign func(*http.Request), decode func(io.Reader) error, headers ...func(http.Header)) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	if sign != nil {
		sign(req)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return fmt.Errorf("listing %s: %s %s", u, resp.Status, strings.TrimSpace(string(b)))
	}
	for _, h := range headers {
		h(resp.Header)
	}
	return decode(resp.Body)
}

// Globs.

func hasGlob(s string) bool { return strings.ContainsAny(s, "*?[") }

// globMatch is path.Match with ** also matching any number of directories.
func globMatch(pattern, name string) bool {
	if !strings.Contains(pattern, "**") {
		ok, _ := path.Match(pattern, name)
		return ok
	}
	pp, np := strings.Split(pattern, "/"), strings.Split(name, "/")
	var match func(i, j int) bool
	match = func(i, j int) bool {
		for i < len(pp) {
			if pp[i] == "**" {
				for k := j; k <= len(np); k++ {
					if match(i+1, k) {
						return true
					}
				}
				return false
			}
			if j >= len(np) {
				return false
			}
			if ok, _ := path.Match(pp[i], np[j]); !ok {
				return false
			}
			i, j = i+1, j+1
		}
		return j == len(np)
	}
	return match(0, 0)
}

// ExpandSources replaces each pattern holding *, ? or [ with the files it
// matches, local or in the cloud, sorted by name. Other sources pass through.
func ExpandSources(ctx context.Context, patterns []string) ([]string, error) {
	var out []string
	for _, p := range patterns {
		if !hasGlob(p) {
			out = append(out, p)
			continue
		}
		matches, err := expand(ctx, p)
		if err != nil {
			return nil, err
		}
		if len(matches) == 0 {
			return nil, fmt.Errorf("no files match %s", p)
		}
		out = append(out, matches...)
	}
	return out, nil
}

func expand(ctx context.Context, p string) ([]string, error) {
	var objs []object
	var err error
	switch {
	case strings.HasPrefix(p, "s3://"):
		bucket, key, _ := strings.Cut(strings.TrimPrefix(p, "s3://"), "/")
		if objs, err = s3Config().list(ctx, bucket, staticPrefix(key)); err == nil {
			objs = keep(objs, func(o object) string { return strings.TrimPrefix(o.src, "s3://"+bucket+"/") }, key)
		}
	case strings.HasPrefix(p, "gs://"):
		bucket, key, _ := strings.Cut(strings.TrimPrefix(p, "gs://"), "/")
		if objs, err = gcsList(ctx, bucket, staticPrefix(key)); err == nil {
			objs = keep(objs, func(o object) string { return strings.TrimPrefix(o.src, "gs://"+bucket+"/") }, key)
		}
	case strings.HasPrefix(p, "hf://"):
		var r hfRepo
		if r, err = parseHF(p); err == nil {
			dir := staticPrefix(r.path)
			if i := strings.LastIndexByte(dir, '/'); i >= 0 {
				dir = dir[:i]
			} else {
				dir = ""
			}
			prefix := r.src("")
			if objs, err = hfList(ctx, r, dir); err == nil {
				objs = keep(objs, func(o object) string { return strings.TrimPrefix(o.src, prefix) }, r.path)
			}
		}
	default:
		return localGlob(p)
	}
	if err != nil {
		return nil, err
	}
	out := make([]string, len(objs))
	for i, o := range objs {
		out[i] = o.src
	}
	sort.Strings(out)
	return out, nil
}

func staticPrefix(key string) string {
	if i := strings.IndexAny(key, "*?["); i >= 0 {
		return key[:i]
	}
	return key
}

func keep(objs []object, rel func(object) string, pattern string) []object {
	var out []object
	for _, o := range objs {
		if r := rel(o); !strings.HasSuffix(r, "/") && globMatch(pattern, r) {
			out = append(out, o)
		}
	}
	return out
}

func localGlob(p string) ([]string, error) {
	p = filepath.ToSlash(p)
	if !strings.Contains(p, "**") {
		m, err := filepath.Glob(filepath.FromSlash(p))
		sort.Strings(m)
		return m, err
	}
	root := staticPrefix(p)
	if i := strings.LastIndexByte(root, '/'); i >= 0 {
		root = root[:i]
	} else {
		root = "."
	}
	var out []string
	err := filepath.WalkDir(filepath.FromSlash(root), func(f string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if s := filepath.ToSlash(f); !d.IsDir() && globMatch(p, strings.TrimPrefix(s, "./")) {
			out = append(out, f)
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}
