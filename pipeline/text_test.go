package pipeline

import (
	"context"
	"strings"
	"testing"
)

func TestCleanText(t *testing.T) {
	cases := []struct {
		in, want string
		change   TextChange
	}{
		{"CafÃ© au lait", "Café au lait", TextMojibake},
		{"donâ€™t â€œquoteâ€\u009d", "don’t “quote”", TextMojibake},
		{"CafÃƒÂ© twice", "Café twice", TextMojibake},
		{"ÐŸÑ€Ð¸Ð²ÐµÑ‚", "Привет", TextMojibake},
		{"caf\xe9 \x93ok\x94", "café “ok”", TextEncoding},
		{"<p>Hello <b>world</b></p><script>track()</script><p>Bye&nbsp;now</p>", "Hello world\nBye now", TextHTML},
		{"Tom &amp; Jerry &#39;99", "Tom & Jerry '99", TextHTML},
		{"été", "été", TextUnicode},
		{"a\x00b​c\r\nd e", "abc\nd e", TextControl},
		{"  one   two  \n\n\n\n  three  ", "one two\n\n  three", TextSpace},
		{"def f():\n    return  1", "def f():\n    return 1", TextSpace},
		{"a < b and c > d", "a < b and c > d", 0},
		{"Crème brûlée, naïve café", "Crème brûlée, naïve café", 0},
		{"Already clean.\n\nTwo paragraphs.", "Already clean.\n\nTwo paragraphs.", 0},
	}
	for _, c := range cases {
		got, change := CleanText(c.in)
		if got != c.want {
			t.Errorf("CleanText(%q) = %q, want %q", c.in, got, c.want)
		}
		if c.change != 0 && change&c.change == 0 || c.change == 0 && change != 0 {
			t.Errorf("CleanText(%q) change = %b, want %b", c.in, change, c.change)
		}
	}
}

func TestCleanTextColumns(t *testing.T) {
	mon := NewMonitor()
	src := `{"id":1,"body":"<p>CafÃ© &amp; bar</p>\n\n\n<p>Open   late</p>","tag":"  News "}` + "\n" +
		`{"id":2,"body":"plain text","tag":"news"}` + "\n"
	tb := Read(WithMonitor(context.Background(), mon), strings.NewReader(src), FormatNDJSON)
	out, schema := Clean(tb, CleanOptions{Types: true, Text: []string{"body"}})
	rows := collect(out)
	if rows[0] != "1,Café & bar\n\nOpen late,News" || rows[1] != "2,plain text,news" || schema.Kinds[1] != KindText {
		t.Fatalf("rows = %q kinds %v", rows, schema.Kinds)
	}
	c := mon.Report().Counts
	if c["text:mojibake"] != 1 || c["text:html"] != 1 || c["text:whitespace"] != 1 {
		t.Fatalf("counts = %v", c)
	}
}

func FuzzCleanText(f *testing.F) {
	for _, s := range []string{"CafÃ©", "<p>a</p>&amp;", "é", "a\r\n\x00b", "  x  \n\n\n y", "&lt;p&gt;hi&lt;/p&gt;", "caf\xe9"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		once, _ := CleanText(s)
		if twice, c := CleanText(once); twice != once || c != 0 {
			t.Fatalf("not idempotent: %q -> %q -> %q", s, once, twice)
		}
	})
}
