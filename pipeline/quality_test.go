package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const goodDoc = `The county assembly met on Tuesday to discuss the new budget for roads and water.
Members raised concerns about delays in paying contractors, and the finance committee agreed to publish a timeline.
Residents who attended said they want regular updates, clearer reporting and a public meeting before the next vote.
The speaker closed the session with a promise to review the plan within two weeks and to share the findings.`

func TestQualityRules(t *testing.T) {
	q := DefaultQuality()
	words := func(n int, w string) string { return strings.TrimSpace(strings.Repeat(w+" ", n)) }
	cases := map[string]string{
		goodDoc:             "",
		"Too short to keep": "min-words",
		words(60, "#tag"):   "symbols",
		strings.Repeat("• bullet item with several normal words here\n", 12):                    "bullets",
		strings.Repeat("and then the story slowly trails off into nothing much at all...\n", 8): "ellipsis",
		words(60, "12345 67890"): "alpha",
		strings.Repeat("Click here to subscribe to our newsletter today\n", 8): "dup-lines",
		words(80, "a b"):                  "word-length",
		words(60, "supercalifragilistic"): "word-length",
	}
	for doc, want := range cases {
		if got := q.Check(doc); got != want {
			t.Errorf("Check(%.40q...) = %q, want %q", doc, got, want)
		}
	}
	swahili := strings.Repeat("Wananchi wa kaunti walikutana jana kujadili bajeti mpya ya barabara na maji safi kwa ajili ya vijiji vyote. ", 6)
	if got := q.Check(swahili); got != "" {
		t.Errorf("Swahili prose failed %q", got)
	}
	strict, err := ParseQuality("stopwords=2,min-words=10")
	if err != nil || strict.Check(swahili) != "stopwords" || strict.Check(goodDoc) != "" || strict.MinWords != 10 {
		t.Fatalf("strict = %+v err %v swahili %q", strict, err, strict.Check(swahili))
	}
	for _, bad := range []string{"min-words", "colour=2", "alpha=x"} {
		if _, err := ParseQuality(bad); err == nil {
			t.Errorf("ParseQuality(%q) should fail", bad)
		}
	}
}

func TestQualityStage(t *testing.T) {
	mon := NewMonitor()
	src := "id,body\n1,\"" + strings.ReplaceAll(goodDoc, "\n", " ") + "\"\n2,too short\n3,\"" + strings.Repeat("#tag ", 60) + "\"\n"
	tb := Read(WithMonitor(context.Background(), mon), strings.NewReader(src), FormatCSV)
	out, err := Quality(tb, DefaultQuality(), []string{"body"})
	if err != nil {
		t.Fatal(err)
	}
	if rows := collect(out); len(rows) != 1 || !strings.HasPrefix(rows[0], "1,") {
		t.Fatalf("rows = %q", rows)
	}
	c := mon.Report().Counts
	if c["dropped:quality:min-words"] != 1 || c["dropped:quality:symbols"] != 1 {
		t.Fatalf("counts = %v", c)
	}
}

func TestDecontaminate(t *testing.T) {
	dir := t.TempDir()
	test := filepath.Join(dir, "mmlu.jsonl")
	os.WriteFile(test, []byte(`{"question":"Which river flows through the city of Kisumu before it reaches Lake Victoria in western Kenya today?","answer":"Nyando"}
{"question":"Name the capital of Kenya without using a map please","answer":"Nairobi"}
{"question":"Paris","answer":"yes"}
`), 0o644)
	c, err := LoadTestSets(context.Background(), []string{test}, 13)
	if err != nil {
		t.Fatal(err)
	}
	if c.Items[0] != 2 || c.Skipped != 1 {
		t.Fatalf("items %v skipped %d", c.Items, c.Skipped)
	}
	mon := NewMonitor()
	src := "id,body\n" +
		"1,\"Quiz: WHICH river flows through the city of Kisumu, before it reaches Lake Victoria in western Kenya?\"\n" +
		"2,\"River flows through the city of Kisumu before it reaches Lake Victoria\"\n" +
		"3,\"Q: name the capital of Kenya without using a map, please. A: Nairobi\"\n" +
		"4,\"We visited Paris and said yes to everything.\"\n"
	tb := Read(WithMonitor(context.Background(), mon), strings.NewReader(src), FormatCSV)
	out, err := Decontaminate(tb, c, []string{"body"})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, r := range collect(out) {
		ids = append(ids, r[:1])
	}
	if strings.Join(ids, ",") != "2,4" {
		t.Fatalf("kept %v", ids)
	}
	if mon.Report().Counts["dropped:decontam:1"] != 2 {
		t.Fatalf("counts = %v", mon.Report().Counts)
	}
	if _, err := LoadTestSets(context.Background(), []string{filepath.Join(dir, "nope.jsonl")}, 13); err == nil {
		t.Fatal("missing test set must fail")
	}
}
