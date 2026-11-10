package pipeline

import (
	"context"
	"strings"
	"testing"
)

// withCheckDigit appends the digit that makes prefix pass check.
func withCheckDigit(t *testing.T, prefix string, check func(string) bool) string {
	t.Helper()
	for d := '0'; d <= '9'; d++ {
		if s := prefix + string(d); check(s) {
			return s
		}
	}
	t.Fatalf("no check digit for %s", prefix)
	return ""
}

func TestRedactText(t *testing.T) {
	aadhaar := withCheckDigit(t, "23456789012", verhoeff)
	saID := withCheckDigit(t, "800101500908", luhn)
	cases := []struct{ in, country, want string }{
		{"Contact ops@acme.co.ke today.", "", "Contact [EMAIL] today."},
		{"a.b-c+tag@sub.example.org, x@y.com.", "", "[EMAIL], [EMAIL]."},
		{"see x@y.com.2025 and 0@0.AA.0000000000", "", "see [EMAIL].2025 and [EMAIL].[PHONE]"},
		{"0@0.AA-0000000000, ivan@example.xn--p1ai", "", "[EMAIL]-[PHONE], [EMAIL]"},
		{"call +254 712 345 678 or 0712345678", "", "call [PHONE] or [PHONE]"},
		{"0712 345 678", "", "[PHONE]"},
		{"(020) 123 4567", "", "[PHONE]"},
		{"+1 (555) 123-4567", "", "[PHONE]"},
		{"555-123-4567", "", "[PHONE]"},
		{"00254712345678", "", "[PHONE]"},
		{"254712345678", "254", "[PHONE]"},
		{"254712345678", "", "254712345678"},
		{"card 4111 1111 1111 1111 exp", "", "card [CARD] exp"},
		{"5500-0000-0000-0004", "", "[CARD]"},
		{"378282246310005", "", "[CARD]"},
		{"6011111111111117", "", "[CARD]"},
		{"4111 1111 1111 1112", "", "4111 1111 1111 1112"},
		{"SSN 123-45-6789", "", "SSN [ID]"},
		{"aadhaar " + aadhaar[:4] + " " + aadhaar[4:8] + " " + aadhaar[8:], "", "aadhaar [ID]"},
		{saID, "", "[ID]"},
		{"PIN A123456789Z filed", "", "PIN [ID] filed"},
		{"NI AB123456C", "", "NI [ID]"},
		{"1,250.00", "", "1,250.00"},
		{"KES 3,400.50", "", "KES 3,400.50"},
		{"2025-01-15", "", "2025-01-15"},
		{"2025-01-15T10:30:00Z", "", "2025-01-15T10:30:00Z"},
		{"15/02/2025", "", "15/02/2025"},
		{"TX000000123", "", "TX000000123"},
		{"INV-2025-00123", "", "INV-2025-00123"},
		{"192.168.1.100", "", "192.168.1.100"},
		{"1234567890.50", "", "1234567890.50"},
		{"Order 1250000000 shipped", "", "Order 1250000000 shipped"},
		{"version 1.2.3 at 10:30", "", "version 1.2.3 at 10:30"},
		{"user@localhost a@b.c @handle", "", "user@localhost a@b.c @handle"},
		{"seasons 2019-2025", "", "seasons 2019-2025"},
		{"QQ123456C DF123456A", "", "QQ123456C DF123456A"},
		{"+" + aadhaar, "", "[PHONE]"},
		{aadhaar, "", aadhaar},
		{"+254792731719", "", "[PHONE]"},
	}
	for _, c := range cases {
		var n [4]int
		if got := RedactText(c.in, RedactAll, c.country, &n); got != c.want {
			t.Errorf("RedactText(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	var n [4]int
	RedactText("a@b.co 0712345678 4111111111111111 A123456789Z", RedactEmail|RedactCard, "", &n)
	if n != [4]int{1, 0, 0, 1} {
		t.Fatalf("counts with only emails and cards = %v", n)
	}
}

func TestRedactStage(t *testing.T) {
	mon := NewMonitor()
	tb := Read(WithMonitor(context.Background(), mon), strings.NewReader(
		"name,National ID,id,email,Mobile,note\nAmina,12345678,12345678,amina@x.co.ke,254712345678,call 0712 345 678\nOtieno,,7,N/A,,paid by 4111 1111 1111 1111\n"), FormatCSV)
	got := strings.Join(collect(Redact(tb, RedactAll, "254")), ";")
	if got != "Amina,[ID],12345678,[EMAIL],[PHONE],call [PHONE];Otieno,,7,N/A,,paid by [CARD]" {
		t.Fatalf("rows = %q", got)
	}
	r := mon.Report()
	want := map[string]int64{"masked:email": 1, "masked:phone": 2, "masked:id": 1, "masked:card": 1}
	for k, v := range want {
		if r.Counts[k] != v {
			t.Errorf("%s = %d, want %d (all %v)", k, r.Counts[k], v, r.Counts)
		}
	}
	if line := maskedLine(r.Counts); line != "personal data masked: 1 email, 2 phone numbers, 1 ID number, 1 card number." {
		t.Fatalf("line = %q", line)
	}
}

func TestParseRedact(t *testing.T) {
	if k, err := ParseRedact("emails, phone"); err != nil || k != RedactEmail|RedactPhone {
		t.Fatalf("k=%v err=%v", k, err)
	}
	if k, _ := ParseRedact("all"); k != RedactAll {
		t.Fatalf("all = %v", k)
	}
	if _, err := ParseRedact("names"); err == nil || !strings.Contains(err.Error(), `"names"`) {
		t.Fatalf("err = %v", err)
	}
}

func TestIDColumn(t *testing.T) {
	for h, want := range map[string]bool{
		"National ID": true, "national_id": true, "ID Number": true, "id_no": true, "passport_no": true,
		"SSN": true, "KRA PIN": true, "Tax ID": true, "nin": true,
		"id": false, "customer_id": false, "Txn ID": false, "Invoice No": false, "earnings": false, "pin_code": false,
	} {
		if idColumn(h) != want {
			t.Errorf("idColumn(%q) = %v", h, !want)
		}
	}
}

func FuzzRedactText(f *testing.F) {
	for _, s := range []string{"a@b.co", "+254 712 345 678", "4111 1111 1111 1111", "123-45-6789", "A123456789Z", "1,250.00 on 2025-01-15"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		var n [4]int
		once := RedactText(s, RedactAll, "254", &n)
		if twice := RedactText(once, RedactAll, "254", &n); twice != once {
			t.Fatalf("not idempotent: %q -> %q -> %q", s, once, twice)
		}
	})
}
