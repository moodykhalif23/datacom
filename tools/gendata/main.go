// gendata writes a realistic, deliberately messy transactions CSV for
// stress testing: East African names, phones, counties, mixed amount and
// date formats, varied null markers, duplicates, and quoted notes with
// commas, quotes and newlines. Output is deterministic for a given seed.
//
//	go run ./tools/gendata -rows 10000000 -o big.csv.gz
package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
)

var (
	first    = strings.Fields("Brian Amina Wanjiru Otieno Kamau Achieng Mwangi Fatuma Kipchoge Njeri Omondi Zawadi Baraka Chebet Mutua Naliaka Juma Wairimu Hassan Atieno Kiprop Nyambura Oduya Halima Gitonga Akinyi Mwende Rotich Imani Kariuki Said Wafula Nekesa Omar Makena Barasa Lucy Peter Grace John")
	last     = strings.Fields("Odhiambo Wanjiku Kiplagat Mwangi Mutiso Okoth Njoroge Chepkoech Kamande Abdalla Ouma Waweru Nyongesa Maina Kibet Onyango Wambui Koech Otieno Mugo Karanja Ali Simiyu Muthoni Owino Langat Macharia Nabwire Gathoni Oloo")
	counties = strings.Fields("Nairobi Mombasa Kisumu Nakuru Uasin-Gishu Kiambu Machakos Kajiado Meru Nyeri Kilifi Kakamega Bungoma Garissa Turkana Kericho Embu Laikipia Kitui Homa-Bay")
	domains  = strings.Fields("gmail.com yahoo.com outlook.com safaricom.co.ke kcbgroup.com go.ke county.go.ke equitybank.co.ke")
	statuses = []string{"paid", "paid", "Paid", "PAID", "pending", "Pending", "failed", "refunded", "", "N/A"}
	channels = []string{"mpesa", "mpesa", "mpesa", "card", "bank", "cash", "M-Pesa", "MPESA"}
	nulls    = []string{"", "", "NULL", "null", "N/A", "n/a", "-", "--", "NaN", "none", "?"}
	notes    = []string{"", "", "", "", "late payment, follow up", `customer said "will pay"`, "split invoice,\nsecond line", "ok", "VAT inclusive", "  spaced   out  "}
)

func main() {
	rows := flag.Int("rows", 1_000_000, "rows to generate")
	seed := flag.Int64("seed", 1, "random seed")
	out := flag.String("o", "", "output file (.gz for gzip); default stdout")
	dirty := flag.Float64("dirty", 0.12, "fraction of cells written in a messy form")
	dup := flag.Float64("dup", 0.02, "fraction of rows that duplicate an earlier row")
	bom := flag.Bool("bom", true, "start with a UTF-8 byte order mark, as Excel exports do")
	flag.Parse()

	var w io.Writer = os.Stdout
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer f.Close()
		w = f
		if strings.HasSuffix(*out, ".gz") {
			gz := gzip.NewWriter(f)
			defer gz.Close()
			w = gz
		}
	}
	bw := bufio.NewWriterSize(w, 1<<20)
	defer bw.Flush()

	r := rand.New(rand.NewSource(*seed))
	pick := func(s []string) string { return s[r.Intn(len(s))] }
	messy := func() bool { return r.Float64() < *dirty }

	if *bom {
		bw.WriteString("\uFEFF")
	}
	bw.WriteString("Txn ID,Customer Name,Phone,Email,County,Amount (KES),Status,Channel,Txn Date,Age,Notes\n")
	recent := make([]string, 0, 1000)
	for i := 0; i < *rows; i++ {
		if len(recent) > 0 && r.Float64() < *dup {
			bw.WriteString(recent[r.Intn(len(recent))])
			continue
		}
		fn, ln := pick(first), pick(last)
		name := fn + " " + ln
		if messy() {
			name = pick([]string{strings.ToUpper(name), " " + name + " ", fn + "  " + ln, "\"" + ln + ", " + fn + "\""})
		}
		phone := fmt.Sprintf("+2547%08d", r.Intn(100_000_000))
		if messy() {
			phone = pick([]string{"07" + phone[5:], "254 " + phone[4:7] + " " + phone[7:10] + " " + phone[10:], phone[4:], pick(nulls)})
		}
		email := strings.ToLower(fn + "." + ln + "@" + pick(domains))
		if messy() {
			email = pick([]string{strings.ToUpper(email), " " + email, pick(nulls)})
		}
		county := pick(counties)
		if messy() {
			county = pick([]string{strings.ToLower(county), strings.ToUpper(county), pick(nulls)})
		}
		amt := r.ExpFloat64() * 8000
		amount := fmt.Sprintf("%.2f", amt)
		if messy() {
			amount = pick([]string{"\"" + commas(amt) + "\"", fmt.Sprintf("KES %.2f", amt), fmt.Sprintf("Ksh %d", int(amt)), fmt.Sprintf("(%.2f)", amt), fmt.Sprintf("%d", int(amt)), pick(nulls)})
		}
		y, m, d := 2024+r.Intn(2), 1+r.Intn(12), 1+r.Intn(28)
		date := fmt.Sprintf("%04d-%02d-%02d", y, m, d)
		if messy() {
			date = pick([]string{fmt.Sprintf("%02d/%02d/%04d", d, m, y), fmt.Sprintf("%d %s %d", d, months[m-1], y), date + "T09:30:00Z", pick(nulls)})
		}
		age := fmt.Sprint(18 + r.Intn(60))
		if messy() {
			age = pick(nulls)
		}
		note := pick(notes)
		if strings.ContainsAny(note, ",\"\n") {
			note = "\"" + strings.ReplaceAll(note, "\"", "\"\"") + "\""
		}
		line := fmt.Sprintf("TX%09d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", i, name, phone, email, county, amount, pick(statuses), pick(channels), date, age, note)
		bw.WriteString(line)
		if len(recent) < cap(recent) {
			recent = append(recent, line)
		} else {
			recent[r.Intn(len(recent))] = line
		}
	}
}

var months = strings.Fields("Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec")

func commas(f float64) string {
	s := fmt.Sprintf("%.2f", f)
	for i := len(s) - 6; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}
