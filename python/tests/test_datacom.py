import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import datacom

SAMPLE = Path(os.environ.get("DATACOM_SAMPLE", Path(__file__).resolve().parents[2] / "testdata" / "messy.csv"))


class DatacomTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.tmp = Path(self.dir.name)

    def tearDown(self):
        self.dir.cleanup()

    def test_bundled_binary(self):
        self.assertTrue(Path(datacom.binary()).is_file())
        out = datacom.run("version").stdout
        self.assertIn("datacom v" + datacom.__version__, out)

    def test_profile(self):
        report = datacom.profile(SAMPLE)
        self.assertEqual(report["rows"], 8)
        self.assertEqual(report["duplicate_rows"], 1)
        self.assertTrue(any("personal data" in i for i in report["issues"]))

    def test_clean_returns_manifest(self):
        out = self.tmp / "clean.csv"
        m = datacom.clean(SAMPLE, out, snake=True, dedupe=True, redact=["emails"])
        self.assertEqual(m["status"], "ok")
        self.assertEqual(m["rows_out"], 7)
        self.assertEqual(m["outputs"][0]["sha256"], hashlib.sha256(out.read_bytes()).hexdigest())
        self.assertEqual(m["inputs"][0]["sha256"], hashlib.sha256(SAMPLE.read_bytes()).hexdigest())
        self.assertNotIn("@", out.read_text())
        rules = {r["rule"]: r for r in m["rules"]}
        self.assertEqual(rules["dedupe"]["rows_dropped"], 1)
        self.assertEqual(rules["redact"]["values_masked"]["email"], 6)

    def test_query_flags(self):
        out = self.tmp / "q.ndjson"
        m = datacom.query(SAMPLE, out, snake=True, where=["amount_kes > 1000", "paid == true"], select=["customer_name", "amount_kes"])
        rows = [json.loads(line) for line in out.read_text().splitlines()]
        self.assertEqual(m["rows_out"], len(rows))
        self.assertTrue(rows and all(set(r) == {"customer_name", "amount_kes"} for r in rows))

    def test_errors(self):
        with self.assertRaises(datacom.DatacomError) as cm:
            datacom.query(SAMPLE, self.tmp / "x.csv", select="zzz")
        self.assertIn('"zzz"', str(cm.exception))
        with self.assertRaises(datacom.DatacomError):
            datacom.profile(self.tmp / "missing.csv")

    def test_console_script(self):
        exe = Path(sys.executable).parent / ("datacom.exe" if os.name == "nt" else "datacom")
        out = subprocess.run([str(exe), "version"], capture_output=True, text=True, check=True).stdout
        self.assertIn("datacom", out)
        out = subprocess.run([sys.executable, "-m", "datacom", "version"], capture_output=True, text=True, check=True).stdout
        self.assertIn("datacom", out)

    def test_flags(self):
        self.assertEqual(datacom._flags({"snake": True, "dedupe": False, "near_dupes": 0.8, "text": ["a", "b"], "where": ["x > 1", "y == 2"], "o": None}),
                         ["-snake", "-near-dupes", "0.8", "-text", "a,b", "-where", "x > 1", "-where", "y == 2"])


if __name__ == "__main__":
    unittest.main()
