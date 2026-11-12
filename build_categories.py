"""Build metadata/arxiv_categories.csv from the official arXiv category taxonomy.

Source: https://arxiv.org/category_taxonomy

Page structure (checked when this script was written):
  <h2 class="accordion-head"> ... Group Name ... </h2>      -> subject_group
  <h3>Archive Name<br><span>(archive-id)</span></h3>        -> physics archives only
  <h4>category.id <span>(Category Name)</span></h4>          -> one category
  <p>description ...</p>                                     -> may say "X is an alias for Y"

Official aliases are normalised to their canonical code ("X is an alias for Y"
-> canonical Y). The alias row keeps its own name but takes the canonical
category's subject group, so a paper listed under both codes is not counted as
cross-disciplinary.

Usage:
    python build_categories.py                 # download page and rebuild CSV
    python build_categories.py --html page.html  # parse a saved copy instead
"""

import argparse
import csv
import datetime as dt
import hashlib
import json
import re
import urllib.request
from html.parser import HTMLParser
from pathlib import Path

URL = "https://arxiv.org/category_taxonomy"
ROOT = Path(__file__).resolve().parent
OUT_CSV = ROOT / "metadata" / "arxiv_categories.csv"
OUT_SOURCE = ROOT / "metadata" / "arxiv_categories_source.json"

ALIAS_RE = re.compile(r"^\s*([\w.\-]+) is an alias for ([\w.\-]*\w)")


class TaxonomyParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.group = None
        self.archive_id = None
        self.archive_name = None
        self._tag = None        # which heading we are inside
        self._buf = []
        self._in_list = False   # skip the "classification guide" legend
        self._await_desc = False

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if attrs.get("id") == "category_taxonomy_list":
            self._in_list = True
        if not self._in_list:
            return
        if tag in ("h2", "h3", "h4"):
            self._tag, self._buf = tag, []
        elif tag == "p" and self._await_desc:
            self._tag, self._buf = "p", []

    def handle_data(self, data):
        if self._tag:
            self._buf.append(data)

    def handle_endtag(self, tag):
        if not self._in_list or tag != self._tag:
            return
        text = " ".join("".join(self._buf).split())
        self._tag = None
        if tag == "h2":
            self.group = text
            self.archive_id = self.archive_name = None
        elif tag == "h3":
            m = re.match(r"(.*)\((.*)\)\s*$", text)
            self.archive_name, self.archive_id = m.group(1).strip(), m.group(2).strip()
        elif tag == "h4":
            m = re.match(r"(\S+)\s*\((.*)\)\s*$", text)
            code, name = m.group(1), m.group(2)
            self.rows.append({
                "category_id": code,
                "category_name": name,
                "subject_group": self.group,
                # outside physics the archive is simply the code prefix (cs, math, ...)
                "archive_id": self.archive_id or code.split(".")[0],
                "archive_name": self.archive_name or self.group,
                "canonical_category_id": code,
                "is_alias": False,
                "alias_note": "",
            })
            self._await_desc = True
        elif tag == "p":
            self._await_desc = False
            m = ALIAS_RE.match(text)
            if m and self.rows and m.group(1) == self.rows[-1]["category_id"]:
                self.rows[-1]["canonical_category_id"] = m.group(2)
                self.rows[-1]["is_alias"] = True
                self.rows[-1]["alias_note"] = m.group(0).strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--html", help="parse a saved copy of the page")
    args = parser.parse_args()

    if args.html:
        html = Path(args.html).read_bytes()
    else:
        req = urllib.request.Request(URL, headers={"User-Agent": "arxiv-course-project/1.0"})
        html = urllib.request.urlopen(req, timeout=60).read()

    p = TaxonomyParser()
    p.feed(html.decode("utf-8"))
    rows = p.rows

    by_id = {r["category_id"]: r for r in rows}
    assert len(by_id) == len(rows), "category ids must be unique"
    for r in rows:
        if r["is_alias"]:
            canon = by_id[r["canonical_category_id"]]
            # alias takes the group of its canonical category
            r["subject_group"] = canon["subject_group"]
            r["archive_id"] = canon["archive_id"]
            r["archive_name"] = canon["archive_name"]

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fields = ["category_id", "category_name", "subject_group", "archive_id", "archive_name",
              "canonical_category_id", "is_alias", "alias_note"]
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    groups = sorted({r["subject_group"] for r in rows})
    source = {
        "source_url": URL,
        "retrieved_date": dt.date.today().isoformat(),
        "page_sha256": hashlib.sha256(html).hexdigest(),
        "num_categories": len(rows),
        "num_aliases": sum(r["is_alias"] for r in rows),
        "aliases": {r["category_id"]: r["canonical_category_id"] for r in rows if r["is_alias"]},
        "subject_groups": groups,
        "note": ("subject_group is the top-level arXiv group heading on the taxonomy page. "
                 "Legacy codes that are no longer listed (e.g. pre-2009 archives) are not in "
                 "the CSV and show up as 'Unknown' in the pipeline."),
    }
    if args.html and OUT_SOURCE.exists():
        source["retrieved_date"] = json.loads(OUT_SOURCE.read_text())["retrieved_date"]
    OUT_SOURCE.write_text(json.dumps(source, indent=2), encoding="utf-8")
    print(f"{len(rows)} categories, {source['num_aliases']} aliases, groups: {groups}")


if __name__ == "__main__":
    main()
