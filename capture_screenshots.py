"""Screenshots of the Spark UI and of the run log, taken with headless Chrome.

The Spark UI only exists while the application is running, so the UI pages are
captured from inside pipeline.py:

    python pipeline.py --capture-ui

storage() is called while the cache experiment's DataFrame is cached and
final_pages() just before spark.stop(), so every picture belongs to the same
run as the files in reports/.

The console log of a finished run can be turned into an image with:

    python capture_screenshots.py --log reports/run_log.txt

(that image shows the log file's text, it is not a Spark UI page).

Playwright drives the Google Chrome that is already installed on the machine.
"""

import argparse
import html
import json
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright

VIEWPORT = {"width": 1600, "height": 1000}


def _launch(pw):
    try:
        return pw.chromium.launch(channel="chrome")
    except Exception:  # fall back to a Playwright-managed Chromium if installed
        return pw.chromium.launch()


class LiveUICapture:
    def __init__(self, spark, out_dir):
        sc = spark.sparkContext
        self.base = f"http://localhost:{urlparse(sc.uiWebUrl).port}"
        self.app_id = sc.applicationId
        self.out = Path(out_dir)
        self.out.mkdir(parents=True, exist_ok=True)
        self.index = []

    # ------------------------------------------------------------------ helpers
    def _api(self, path):
        url = f"{self.base}/api/v1/applications/{self.app_id}/{path}"
        with urllib.request.urlopen(url, timeout=60) as r:
            return json.load(r)

    def _shot(self, page, path, name, caption, full_page=True, max_height=None):
        page.goto(self.base + path, wait_until="networkidle")
        page.wait_for_timeout(1500)  # DAG/timeline are drawn by JavaScript
        file = self.out / name
        if max_height:
            height = min(page.evaluate("document.body.scrollHeight"), max_height)
            page.screenshot(path=str(file), clip={"x": 0, "y": 0, "width": VIEWPORT["width"], "height": height},
                            full_page=True)
        else:
            page.screenshot(path=str(file), full_page=full_page)
        self.index.append({"file": name, "url": path, "caption": caption})
        print(f"screenshot: {file.name}  <- {path}")

    def _sql_page(self, page, exec_id, stem, caption, click_nodes=(), plan_height=1500):
        """SQL execution page: (a) plan graph with the metrics panel of each key node
        opened by clicking it, (b) the physical plan text ('Plan Details')."""
        path = f"/SQL/execution/?id={exec_id}"
        page.goto(self.base + path, wait_until="networkidle")
        page.wait_for_timeout(1500)
        nodes = page.locator("#plan-viz-graph svg g.node")
        for node_name in click_nodes:
            for i in range(nodes.count()):
                label = (nodes.nth(i).text_content() or "").strip()
                if label.startswith(node_name):
                    nodes.nth(i).click()
                    page.wait_for_timeout(800)
                    page.mouse.move(5, 5)          # hide hover tooltips
                    page.wait_for_timeout(300)
                    bottom = page.evaluate(
                        "() => { const r = document.getElementById('plan-viz-content').getBoundingClientRect();"
                        " return r.bottom + window.scrollY; }")
                    name = f"{stem}_graph_{node_name.split()[0].lower()}.png"
                    page.screenshot(path=str(self.out / name), full_page=True,
                                    clip={"x": 0, "y": 0, "width": VIEWPORT["width"], "height": bottom + 10})
                    self.index.append({"file": name, "url": path,
                                       "caption": f"{caption} Metrics panel of node '{node_name}'."})
                    print(f"screenshot: {name}  <- {path}")
                    break
        page.evaluate("() => document.getElementById('physical-plan-details').classList.add('show')")
        page.wait_for_timeout(400)
        box = page.locator("#physical-plan-details").bounding_box()
        name = f"{stem}_physical_plan.png"
        page.screenshot(path=str(self.out / name), full_page=True,
                        clip={"x": 0, "y": box["y"] - 40, "width": VIEWPORT["width"],
                              "height": min(box["height"] + 40, plan_height)})
        self.index.append({"file": name, "url": path, "caption": f"{caption} Physical plan text (Plan Details)."})
        print(f"screenshot: {name}  <- {path}")

    def _with_page(self, fn):
        with sync_playwright() as pw:
            browser = _launch(pw)
            page = browser.new_page(viewport=VIEWPORT, device_scale_factor=1.5)
            try:
                fn(page)
            finally:
                browser.close()
        (self.out / "index.json").write_text(json.dumps(
            {"application_id": self.app_id, "screenshots": self.index}, indent=2), encoding="utf-8")

    def _find_execution(self, execs, desc_prefix, must_contain=()):
        hits = [e for e in execs if (e.get("description") or "").startswith(desc_prefix)
                and all(s in (e.get("planDescription") or "") for s in must_contain)]
        return hits[-1]["id"] if hits else None

    # ------------------------------------------------------------------ pages
    def storage(self):
        """Call while a DataFrame is cached (before unpersist)."""
        def run(page):
            self._shot(page, "/storage/", "09_storage_cached_dataframe.png",
                       "Storage tab while the cleaned-papers DataFrame is cached (cache experiment).",
                       full_page=False)
            rdds = self._api("storage/rdd")
            if rdds:
                self._shot(page, f"/storage/rdd/?id={rdds[0]['id']}", "10_storage_rdd_detail.png",
                           "Cached partitions of the cleaned-papers DataFrame (memory size per partition).",
                           max_height=1400)
        self._with_page(run)

    def final_pages(self):
        execs = self._api("sql?details=false&planDescription=true&offset=0&length=100000")
        targets = [
            ("B1 ", ("InsertIntoHadoopFsRelationCommand",), "03_B1_clean_write", ("Scan parquet", "Exchange"),
             "B1 clean + write query (filters, withColumn, regex flags, write by year)."),
            ("B3 ", ("BroadcastHashJoin", "InsertIntoHadoopFsRelationCommand"), "04_B3_join",
             ("BroadcastHashJoin", "Exchange"),
             "B3 explode + broadcast LEFT join + dedupe on (id, subject_group)."),
            ("C1 ", ("InsertIntoHadoopFsRelationCommand",), "05_C1_sql1_monthly", ("Scan parquet",),
             "SQL 1 (monthly AI trends), executed and written to Parquet."),
            ("C2 ", ("InsertIntoHadoopFsRelationCommand",), "06_C2_sql2_fields", ("Scan parquet",),
             "SQL 2 (2025 vs 2026 field comparison), executed and written to Parquet."),
            ("Partition pruning check: year_2025", (), "07_pruning_year_2025", ("Scan parquet",),
             "Dataset root read with submitted_year = 2025 (partition pruning)."),
            ("Partition pruning check: all_years", (), "07b_pruning_all_years", ("Scan parquet",),
             "Same query without the year filter (baseline)."),
            ("Lazy evaluation demo", (), "08_lazy_demo_action", ("Project",),
             "Query run by the lazy-evaluation demo's collect() action."),
        ]

        def run(page):
            self._shot(page, "/jobs/", "01_jobs_all_succeeded.png",
                       "Jobs tab of the full run: completed jobs, none failed.", max_height=1800)
            self._shot(page, "/SQL/", "02_sql_queries_list.png",
                       "SQL / DataFrame tab: every query of the run with its duration.", max_height=2600)
            for prefix, must, stem, click_nodes, caption in targets:
                exec_id = self._find_execution(execs, prefix, must)
                if exec_id is not None:
                    self._sql_page(page, exec_id, stem, caption + f" (SQL execution {exec_id}).", click_nodes)
            stages = self._api("stages")
            b1 = [s for s in stages if (s.get("description") or "").startswith("B1 ")]
            if b1:
                big = max(b1, key=lambda s: s.get("inputBytes", 0))
                self._shot(page, f"/stages/stage/?id={big['stageId']}&attempt={big['attemptId']}",
                           "11_stage_B1_scan_tasks.png",
                           "Scan/regex stage of B1: input size, task summary (min/median/max) and parallelism.",
                           max_height=2200)
            self._shot(page, "/stages/", "12_stages_list.png",
                       "Stages tab: input, shuffle read/write per stage.", max_height=2200)
            self._shot(page, "/executors/", "13_executors_local_mode.png",
                       "Executors tab: single local driver/executor (local mode).", full_page=False)
        self._with_page(run)


def render_log(log_file, out_png, title="Console output of the full pipeline run"):
    """Screenshot of the real run log, rendered as plain monospaced text."""
    text = Path(log_file).read_text(encoding="utf-8")
    page_html = f"""<html><head><meta charset="utf-8"><style>
      body {{ margin:0; background:#1a1a19; color:#f0efec; font:13px/1.45 Consolas, monospace; }}
      .bar {{ background:#2b2b29; color:#c3c2b7; padding:8px 14px; font-family:Segoe UI, sans-serif; }}
      pre {{ margin:0; padding:12px 14px; white-space:pre-wrap; word-break:break-all; }}
      .pass {{ color:#7fd18b; }} .done {{ color:#8ab8f0; }}
    </style></head><body><div class="bar">{html.escape(title)} &mdash; {html.escape(str(log_file))}</div>
    <pre>{html.escape(text).replace("check PASS", '<span class="pass">check PASS</span>').replace("SUCCESS", '<span class="pass">SUCCESS</span>').replace("[done ]", '<span class="done">[done ]</span>')}</pre></body></html>"""
    with sync_playwright() as pw:
        browser = _launch(pw)
        page = browser.new_page(viewport={"width": 1500, "height": 900})
        page.set_content(page_html)
        page.screenshot(path=str(out_png), full_page=True)
        browser.close()
    print(f"screenshot: {out_png}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", default="reports/run_log.txt")
    ap.add_argument("--out", default="screenshots/00_run_log_full_pipeline.png")
    a = ap.parse_args()
    render_log(a.log, a.out)
