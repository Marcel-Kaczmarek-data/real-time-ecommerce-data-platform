"""Capture real screenshots of every dashboard page (Playwright, headless Chromium).

    python scripts/capture_screenshots.py --url http://localhost:8501 --out dashboards/screenshots

Fails (non-zero) if a page shows a Streamlit exception, so it doubles as a UI smoke test.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PAGES = [
    ("", "01_executive_overview"),  # default page is served at the root URL
    ("sales-revenue", "02_sales_revenue"),
    ("customer-analytics", "03_customer_analytics"),
    ("customer-360", "04_customer_360"),
    ("product-performance", "05_product_performance"),
    ("conversion-funnel", "06_conversion_funnel"),
    ("inventory-management", "07_inventory_management"),
    ("order-fulfillment", "08_order_fulfillment"),
    ("payments-refunds", "09_payments_refunds"),
    ("data-quality-pipeline-health", "10_data_quality"),
]


def main(argv: list[str] | None = None) -> int:
    from playwright.sync_api import sync_playwright

    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8501")
    parser.add_argument("--out", default="dashboards/screenshots")
    parser.add_argument("--width", type=int, default=1600)
    parser.add_argument("--channel", default=None, help="use an installed browser, e.g. msedge or chrome")
    args = parser.parse_args(argv)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    failures = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(channel=args.channel) if args.channel else pw.chromium.launch()
        page = browser.new_page(viewport={"width": args.width, "height": 1000}, device_scale_factor=1)
        for slug, name in PAGES:
            page.goto(f"{args.url}/{slug}", wait_until="networkidle")
            page.wait_for_selector("[data-testid='stAppViewContainer']", timeout=60_000)
            # Wait until Streamlit has finished running the script (status widget disappears).
            page.wait_for_function(
                "() => !document.querySelector('[data-testid=\"stStatusWidget\"]')", timeout=120_000
            )
            page.wait_for_timeout(2500)  # let Plotly finish drawing
            errors = page.locator("[data-testid='stException']").count()
            if errors:
                failures.append(f"{slug}: {errors} exception(s)")
            page.screenshot(path=str(out / f"{name}.png"), full_page=True)
            print(f"captured {name}.png{'  (EXCEPTION)' if errors else ''}")
        browser.close()
    if failures:
        print("pages with errors:", failures)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
