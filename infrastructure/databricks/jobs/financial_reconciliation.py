"""Databricks job: Orders vs payments vs refunds reconciliation."""

import sys

from _bootstrap import bootstrap

bootstrap()

from pipelines.gold.reconciliation import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
