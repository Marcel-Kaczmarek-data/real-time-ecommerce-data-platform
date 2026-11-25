"""Databricks job: Bronze -> Silver incremental processing (availableNow)."""

import sys

from _bootstrap import bootstrap

bootstrap()

from pipelines.silver.run import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
