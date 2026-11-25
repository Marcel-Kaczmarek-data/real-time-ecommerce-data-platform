"""Databricks job: Silver -> Gold (guarded by the critical data-quality gate)."""

import sys

from _bootstrap import bootstrap

bootstrap()

from pipelines.gold.run import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
