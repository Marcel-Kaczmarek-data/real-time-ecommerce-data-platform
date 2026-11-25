"""Databricks job: Data-quality suites; results to audit.dq_results."""

import sys

from _bootstrap import bootstrap

bootstrap()

from data_quality.run import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
