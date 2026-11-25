"""Databricks job: OPTIMIZE / VACUUM / retention (predictive optimisation may make parts redundant)."""

import sys

from _bootstrap import bootstrap

bootstrap()

from pipelines.maintenance import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
