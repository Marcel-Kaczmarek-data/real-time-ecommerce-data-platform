"""Shared bootstrap for Databricks job entry points.

The bundle syncs the repository next to the job files. This module puts the
repository root on ``sys.path`` and selects the cloud configuration, so the
job scripts are one-liners around the same ``main()`` functions used locally.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def bootstrap() -> None:
    here = Path(__file__).resolve()
    # Bundle layout: <root>/infrastructure/databricks/jobs/<file> -> repo root is 3 levels up.
    repo_root = here.parents[3]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    os.environ.setdefault("PLATFORM_ENV", "cloud")
