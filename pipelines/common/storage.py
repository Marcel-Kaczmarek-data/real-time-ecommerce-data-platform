"""Object-storage helpers (MinIO locally, S3 in AWS) for non-Spark code."""

from __future__ import annotations

import hashlib
from typing import Any

from pipelines.common.config import Config, load_config


def s3_client(cfg: Config | None = None) -> Any:
    import boto3
    from botocore.config import Config as BotoConfig

    cfg = cfg or load_config()
    st = cfg["storage"]
    kwargs: dict[str, Any] = {
        "region_name": st.get("region") or "us-east-1",
        "config": BotoConfig(
            retries={"max_attempts": 10, "mode": "adaptive"},
            s3={"addressing_style": "path" if st.get("path_style_access") else "auto"},
        ),
    }
    if st.get("endpoint_url"):
        kwargs["endpoint_url"] = st["endpoint_url"]
    if st.get("access_key"):
        kwargs["aws_access_key_id"] = st["access_key"]
        kwargs["aws_secret_access_key"] = st["secret_key"]
    return boto3.client("s3", **kwargs)


def delta_rs_storage_options(cfg: Config | None = None) -> dict[str, str]:
    """Storage options for delta-rs (`deltalake` package)."""
    cfg = cfg or load_config()
    st = cfg["storage"]
    opts: dict[str, str] = {"AWS_REGION": st.get("region") or "us-east-1"}
    if st.get("endpoint_url"):
        opts["AWS_ENDPOINT_URL"] = st["endpoint_url"]
        opts["AWS_ALLOW_HTTP"] = "true" if not st.get("ssl_enabled", True) else "false"
    if st.get("access_key"):
        opts["AWS_ACCESS_KEY_ID"] = st["access_key"]
        opts["AWS_SECRET_ACCESS_KEY"] = st["secret_key"]
    if st.get("path_style_access"):
        opts["AWS_S3_ADDRESSING_STYLE"] = "path"
        opts["aws_virtual_hosted_style_request"] = "false"
    # Single-writer setup (see ADR 0003): delta-rs only reads, never writes.
    opts["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
    return opts


def to_s3_uri(path: str) -> str:
    """Convert Spark's ``s3a://`` URIs to the ``s3://`` scheme used by delta-rs/boto3."""
    return "s3://" + path[len("s3a://") :] if path.startswith("s3a://") else path


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
