# ADR 0011 — Local S3-compatible object storage: SeaweedFS instead of MinIO

**Status:** Accepted

## Context
The original specification called for MinIO as the local S3-compatible store.
When the project was built (November 2026), the open-source MinIO server and
client were archived:

* `minio/minio` and `minio/mc` are no longer pullable from Docker Hub
  ("repository does not exist").
* `quay.io/minio/*` returns `401 UNAUTHORIZED`.
* `dl.min.io` returns `410 Gone`, with a notice that community releases are no
  longer served and receive no security updates.

Running an unmaintained, unpatched storage server, even for local use, is a
poor engineering example.

## Decision
Use **SeaweedFS 3.85** (Apache-2.0, actively maintained) in all-in-one mode
(`weed server -s3`) as the local S3 endpoint on port 9000.

* Credentials are rendered from environment variables into the S3 identity
  file at container start (`docker/object-storage/entrypoint.sh`).
* A second, read-only identity (`bi-reader`) demonstrates least-privilege access.
* Buckets `lakehouse` and `landing` are created idempotently by `storage-init`.

## Consequences
* The pipelines are unaffected. Spark uses `s3a://` with path-style access,
  and delta-rs and boto3 use `s3://` with a custom endpoint. Switching to AWS
  S3, MinIO or any other S3-compatible store only changes `S3_ENDPOINT_URL`
  and the credentials.
* SeaweedFS does not implement bucket lifecycle (ILM) rules the way S3 does.
  Retention for the local landing bucket is therefore handled by the
  `delta_table_maintenance` DAG. The AWS deployment uses native S3 lifecycle
  rules (Terraform).
* SeaweedFS exposes Prometheus metrics on `:9324`, and they are scraped.

## Alternatives considered
* **RustFS**: MinIO-compatible API, but young (alpha/beta in 2025).
* **Garage**: lightweight, but its cluster layout and key bootstrap make one-command start-up harder.
* **LocalStack**: emulates many AWS services; recent images need an account token.
* **Building MinIO from the archived source**: unmaintained, with no security fixes.
