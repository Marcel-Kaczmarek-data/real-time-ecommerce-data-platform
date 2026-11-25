#!/bin/sh
# SeaweedFS all-in-one (master + volume + filer + S3 gateway) exposing an
# S3-compatible API on :9000. Credentials come from the environment and are
# rendered into the S3 identity file at start-up (never baked into the image).
set -eu

cat > /tmp/s3.json <<EOF
{
  "identities": [
    {
      "name": "platform",
      "credentials": [{"accessKey": "${S3_ACCESS_KEY}", "secretKey": "${S3_SECRET_KEY}"}],
      "actions": ["Admin", "Read", "List", "Tagging", "Write"]
    },
    {
      "name": "bi-reader",
      "credentials": [{"accessKey": "${S3_READONLY_ACCESS_KEY:-bireader}", "secretKey": "${S3_READONLY_SECRET_KEY:-bireader-local-only}"}],
      "actions": ["Read:lakehouse", "List:lakehouse"]
    }
  ]
}
EOF

exec weed server \
  -dir=/data \
  -ip=object-storage \
  -ip.bind=0.0.0.0 \
  -master.volumeSizeLimitMB=1024 \
  -volume.max=0 \
  -filer \
  -s3 \
  -s3.port=9000 \
  -s3.config=/tmp/s3.json \
  -metricsPort=9324
