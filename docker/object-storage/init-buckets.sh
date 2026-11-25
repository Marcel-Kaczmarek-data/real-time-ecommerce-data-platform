#!/bin/sh
# Create the lakehouse and landing buckets (idempotent).
set -eu
for bucket in lakehouse landing; do
  if echo "s3.bucket.list" | weed shell -master=object-storage:9333 2>/dev/null | grep -qw "${bucket}"; then
    echo "bucket ${bucket} exists"
  else
    echo "s3.bucket.create -name ${bucket}" | weed shell -master=object-storage:9333
    echo "bucket ${bucket} created"
  fi
done
