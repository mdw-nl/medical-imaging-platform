#!/bin/sh
set -e

chown app:app /dicomsorter/data
chown app:app /dicom-staging-overflow
exec gosu app "$@"
