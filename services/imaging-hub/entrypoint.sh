#!/bin/sh
set -e

chown app:app /dicomsorter/data
exec gosu app "$@"
