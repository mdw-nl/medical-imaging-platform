#!/bin/sh
set -e

TARGET_UID=${HOST_UID:-$(id -u app)}
TARGET_GID=${HOST_GID:-$(id -g app)}

chown ${TARGET_UID}:${TARGET_GID} /dicomsorter/data
chown ${TARGET_UID}:${TARGET_GID} /dicom-staging-overflow
exec gosu ${TARGET_UID}:${TARGET_GID} "$@"
