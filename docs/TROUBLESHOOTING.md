# Troubleshooting

Common issues encountered when running the imaging pipeline.

---

## DICOM sender receives Response 0xA700 (Out of Resources)

**Symptom:** The sending PACS or `storescu` reports DIMSE status `0xA700`
(Out of Resources). Logs may show either:

```
Staging failed (disk full?) for SOP <uid>
```

or:

```
Processing queue full after timeout, returning 0xA700 for SOP <uid>
```

**Cause:** The tmpfs RAM disk used for staging incoming DICOM has run out of
space, or the processing queue cannot keep up with the send rate.

The imaging-hub stages every incoming file on a tmpfs mount
(`/dicom-staging`, default **2 GB**). When usage exceeds the threshold
(default **85 %**), writes spill to an encrypted overflow volume. If the
tmpfs fills completely before the threshold check kicks in, or if the
overflow path also cannot accept writes, the handler returns `0xA700`.

**Fix — increase tmpfs size:**

Set `STAGING_TMPFS_SIZE` in your `.env` or `docker-compose.yml` override:

```bash
# .env
STAGING_TMPFS_SIZE=4g
```

This maps to the Docker tmpfs mount option in `docker-compose.yml`:

```yaml
tmpfs:
  - /dicom-staging:size=${STAGING_TMPFS_SIZE:-2g},mode=1777
```

> **Note:** tmpfs is backed by RAM (and swap). Ensure the host has enough
> available memory for the size you configure.

**Fix — lower the overflow threshold:**

If you want to spill to encrypted disk overflow earlier (preserving tmpfs
headroom), lower `STAGING_TMPFS_THRESHOLD_PCT`:

```bash
# .env
STAGING_TMPFS_THRESHOLD_PCT=70
```

At 70 % the staging manager switches to the overflow volume sooner, reducing
the chance of a hard `0xA700`.

**Fix — queue backpressure:**

If logs specifically mention "Processing queue full", the anonymization
workers cannot keep up. Increase worker parallelism:

```bash
POOL_MAX_WORKERS=8
```

Or allow a larger queue buffer (default `0` = unlimited):

```bash
QUEUE_MAX_SIZE=500
```

---

## DICOM files are silently dropped (no error, no output)

**Symptom:** `storescu` reports success (`0x0000`) for every file, but no
anonymized output appears in `DATA_DIR`.

**Cause:** The PatientID in the DICOM files is not present in
`recipes/patient_lookup.csv`. The handler logs a warning and returns success
to avoid blocking the sender:

```
SKIPPED: unknown PatientID (not in patient_lookup.csv). SOP <uid> from <AET>
```

**Fix:** Add the missing patient IDs to `deploy/recipes/patient_lookup.csv`
(or the mounted file):

```csv
original,new
REAL_ID_001,ANON_001
```

Then restart the imaging-hub container. See the
[Patient Lookup](../README.md#important-patient-lookup) section in the README
for details.

---

## Association rejected — AE Title not accepted

**Symptom:** The DICOM sender gets an association abort immediately after
requesting an association. Logs show:

```
REJECTED association <id>: Called AE Title 'WRONG_AET' not in accepted list {'ALPHA', 'BETA', 'GAMMA'}
```

**Cause:** The Called AE Title sent by the PACS does not match any entry in
the `scp.ae_titles` list in the imaging-hub config.

**Fix:** Either configure your sending system to use one of the accepted AE
titles, or add the title to `deploy/config/imaging-hub.yaml`:

```yaml
scp:
  ae_titles:
    - "ALPHA"
    - "BETA"
    - "GAMMA"
    - "YOUR_AET"
```

---

## Study rejected with 0xC211

**Symptom:** The sender receives DIMSE status `0xC211`. Logs show:

```
REJECTED: Study UID <uid> not in allowed list
```

**Cause:** `recipes/uuids.txt` contains a whitelist of allowed
StudyInstanceUIDs, and the incoming study is not listed.

**Fix:** Add the StudyInstanceUID to `deploy/recipes/uuids.txt` (one UID per
line), or empty the file to accept all studies.

---

## Port 104 permission denied

**Symptom:** The imaging-hub container fails to start, or the host mapping
for port 104 fails with a permission error.

**Cause:** Port 104 is a privileged port (< 1024). On some Linux
configurations, binding to it requires elevated permissions.

**Fix:** Either run `docker compose` with sufficient privileges, or remap to
a non-privileged port:

```bash
# .env
DICOM_PORT=11112
```

Then point your sending system to the new port.

---

## Container keeps restarting (restart loop)

**Symptom:** `docker compose ps` shows the imaging-hub in a restart loop.

**Cause:** Usually a missing or invalid required environment variable. The
compose file enforces:

- `IMAGING_API_KEY` — shared API key for inter-service auth
- `POSTGRES_PASSWORD` — database credentials

**Fix:** Ensure both are set in your `.env` file:

```bash
IMAGING_API_KEY=your-api-key
POSTGRES_PASSWORD=your-db-password
```

Check container logs for the specific error:

```bash
docker compose logs imaging-hub
```
