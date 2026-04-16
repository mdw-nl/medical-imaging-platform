# Imaging Pipeline — Architecture & Component Reference

## Table of Contents

1. [Overview](#1-overview)
2. [Imaging Hub](#2-imaging-hub)
   - [Reception](#21-reception)
   - [File Storage & Staging](#22-file-storage--staging)
   - [Anonymisation](#23-anonymisation)
   - [Association Tracking](#24-association-tracking)
   - [FastAPI Endpoints](#25-fastapi-endpoints)
3. [Inter-Service Communication](#3-inter-service-communication)
4. [DVH Calculator](#4-dvh-calculator)
   - [Study Discovery & Parallelism](#41-study-discovery--parallelism)
   - [Bundle Verification](#42-bundle-verification)
   - [DVH Calculation](#43-dvh-calculation)
   - [Result Storage](#44-result-storage)
5. [Radiomics Calculator](#5-radiomics-calculator)
   - [Study Discovery & Parallelism](#51-study-discovery--parallelism)
   - [Calculation Pipeline](#52-calculation-pipeline)
   - [Result Storage](#53-result-storage)
6. [PACS Archiver](#6-pacs-archiver)
7. [PostgreSQL Database Schema](#7-postgresql-database-schema)
8. [XNAT](#8-xnat)
9. [Monitoring (Grafana)](#9-monitoring-grafana)
10. [Full Data-Flow Walkthrough](#10-full-data-flow-walkthrough)
11. [Configuration Reference](#11-configuration-reference)
12. [Environment Variables](#12-environment-variables)

---

## 1. Overview

The imaging pipeline receives radiotherapy DICOM files (CT, RTDOSE, RTPLAN, RTSTRUCT), anonymises them, archives them to PACS (XNAT), calculates dose-volume histograms (DVH), and extracts radiomic features. All downstream services discover new work by polling the Imaging Hub's FastAPI endpoint on a cron schedule. All persistent state lives in PostgreSQL.

The project is structured as a Python monorepo with a shared library (`packages/imaging-common`) and four services under `services/`.

```
Hospital scanner / DICOM sender
        |  C-STORE  (port 104)
        v
+------------------+
|   Imaging Hub    |---- staging --> tmpfs --> /dicomsorter/data/
|  (FastAPI :9000) |---- writes metadata --------> PostgreSQL
+------------------+
        |
        | Consumers poll Imaging Hub API (POLL_CRON)
        +---------------------+---------------------+
        |                     |                     |
        v                     v                     v
+---------------+   +------------------+   +---------------+
| DVH Calculator|   |Radiomics Calculat|   | PACS Archiver |
|               |   |                  |   |               |
|  --> Postgres |   |  --> Postgres    |   |  --> XNAT     |
+---------------+   +------------------+   +---------------+

PACS Archiver:
  --> XNAT (C-STORE, port 8104)
```

---

## 2. Imaging Hub

**Source:** `services/imaging-hub/`
**Module:** `imaging_hub`
**Ports:** `104` (DICOM C-STORE), `9000` (FastAPI management)

### 2.1 Reception

The hub runs a pynetdicom Application Entity (AE) in blocking mode on port 104. When a remote DICOM sender opens an association and issues a C-STORE request, the `handle_store()` callback fires for every SOP instance (individual DICOM file) in the association.

Files are **not** passed over HTTP or any custom protocol — they arrive via the DICOM network standard (DIMSE C-STORE). An optional whitelist (`uuids.txt`) can restrict which StudyInstanceUIDs are accepted; rejected files receive DICOM error code `0xC211`.

There is also a FastAPI server on port 9000 exposing management and data-discovery endpoints (health, status, manual triggers, and the polling endpoints used by downstream consumers).

### 2.2 File Storage & Staging

**Raw (pre-anonymisation) DICOM files are never written to the final storage location.** Only the anonymised output is persisted.

Incoming DICOM data passes through a **three-layer staging architecture** before reaching its final path:

1. **tmpfs staging** (`/dicom-staging`, RAM-backed, default 2 GB) — `handle_store()` writes the raw dataset here and returns immediately, decoupling network reception from the slower anonymisation step. This prevents sender timeouts when anonymisation cannot keep up.
2. **Encrypted overflow** (`/dicom-staging-overflow`, persistent volume) — when tmpfs usage exceeds the threshold (default 85 %), new files spill to persistent storage encrypted with Fernet. This ensures clinical data never sits unencrypted on persistent disk.
3. **Final path** — after anonymisation the result is written to its permanent location:

```
/dicomsorter/data/
  {anon_patient_id}/          <- PAT-{UUID}, never the real patient ID
    {study_instance_uid}/
      {modality}/
        {sop_instance_uid}.dcm
```

These anonymised files persist on the shared `ASSOCIATION_DATA` volume. All consumer services (DVH calculator, radiomics-calculator, pacs-archiver) read directly from this volume. No service deletes DICOM data after processing. Downstream consumers look up `file_path` in `dicom_insert` to locate the files.

### 2.3 Anonymisation

Anonymisation is performed in a **multiprocessing pool** (not threads). The pool has a maximum of 4 worker processes.

**Full data path — from wire to disk:**

1. `handle_store()` receives the raw DICOM dataset from the pynetdicom network stream and **writes it to the tmpfs staging area** (`/dicom-staging`). If tmpfs is above the threshold, the file is Fernet-encrypted and written to the overflow volume instead. The callback returns immediately.
2. The pixel array is detached from the dataset and held separately in memory. This reduces the size of the object passed into the worker process.
3. The metadata-only dataset is enqueued and submitted to a worker process via the multiprocessing pool.
4. Inside the worker, the staged file (from tmpfs or decrypted from overflow) is loaded into memory with pydicom.
5. A whitelist-based anonymiser removes all tags not explicitly kept, blanks/replaces others per the recipe, and re-hashes UIDs.
6. The staging file is deleted.
7. The pixel array is reattached to the now-anonymised in-memory dataset.
8. The anonymised dataset is written to its final path under `/dicomsorter/data/{anon_patient_id}/...`.

The raw clinical data therefore exists on disk for only the brief duration of step 4-6 (a few hundred milliseconds per file, in a system temp directory), and is never written to the final storage path. The first and only persistent copy on disk is the anonymised file.

**Other anonymisation operations performed by each worker:**

- Looks up the patient in `patient_lookup.csv` and replaces the real patient ID with a stable anonymised ID (`PAT-{UUID}`). New patients get a freshly minted UUID written back to the CSV.
- For RTSTRUCT modalities, normalises ROI names using regex patterns from `ROI_normalization.yaml` so that structure names are consistent across institutions (e.g. `"Left Lung"`, `"lung_l"`, `"Lung (L)"` all map to the same canonical name).
- Strips all private DICOM tags, then reinserts a controlled set of private tags under a known block (`0x1001`-`0x1009`) carrying project name, trial name, site name, and site ID.

After the worker returns, metadata (anonymised patient IDs, modality, file path, all UIDs) is inserted into the `dicom_insert` and `patient_id_map` tables by the **main process** (database writes are not done inside the worker processes).

### 2.4 Association Tracking

`association_tracker.py` assigns every incoming DICOM association a UUID and tracks per-association and per-patient counters (expected files, processed files, errors).

### 2.5 FastAPI Endpoints

The Imaging Hub exposes several endpoints on port 9000 that downstream consumers poll:

| Endpoint | Consumer | Purpose |
|---|---|---|
| `POST /rt_package` | DVH Calculator | Discover studies with RT data ready for DVH calculation |
| `POST /nifti_package` | Radiomics Calculator | Discover studies ready for NIfTI conversion and radiomics |
| `POST /archive_package` | PACS Archiver | Discover files ready for archiving to XNAT |
| `POST /archive_callback` | PACS Archiver | Report archiving results back to the hub |
| `POST /sop_instance_uids` | General | List SOP instances by modality |

---

## 3. Inter-Service Communication

All downstream consumers discover new work by **polling the Imaging Hub's FastAPI endpoint** on a configurable cron schedule (`POLL_CRON`). There is no message broker.

| Consumer | Endpoint polled | Default schedule |
|---|---|---|
| DVH Calculator | `POST /rt_package` | `*/1 * * * *` (every minute) |
| Radiomics Calculator | `POST /nifti_package` | `*/1 * * * *` (every minute) |
| PACS Archiver | `POST /archive_package` | `*/5 * * * *` (every 5 minutes) |

The shared `APIPoller` class from `imaging-common` handles the cron-based polling logic used by all three consumers.

---

## 4. DVH Calculator

**Source:** `services/dvh-calculator/`
**Module:** `dvh_calculator`
**Discovery:** Polls Imaging Hub API (`/rt_package`) on `POLL_CRON` schedule

### 4.1 Study Discovery & Parallelism

The calculator polls the Imaging Hub's FastAPI endpoint (`DICOM_SERVICE_URL`) on a cron schedule (default: every minute) to discover studies ready for processing. Each discovered study is handled end-to-end: database lookup -> file reading -> DVH calculation -> result storage.

### 4.2 Bundle Verification

Before any calculation starts, the consumer checks that all four required DICOM modalities are present for the study:

- `CT` — the planning CT scan
- `RTSTRUCT` — structure set (ROI definitions)
- `RTPLAN` — treatment plan (provides prescription dose)
- `RTDOSE` — dose distribution grid

If any modality is missing the study is skipped and retried on the next poll cycle, allowing for the case where files from the same study arrive via separate associations.

### 4.3 DVH Calculation

`DVH/dvh.py` uses the `dicompylercore` library.

1. CT, RTPLAN, RTDOSE, and RTSTRUCT are loaded from disk into memory.
2. Combined ROIs are built if configured (e.g. `"P-LUNG"` = left lung + right lung, with Boolean union of their contour masks).
3. For each ROI:
   - `dvhcalc._calculate_dvh()` computes the dose-volume histogram on the dose grid.
   - If an RTPLAN is available, the DVH is converted from absolute dose to relative-to-prescription dose.
   - The following metrics are extracted:
     - **Dose at volume:** D2, D50, D95, D98
     - **Volume at dose:** V0, V15, V35
     - **Summary:** min dose, mean dose, max dose
4. Results are serialised to a JSON-LD structure with `@id`, `structureName`, `dose_bins`, `volume_bins`, and all metrics.

### 4.4 Result Storage

Results are written to PostgreSQL by `postgres_dvh.py`:

- One row in `dvh_result` per ROI per study.
- One row in `dvh_package` linking the RTDOSE SOP UID and ROI name back to the `dvh_result` row.
- `calculation_status` is updated with `status = TRUE` on success or `status = FALSE` on error.

Optionally, results can be uploaded to XNAT (`UPLOAD_DESTINATION=xnat`) or sent to a GDP endpoint instead of (or in addition to) PostgreSQL.

---

## 5. Radiomics Calculator

**Source:** `services/radiomics-calculator/`
**Module:** `radiomics_calculator`
**Discovery:** Polls Imaging Hub API (`/nifti_package`) on `POLL_CRON` schedule

### 5.1 Study Discovery & Parallelism

Identical polling pattern to the DVH Calculator: polls `DICOM_SERVICE_URL` on a cron schedule. Both consumers run independently and concurrently — a study can be undergoing DVH calculation and radiomics extraction simultaneously.

ROIs listed in `SKIP_ROIS` (e.g. `Body,Shoulders,Posterior_Neck,RingPTVLow,RingPTVHigh`) are excluded from feature extraction. Matching is case-sensitive.

### 5.2 Calculation Pipeline

`radiomics_calculator.py` processes one study at a time per thread:

1. **File Discovery:** Walks the study directory (looked up from `dicom_insert.file_path`) to find the RTSTRUCT `.dcm` file and all CT `.dcm` files in the same directory.

2. **Format Conversion:** Uses `platipy` to convert DICOM CT + RTSTRUCT into NIfTI format. This creates a temporary `niftidata/` folder:
   ```
   niftidata/
     image.nii.gz          <- CT volume
     Mask_Lung_L.nii.gz    <- one binary mask per ROI
     Mask_Lung_R.nii.gz
     ...
   ```
   The conversion step is the most time-consuming part of the pipeline.

3. **Feature Extraction:** PyRadiomics processes each NIfTI mask against the CT image using the settings in `radiomics_settings/Params.yaml`. For each ROI, approximately 109 features are extracted across seven feature classes:

   | Class | # Features | Examples |
   |---|---|---|
   | Shape | 14 | SurfaceArea, Sphericity, Elongation, Flatness, MeshVolume |
   | First Order | 18 | Mean, Median, Energy, Entropy, Kurtosis, Skewness |
   | GLCM | 25 | Contrast, Correlation, Homogeneity, JointEntropy, MCC |
   | GLRLM | 16 | RunLengthNonUniformity, ShortRunEmphasis |
   | GLSZM | 16 | ZoneEntropy, LargeAreaEmphasis |
   | GLDM | 15 | DependenceNonUniformity, SmallDependenceEmphasis |
   | NGTDM | 5 | Coarseness, Contrast, Strength |

4. Results are collected into an in-memory CSV (one row per ROI).

### 5.3 Result Storage

`radiomics_results_postgress.py` writes two tables:

- **`radiomics_manager`**: one row per study calculation. Stores a UUID primary key (`radiomics_id`), the RTSTRUCT `sop_instance_uid`, and `created_at`.
- **`radiomics_results`**: one row per ROI. Stores the `radiomics_id` foreign key, `roi_name`, 15 diagnostic columns (image hash, voxel spacing, mask statistics), and all ~109 feature columns. Missing features are stored as `NULL`.

Optionally, results are also uploaded to XNAT as a CSV attachment on the session (`SEND_XNAT=True`).

---

## 6. PACS Archiver

**Source:** `services/pacs-archiver/`
**Module:** `pacs_archiver`
**Discovery:** Polls Imaging Hub API (`/archive_package`) on `POLL_CRON` schedule (default: every 5 minutes)

The PACS Archiver is a standalone service that discovers pending files by polling the Imaging Hub and sends them to XNAT via DICOM C-STORE.

**Process:**

1. Polls the Imaging Hub's `/archive_package` endpoint for files with `status = 'pending'`.
2. Reads each DICOM file from disk.
3. Opens a DICOM association to the PACS (XNAT on `xnat-web:8104`) using the project name as the Called AE Title, and sends via C-STORE.
4. A background verifier thread validates the archiving status against the XNAT API.
5. Reports results back to the Imaging Hub via the `/archive_callback` endpoint.

---

## 7. PostgreSQL Database Schema

All services share a single PostgreSQL 13 instance. Initialisation scripts in `deploy/postgres/` are mounted into `docker-entrypoint-initdb.d` and run on first start:

- `init_xnat.sh` — creates the XNAT user and database

Application-level tables (`dicom_insert`, `patient_id_map`, `associations`, etc.) are created by the Imaging Hub and consumer services on startup, not by init scripts in this repo.

### `dicom_insert`
Written by the Imaging Hub. One row per received DICOM file.

| Column | Type | Notes |
|---|---|---|
| `patient_id` | text | Anonymised patient ID (`PAT-{UUID}`) |
| `patient_name` | text | Anonymised patient name |
| `study_instance_uid` | text | DICOM Study UID |
| `series_instance_uid` | text | DICOM Series UID |
| `sop_instance_uid` | text | **Unique.** Individual file identifier |
| `modality` | text | CT, RTSTRUCT, RTPLAN, RTDOSE, etc. |
| `file_path` | text | Absolute path to the `.dcm` file on disk |
| `referenced_rt_plan_uid` | text | For RTDOSE/RTSTRUCT: which plan they belong to |
| `referenced_rtstruct_sop_uid` | text | For RTPLAN/RTDOSE: the RTSTRUCT they reference |
| `referenced_ct_series_uid` | text | For RTSTRUCT: the CT series it was drawn on |
| `assoc_id` | uuid | The DICOM association that delivered this file |
| `timestamp` | timestamptz | Receipt time |

### `patient_id_map`
Written by the Imaging Hub anonymiser. One row per unique patient.

| Column | Type | Notes |
|---|---|---|
| `original_patient_id` | text | Original (identifiable) patient ID |
| `generated_patient_id` | text | Anonymised ID (`PAT-{UUID}`) |
| `created_at` | timestamptz | When the mapping was first created |

### `associations`
Written by the Imaging Hub. One row per DICOM association (connection).

| Column | Type |
|---|---|
| `assoc_id` | uuid |
| `ae_title` | text |
| `ip_address` | text |
| `port` | int |
| `timestamp` | timestamptz |

### `pacs_archive`
Written by the Imaging Hub; updated by the PACS Archiver.

| Column | Type | Notes |
|---|---|---|
| `sop_instance_uid` | text PK | |
| `series_instance_uid` | text | |
| `study_instance_uid` | text | |
| `modality` | text | |
| `patient_id` | text | |
| `queued_at` | timestamptz | When added to the archive queue |
| `archived_at` | timestamptz | When archiving completed (NULL until done) |
| `status` | text | `'pending'` -> `'archived'` |

### `calculation_status`
Written by DVH Calculator and Radiomics Calculator.

| Column | Type | Notes |
|---|---|---|
| `study_uid` | text | |
| `status` | boolean | `TRUE` = success, `FALSE` = error |
| `timestamp` | timestamptz | |

### `dvh_result`
Written by the DVH Calculator.

| Column | Type | Notes |
|---|---|---|
| `result_id` | serial PK | |
| `json_id` | text unique | Composite identifier for the ROI |
| `dose_bins` | float[] | DVH dose axis |
| `volume_bins` | float[] | DVH volume axis |
| `d2`, `d50`, `d95`, `d98` | float | Dose at 2/50/95/98 % volume |
| `min_dose`, `mean_dose`, `max_dose` | float | |
| `v0`, `v15`, `v35` | float | Volume at 0/15/35 Gy |

### `dvh_package`
Links DVH results to source DICOM files.

| Column | Type |
|---|---|
| `sop_instance_uid` | text (RTDOSE UID) |
| `roi_name` | text |
| `result_id` | int FK -> `dvh_result` |

### `radiomics_manager`
Written by the Radiomics Calculator. One row per completed radiomics run.

| Column | Type |
|---|---|
| `radiomics_id` | uuid PK |
| `sop_instance_uid` | text (RTSTRUCT UID) |
| `created_at` | timestamptz |

### `radiomics_results`
Written by the Radiomics Calculator. One row per ROI per run.

| Column | Type | Notes |
|---|---|---|
| `id` | serial PK | |
| `radiomics_id` | uuid FK -> `radiomics_manager` | |
| `roi_name` | text | Derived from NIfTI mask filename |
| `diag_*` (15 cols) | float/text | Image hash, voxel spacing, mask voxel counts, etc. |
| `original_*` (~109 cols) | float | One column per PyRadiomics feature |

---

## 8. XNAT

XNAT serves two roles:

1. **PACS archive**: The PACS Archiver sends anonymised DICOM files to XNAT via C-STORE on port 8104. XNAT stores them as a standard DICOM archive and makes them browsable.
2. **Result upload** (optional): Both DVH Calculator and Radiomics Calculator can upload results as file attachments on XNAT sessions when configured.

XNAT (`ghcr.io/mdw-nl/mdw-xnat:1.9`) is backed by the shared PostgreSQL instance (using a separate database/schema) and exposes a web UI on port 8080.

---

## 9. Monitoring (Grafana)

Grafana connects directly to PostgreSQL as a read-only datasource and provides a pre-provisioned **Pipeline Overview** dashboard. No additional instrumentation is needed in any service — all data comes from existing tables. The dashboard is set as the Grafana home page.

**Access:** `http://localhost:3000` (credentials from `.env` or default `admin`/`admin`)

| Panel | Source table |
|---|---|
| DICOM files received (total) | `dicom_insert` |
| DICOM files by modality | `dicom_insert` |
| Unique studies | `dicom_insert` |
| Patients anonymised | `patient_id_map` |
| PACS archived / pending | `pacs_archive` |
| DVH completed / errors | `calculation_status` |
| Radiomics jobs completed | `radiomics_manager` |
| DICOM receipt rate (time series) | `dicom_insert.timestamp` |
| PACS archiving rate (time series) | `pacs_archive.archived_at` |
| DVH completion rate (time series) | `calculation_status.timestamp` |
| Radiomics completion rate (time series) | `radiomics_manager.created_at` |

Provisioning files:

```
deploy/monitoring/grafana/
+-- provisioning/
|   +-- dashboards/
|   |   +-- dashboard-provider.yaml    <- points Grafana at /var/lib/grafana/dashboards/
|   +-- datasources/
|       +-- datasources.yaml           <- PostgreSQL datasource (uid: pipeline-postgres)
+-- dashboards/
    +-- pipeline-overview.json         <- full dashboard definition, loaded at startup
```

---

## 10. Full Data-Flow Walkthrough

This traces a single patient's journey through the pipeline from scanner to results.

**Step 1 — DICOM reception**
The scanner (or a DICOM routing appliance) opens a DICOM association to port 104 and sends C-STORE requests. The Imaging Hub's `handle_store()` fires for each file and writes it to the **tmpfs staging area** (`/dicom-staging`). If tmpfs usage exceeds the threshold, the file is Fernet-encrypted and spills to the overflow volume. The callback returns immediately, preventing sender timeouts.

**Step 2 — Anonymisation (multiprocessing)**
Up to 4 worker processes run concurrently. Each worker:
- Reads the staged file from tmpfs (or decrypts from overflow)
- Applies whitelist-based anonymisation: only explicitly kept tags survive; everything else is removed
- Replaces the patient ID with `PAT-{UUID}` (stable across re-sends of the same patient)
- Normalises ROI names for RTSTRUCT files
- Writes the anonymised result to its final disk path; deletes the staging file

**Step 3 — Database record creation**
Once a worker finishes, the main process inserts into `dicom_insert` and `patient_id_map`. Simultaneously, the file's SOP instance is added to `pacs_archive` with `status='pending'`.

**Step 4 — Study becomes available**
The Imaging Hub's FastAPI endpoint exposes the study as ready for processing via the `/rt_package`, `/nifti_package`, and `/archive_package` endpoints.

**Step 5 — DVH, Radiomics, and PACS Archiver discover the study**
All three consumers poll the Imaging Hub API on their respective `POLL_CRON` schedules. When a new study appears, all begin processing independently. DVH Calculator and Radiomics Calculator read files from the same disk paths stored in `dicom_insert`.

**Step 6 — DVH Calculator**
Checks that CT + RTSTRUCT + RTPLAN + RTDOSE are all present. If not, skips until next poll. Otherwise, calculates DVH curves for all structures (and any configured combined structures), writes to `dvh_result` and `dvh_package`, updates `calculation_status`.

**Step 7 — Radiomics Calculator**
Finds RTSTRUCT and CT files, converts them to NIfTI with platipy, extracts ~109 PyRadiomics features per ROI, writes to `radiomics_manager` and `radiomics_results`, updates `calculation_status`.

**Step 8 — PACS Archiver**
Polls every 5 minutes, sends pending files to XNAT via C-STORE, verifies archiving via the XNAT API, and reports results back to the Imaging Hub via `/archive_callback`.

---

## 11. Configuration Reference

All services load a YAML config file mounted at runtime. The config files live in `deploy/config/`.

### Imaging Hub — `deploy/config/imaging-hub.yaml`

```yaml
postgres:
  host: postgres
  port: 5432
  username: ...
  password: ...
  db: ...

anonymization:
  patient_name: "..."
  profile_name: "..."
  project_name: "..."
  trial_name: "..."
  site_name: "..."
  site_id: "..."
  uid_secret: "..."       # REQUIRED — override via IMAGING__ANONYMIZATION__UID_SECRET
  uid_prefix: "99999."
```

### DVH Calculator — `deploy/config/dvh.yaml`

```yaml
postgres:
  host: postgres
  port: 5432
  username: ...
  password: ...
  db: ...

dvh-calculations:
  - P-LUNG:
      roi: "LUNG_L + LUNG_R"
  - MeanDoseIpsiLateralParotidGland:
      roi: "PAROTID_IPSI"
```

### Radiomics Calculator — `deploy/config/radiomics.yaml`

```yaml
postgres:
  host: postgres
  port: 5432
  username: ...
  password: ...
  db: ...
```

### PACS Archiver — `deploy/config/pacs-archiver.yaml`

```yaml
postgres:
  host: postgres
  port: 5432
  username: ...
  password: ...
  db: ...
```

### Radiomics Feature Settings — `deploy/radiomics-settings/Params.yaml`

Standard PyRadiomics configuration. Controls image normalisation, bin width, enabled feature classes, and image filters (e.g. wavelet, Laplacian of Gaussian).

### Anonymisation Recipes — `deploy/recipes/`

| File | Purpose |
|---|---|
| `recipe.dicom` | Anonymisation recipe: tag-level actions (keep, blank, add, replace) |
| `patient_lookup.csv` | `original_id,anonymised_id` mapping file (read/written by anonymiser) |
| `ROI_normalization.yaml` | Regex -> canonical name mappings for RTSTRUCT ROI names |
| `uuids.txt` | Optional whitelist of allowed StudyInstanceUIDs |

---

## 12. Environment Variables

### Imaging Hub

| Variable | Default | Effect |
|---|---|---|
| `USE_NIFTI` | `true` | Enable NIfTI conversion support |
| `DEFER_NIFTI` | `true` | Defer NIfTI conversion to reduce peak memory usage |
| `LOG_LEVEL` | `INFO` | Python logging level |
| `DICOM_PORT` | `104` | Host port mapped to the DICOM listener |
| `STAGING_TMPFS_SIZE` | `2g` | Size of the RAM-backed tmpfs mount (counts against container memory limit) |
| `STAGING_TMPFS_DIR` | `/dicom-staging` | RAM-backed staging directory for incoming DICOM |
| `STAGING_OVERFLOW_DIR` | `/dicom-staging-overflow` | Encrypted overflow when tmpfs fills up |
| `STAGING_TMPFS_THRESHOLD_PCT` | `85` | tmpfs usage % before spilling to overflow |
| `POOL_MAX_WORKERS` | `4` | Number of parallel anonymisation worker processes |
| `QUEUE_MAX_SIZE` | `0` | Max files queued for anonymisation (`0` = unlimited) |
| `STAGING_ENCRYPT_OVERFLOW` | `true` | Fernet-encrypt overflow files; disable to reduce CPU on constrained hosts |
| `DATA_DIR` | `/dicomsorter/data` | Final anonymised file storage |
| `CONFIG_PATH` | — | Path to YAML config file |
| `RECIPES_PATH` | — | Path to anonymisation recipes directory |
| `IMAGING_API_KEY` | — | Shared API key for inter-service authentication |

### DVH Calculator

| Variable | Default | Effect |
|---|---|---|
| `DICOM_SERVICE_URL` | — | Imaging Hub FastAPI URL (e.g. `http://imaging-hub:9000`) |
| `POLL_CRON` | `*/1 * * * *` | Cron schedule for polling the Imaging Hub |
| `UPLOAD_DESTINATION` | `postgres` | `postgres`, `xnat`, or `gdp` |

### Radiomics Calculator

| Variable | Default | Effect |
|---|---|---|
| `DICOM_SERVICE_URL` | — | Imaging Hub FastAPI URL (e.g. `http://imaging-hub:9000`) |
| `POLL_CRON` | `*/1 * * * *` | Cron schedule for polling the Imaging Hub |
| `SKIP_ROIS` | — | Comma-separated ROI names to exclude from extraction |
| `SEND_POSTGRES` | `true` | Write results to PostgreSQL |
| `SEND_XNAT` | `false` | Upload result CSV to XNAT session |

### PACS Archiver

| Variable | Default | Effect |
|---|---|---|
| `DICOM_SERVICE_URL` | — | Imaging Hub FastAPI URL (e.g. `http://imaging-hub:9000`) |
| `POLL_CRON` | `*/5 * * * *` | Cron schedule for polling the Imaging Hub |
| `PACS_SCP_HOST` | `xnat-web` | Hostname of the PACS (C-STORE target) |
| `PACS_SCP_PORT` | `8104` | DICOM port of the PACS |
| `PACS_SCP_AE_TITLE` | *(none)* | Fallback Called AE title (normally derived from project) |
| `XNAT_API_URL` | `http://xnat-web:8080` | XNAT REST API URL |
| `XNAT_API_USER` | `admin` | XNAT API username |
| `XNAT_API_PASSWORD` | `admin` | XNAT API password |
| `XNAT_PROJECT` | *(none)* | XNAT project identifier |
