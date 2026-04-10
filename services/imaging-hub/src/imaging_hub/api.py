"""FastAPI endpoints for downstream services to pull DICOM metadata and packages."""

import logging
import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from imaging_common import PostgresInterface, load_settings

logger = logging.getLogger(__name__)

_NOT_SENT = "AND {alias}.sop_instance_uid NOT IN (SELECT sop_instance_uid FROM calculation_status WHERE modality = '{mod}' AND status = TRUE)"

_RT_CHAIN_CONFIG: dict[str, tuple[str, list[str]]] = {
    "RTDOSE": (
        """
        SELECT dose.sop_instance_uid, dose.patient_id, dose.study_instance_uid,
               plan.sop_instance_uid, struct.sop_instance_uid, struct.referenced_ct_series_uid
        FROM dicom_insert dose
        JOIN dicom_insert plan  ON plan.sop_instance_uid  = dose.referenced_rt_plan_uid      AND plan.modality   = 'RTPLAN'
        JOIN dicom_insert struct ON struct.sop_instance_uid = plan.referenced_rtstruct_sop_uid AND struct.modality = 'RTSTRUCT'
        WHERE dose.modality = 'RTDOSE'
        """
        + _NOT_SENT.format(alias="dose", mod="RTDOSE"),
        ["rtdose_sop_uid", "patient_id", "study_uid", "rtplan_sop_uid", "rtstruct_sop_uid", "ct_series_uid"],
    ),
    "RTPLAN": (
        """
        SELECT plan.sop_instance_uid, plan.patient_id, plan.study_instance_uid,
               struct.sop_instance_uid, struct.referenced_ct_series_uid
        FROM dicom_insert plan
        JOIN dicom_insert struct ON struct.sop_instance_uid = plan.referenced_rtstruct_sop_uid AND struct.modality = 'RTSTRUCT'
        WHERE plan.modality = 'RTPLAN'
        """
        + _NOT_SENT.format(alias="plan", mod="RTPLAN"),
        ["rtplan_sop_uid", "patient_id", "study_uid", "rtstruct_sop_uid", "ct_series_uid"],
    ),
    "RTSTRUCT": (
        """
        SELECT struct.sop_instance_uid, struct.patient_id, struct.study_instance_uid,
               struct.referenced_ct_series_uid
        FROM dicom_insert struct
        WHERE struct.modality = 'RTSTRUCT'
        """
        + _NOT_SENT.format(alias="struct", mod="RTSTRUCT"),
        ["rtstruct_sop_uid", "patient_id", "study_uid", "ct_series_uid"],
    ),
}

_ARCHIVE_PACKAGE_SQL = """
    SELECT di.sop_instance_uid, di.series_instance_uid, di.modality,
           di.study_instance_uid, di.patient_id, di.file_path, di.project
    FROM dicom_insert di
    LEFT JOIN pacs_archive pa
        ON pa.sop_instance_uid = di.sop_instance_uid
    WHERE pa.sop_instance_uid IS NULL
       OR (pa.status != 'archived' AND pa.status != 'in_progress')
    ORDER BY di.study_instance_uid
"""

_API_KEY = os.environ.get("IMAGING_API_KEY")
if not _API_KEY:
    raise RuntimeError("IMAGING_API_KEY environment variable must be set")


class ModalityRequest(BaseModel):
    """Request body carrying a single DICOM modality identifier."""

    modality: str


class ArchiveCallbackItem(BaseModel):
    """Single archive result reported back by the PACS archiver."""

    sop_instance_uid: str
    success: bool


class ArchiveCallbackRequest(BaseModel):
    """Batch of archive results from the PACS archiver."""

    results: list[ArchiveCallbackItem]


_config_path = Path(os.getenv("CONFIG_PATH", str(Path(__file__).parents[2] / "config" / "config.yaml")))
_settings = load_settings(_config_path)
db = PostgresInterface.from_settings(_settings.postgres)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.connect()
    yield
    db.disconnect()


app = FastAPI(lifespan=lifespan)


@app.middleware("http")
async def api_key_middleware(request: Request, call_next):
    key = request.headers.get("X-API-Key")
    if key != _API_KEY:
        return JSONResponse(status_code=401, content={"detail": "Invalid or missing API key"})
    return await call_next(request)


@app.post("/sop_instance_uids")
async def get_new_sop_instance_uids(request: ModalityRequest):
    """Return SOP Instance UIDs not yet sent for the given modality and mark them as sent."""
    modality = request.modality

    try:
        sql_query = """
        SELECT sop_instance_uid, study_instance_uid, patient_name
        FROM dicom_insert
        WHERE modality = %s
        AND sop_instance_uid NOT IN (
            SELECT sop_instance_uid
            FROM calculation_status
            WHERE status = TRUE AND modality = %s
        )
        """
        results = db.fetch_all(sql_query, (modality, modality))

        new_sops = (
            [{"sop_instance_uid": row[0], "study_instance_uid": row[1], "patient_name": row[2]} for row in results]
            if results
            else []
        )

        for sop in new_sops:
            db.execute_query(
                """
                INSERT INTO calculation_status (sop_instance_uid, modality, status, timestamp)
                VALUES (%s, %s, TRUE, %s)
                """,
                (sop["sop_instance_uid"], modality, datetime.now(UTC)),
            )

    except Exception as exc:
        logger.exception("Error in /sop_instance_uids")
        raise HTTPException(status_code=500, detail="Internal server error") from exc
    else:
        return {"modality": modality, "new_sop_instances": new_sops}


@app.post("/nifti_package")
async def get_nifti_packages(request: ModalityRequest):
    """Return completed NIfTI conversion packages not yet dispatched for radiomics."""
    sql = """
        SELECT nc.rtstruct_sop_uid, nc.patient_id, nc.study_instance_uid,
               nc.nifti_dir, nc.image_path, nc.ct_series_uid
        FROM nifti_conversion nc
        WHERE nc.status = 'completed'
          AND nc.rtstruct_sop_uid NOT IN (
              SELECT sop_instance_uid FROM calculation_status
              WHERE modality = 'NIFTI_RADIOMICS' AND status = TRUE
          )
    """
    try:
        rows = db.fetch_all(sql, ())
        packages = []
        for row in rows:
            rtstruct_sop_uid = row[0]
            masks = db.fetch_all(
                "SELECT roi_name, file_path FROM nifti_masks WHERE nifti_conversion_id = "
                "(SELECT id FROM nifti_conversion WHERE rtstruct_sop_uid = %s)",
                (rtstruct_sop_uid,),
            )
            packages.append(
                {
                    "rtstruct_sop_uid": rtstruct_sop_uid,
                    "patient_id": row[1],
                    "study_uid": row[2],
                    "nifti_dir": row[3],
                    "image_path": row[4],
                    "ct_series_uid": row[5],
                    "masks": [{"roi_name": m[0], "file_path": m[1]} for m in masks],
                }
            )
            db.execute_query(
                "INSERT INTO calculation_status (sop_instance_uid, modality, status, timestamp) VALUES (%s, %s, TRUE, %s)",
                (rtstruct_sop_uid, "NIFTI_RADIOMICS", datetime.now(UTC)),
            )
    except Exception as exc:
        logger.exception("Error in /nifti_package")
        raise HTTPException(status_code=500, detail="Internal server error") from exc
    else:
        return {"packages": packages}


@app.post("/rt_package")
async def get_rt_package(request: ModalityRequest):
    """Return RT chain packages (RTDOSE/RTPLAN/RTSTRUCT) with linked references."""
    modality = request.modality.upper()

    if modality not in _RT_CHAIN_CONFIG:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported modality '{modality}'. Must be one of: {sorted(_RT_CHAIN_CONFIG)}",
        )

    sql, columns = _RT_CHAIN_CONFIG[modality]
    anchor_col = columns[0]

    try:
        rows = db.fetch_all(sql, ())
        packages = [dict(zip(columns, row, strict=False)) for row in rows]
        for pkg in packages:
            db.execute_query(
                "INSERT INTO calculation_status (sop_instance_uid, modality, status, timestamp) VALUES (%s, %s, TRUE, %s)",
                (pkg[anchor_col], modality, datetime.now(UTC)),
            )
    except Exception as exc:
        logger.exception("Error in /rt_package")
        raise HTTPException(status_code=500, detail="Internal server error") from exc
    else:
        return {"modality": modality, "packages": packages}


@app.post("/archive_package")
async def get_archive_package():
    """Return DICOM instances not yet archived to PACS and mark them in-progress."""
    try:
        rows = db.fetch_all(_ARCHIVE_PACKAGE_SQL, ())
        if not rows:
            return {"packages": []}

        packages = []
        for row in rows:
            sop_uid = row[0]
            db.execute_query(
                """
                INSERT INTO pacs_archive (sop_instance_uid, series_instance_uid, modality, study_instance_uid, patient_id, status, project)
                VALUES (%s, %s, %s, %s, %s, 'in_progress', %s)
                ON CONFLICT (sop_instance_uid) DO UPDATE SET status = 'in_progress'
                """,
                (sop_uid, row[1], row[2], row[3], row[4], row[6]),
            )
            packages.append(
                {
                    "sop_instance_uid": sop_uid,
                    "series_instance_uid": row[1],
                    "modality": row[2],
                    "study_instance_uid": row[3],
                    "patient_id": row[4],
                    "file_path": row[5],
                    "project": row[6],
                }
            )
    except Exception as exc:
        logger.exception("Error in /archive_package")
        raise HTTPException(status_code=500, detail="Internal server error") from exc
    else:
        return {"packages": packages}


@app.post("/archive_callback")
async def archive_callback(request: ArchiveCallbackRequest):
    """Update archive status for each SOP based on archiver-reported results."""
    try:
        for item in request.results:
            if item.success:
                db.execute_query(
                    "UPDATE pacs_archive SET status = 'archived', archived_at = NOW() WHERE sop_instance_uid = %s",
                    (item.sop_instance_uid,),
                )
            else:
                db.execute_query(
                    "UPDATE pacs_archive SET status = 'pending' WHERE sop_instance_uid = %s",
                    (item.sop_instance_uid,),
                )
    except Exception as exc:
        logger.exception("Error in /archive_callback")
        raise HTTPException(status_code=500, detail="Internal server error") from exc
    else:
        return {"status": "ok"}
