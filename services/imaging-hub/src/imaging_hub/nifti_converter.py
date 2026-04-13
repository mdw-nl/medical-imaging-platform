"""Convert RTSTRUCT + CT DICOM pairs to NIfTI masks in background processes."""

import logging
import shutil
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

from imaging_hub.settings import BASE_DIR

logger = logging.getLogger(__name__)


def _convert_in_process(ct_folder, rtstruct_file, tmp_dir):
    import builtins  # noqa: PLC0415 — must import inside subprocess worker

    original_quit = builtins.quit

    def _no_quit(*_args, **_kwargs):
        raise RuntimeError("platipy called quit() — contour geometry error")

    builtins.quit = _no_quit
    try:
        from platipy.dicom.io.rtstruct_to_nifti import convert_rtstruct  # noqa: PLC0415 — lazy import in subprocess

        convert_rtstruct(
            dcm_img=ct_folder,
            dcm_rt_file=rtstruct_file,
            output_img="image.nii",
            prefix="Mask_",
            output_dir=tmp_dir,
        )
    finally:
        builtins.quit = original_quit


def _make_db(db_params):
    from imaging_common.database import PostgresInterface  # noqa: PLC0415 — fresh import in subprocess

    db = PostgresInterface(**db_params)
    db.connect()
    return db


def _convert_task(db_params, study_uid, patient_id, rtstruct_sop_uid, ct_series_uid):
    db = _make_db(db_params)
    nifti_dir = str(Path(BASE_DIR) / patient_id / study_uid / "NIFTI" / rtstruct_sop_uid)

    try:
        rtstruct_row = db.fetch_one(
            "SELECT file_path FROM dicom_insert WHERE sop_instance_uid = %s",
            (rtstruct_sop_uid,),
        )
        if not rtstruct_row:
            raise FileNotFoundError(f"RTSTRUCT file not found in DB for SOP {rtstruct_sop_uid}")
        rtstruct_file = rtstruct_row[0]

        ct_row = db.fetch_one(
            "SELECT file_path FROM dicom_insert WHERE series_instance_uid = %s AND modality = 'CT' LIMIT 1",
            (ct_series_uid,),
        )
        if not ct_row:
            raise FileNotFoundError(f"CT series not found in DB for series {ct_series_uid}")
        ct_folder = str(Path(ct_row[0]).parent)

        tmp_dir = nifti_dir + ".tmp"
        if Path(tmp_dir).exists():
            shutil.rmtree(tmp_dir)
        Path(tmp_dir).mkdir(parents=True, exist_ok=True)

        _convert_in_process(ct_folder, rtstruct_file, tmp_dir)

        if Path(nifti_dir).exists():
            shutil.rmtree(nifti_dir)
        Path(tmp_dir).rename(nifti_dir)

        masks = []
        for p in Path(nifti_dir).iterdir():
            if p.is_file() and p.name != "image.nii.gz":
                roi_name = p.stem.replace("Mask_", "").replace(".nii", "")
                masks.append((roi_name, str(p)))

        conversion_row = db.fetch_one(
            "SELECT id FROM nifti_conversion WHERE rtstruct_sop_uid = %s",
            (rtstruct_sop_uid,),
        )
        if not conversion_row:
            raise RuntimeError(f"nifti_conversion row missing for {rtstruct_sop_uid}")
        conversion_id = conversion_row[0]

        for roi_name, file_path in masks:
            db.execute_query(
                "INSERT INTO nifti_masks (nifti_conversion_id, roi_name, file_path) VALUES (%s, %s, %s)",
                (conversion_id, roi_name, file_path),
            )

        db.execute_query(
            "UPDATE nifti_conversion SET status = 'completed', mask_count = %s, completed_at = %s "
            "WHERE rtstruct_sop_uid = %s",
            (len(masks), datetime.now(UTC), rtstruct_sop_uid),
        )
        logging.getLogger(__name__).info(
            "NIfTI conversion complete for RTSTRUCT %s: %d masks in %s",
            rtstruct_sop_uid,
            len(masks),
            nifti_dir,
        )

    except Exception as exc:
        logging.getLogger(__name__).exception("NIfTI conversion failed for RTSTRUCT %s", rtstruct_sop_uid)
        db.execute_query(
            "UPDATE nifti_conversion SET status = 'failed', error_message = %s, completed_at = %s "
            "WHERE rtstruct_sop_uid = %s",
            (str(exc), datetime.now(UTC), rtstruct_sop_uid),
        )
    finally:
        db.disconnect()


class NiftiConverter:
    """Schedule and run RTSTRUCT-to-NIfTI conversions in a process pool."""

    def __init__(self, db):
        self._db = db
        self._db_params = {
            "host": db.host,
            "database": db.database,
            "user": db.user,
            "password": db.password,
            "port": db.port,
        }
        self._executor = ProcessPoolExecutor(max_workers=2, mp_context=None)

    def record_pending(self, study_uid, patient_id):
        """Insert pending nifti_conversion rows without starting the actual conversion."""
        rtstruct_rows = self._db.fetch_all(
            "SELECT sop_instance_uid, referenced_ct_series_uid FROM dicom_insert "
            "WHERE study_instance_uid = %s AND modality = 'RTSTRUCT'",
            (study_uid,),
        )
        if not rtstruct_rows:
            return
        for rtstruct_sop_uid, ct_series_uid in rtstruct_rows:
            if not ct_series_uid:
                logger.warning("RTSTRUCT %s has no referenced CT series, skipping", rtstruct_sop_uid)
                continue
            existing = self._db.fetch_one(
                "SELECT status FROM nifti_conversion WHERE rtstruct_sop_uid = %s",
                (rtstruct_sop_uid,),
            )
            if existing:
                continue
            self._db.execute_query(
                "INSERT INTO nifti_conversion "
                "(study_instance_uid, patient_id, rtstruct_sop_uid, ct_series_uid, nifti_dir, image_path, status, started_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, 'pending', %s)",
                (
                    study_uid,
                    patient_id,
                    rtstruct_sop_uid,
                    ct_series_uid,
                    str(Path(BASE_DIR) / patient_id / study_uid / "NIFTI" / rtstruct_sop_uid),
                    str(Path(BASE_DIR) / patient_id / study_uid / "NIFTI" / rtstruct_sop_uid / "image.nii.gz"),
                    datetime.now(UTC),
                ),
            )

    def run_pending(self):
        """Submit all pending nifti_conversion rows to the process pool."""
        rows = self._db.fetch_all(
            "SELECT study_instance_uid, patient_id, rtstruct_sop_uid, ct_series_uid "
            "FROM nifti_conversion WHERE status = 'pending'",
        )
        if not rows:
            return
        for study_uid, patient_id, rtstruct_sop_uid, ct_series_uid in rows:
            future = self._executor.submit(
                _convert_task, self._db_params, study_uid, patient_id, rtstruct_sop_uid, ct_series_uid
            )
            future.add_done_callback(lambda f, sop=rtstruct_sop_uid: self._on_done(f, sop))
        logger.info("Submitted %d pending NIfTI conversions", len(rows))

    def schedule(self, study_uid, patient_id):
        """Record and immediately execute NIfTI conversions (eager mode)."""
        self.record_pending(study_uid, patient_id)
        self.run_pending()

    def _on_done(self, future, rtstruct_sop_uid):
        exc = future.exception()
        if exc is not None:
            logger.error("NIfTI process died for RTSTRUCT %s: %s", rtstruct_sop_uid, exc)
            self._db.execute_query(
                "UPDATE nifti_conversion SET status = 'failed', error_message = %s, completed_at = %s "
                "WHERE rtstruct_sop_uid = %s",
                (str(exc), datetime.now(UTC), rtstruct_sop_uid),
            )
