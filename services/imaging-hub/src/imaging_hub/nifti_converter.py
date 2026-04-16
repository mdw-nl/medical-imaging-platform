"""Convert RTSTRUCT + CT DICOM pairs to NIfTI masks in background processes.

Uses dcm2niix for CT-to-NIfTI conversion (C binary, ~50 MB) and a custom
rasterizer that processes one ROI mask at a time to keep peak memory around
200 MB regardless of ROI count.
"""

import logging
import os
import shutil
import subprocess
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

from imaging_hub.settings import BASE_DIR

_NIFTI_MAX_WORKERS = int(os.getenv("NIFTI_MAX_WORKERS", "1"))
_DCM2NIIX = shutil.which("dcm2niix")

logger = logging.getLogger(__name__)


def _run_dcm2niix(ct_folder, output_dir):
    """Convert a CT DICOM series to compressed NIfTI using dcm2niix.

    Falls back to pydicom + nibabel when dcm2niix cannot handle the data
    (e.g. non-uniform slice spacing or unusual pixel data types).
    """
    if not _DCM2NIIX:
        logger.warning("dcm2niix not found on PATH, falling back to pydicom")
        _convert_ct_with_pydicom(ct_folder, str(Path(output_dir) / "image.nii.gz"))
        return

    result = subprocess.run(
        [
            _DCM2NIIX,
            "-z",
            "y",
            "-f",
            "image",
            "-o",
            output_dir,
            "-b",
            "n",
            "-w",
            "1",
            ct_folder,
        ],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )

    target = Path(output_dir) / "image.nii.gz"

    if result.returncode != 0:
        logger.warning("dcm2niix failed (rc=%d), falling back to pydicom: %s", result.returncode, result.stdout)
        _convert_ct_with_pydicom(ct_folder, str(target))
        return

    candidates = sorted(
        Path(output_dir).glob("image*.nii.gz"),
        key=lambda p: p.stat().st_size,
        reverse=True,
    )
    if not candidates:
        logger.warning("dcm2niix produced no output, falling back to pydicom")
        _convert_ct_with_pydicom(ct_folder, str(target))
        return

    if len(candidates) > 1:
        logger.warning(
            "dcm2niix produced %d files, using largest: %s",
            len(candidates),
            candidates[0].name,
        )

    if candidates[0] != target:
        candidates[0].rename(target)


def _convert_ct_with_pydicom(ct_folder, output_path):
    """Fallback CT-to-NIfTI conversion using pydicom + nibabel.

    Handles edge cases dcm2niix cannot (non-uniform slice spacing, unusual
    pixel formats).  Reads slices one at a time, builds the volume, and writes
    a compressed NIfTI with the correct affine.
    """
    import nibabel as nib  # noqa: PLC0415 — subprocess-only
    import numpy as np  # noqa: PLC0415
    import pydicom  # noqa: PLC0415

    dcm_files = sorted(Path(ct_folder).glob("*.dcm"))
    if not dcm_files:
        dcm_files = sorted(Path(ct_folder).iterdir())

    slices = []
    for f in dcm_files:
        ds = pydicom.dcmread(str(f))
        if hasattr(ds, "ImagePositionPatient"):
            slices.append(ds)
    if not slices:
        raise FileNotFoundError(f"No valid CT DICOM slices found in {ct_folder}")

    slices.sort(key=lambda s: float(s.ImagePositionPatient[2]))

    first = slices[0]
    rows = int(first.Rows)
    cols = int(first.Columns)
    n_slices = len(slices)

    pixel_spacing = [float(v) for v in first.PixelSpacing]
    orientation = [float(v) for v in first.ImageOrientationPatient]
    position = [float(v) for v in first.ImagePositionPatient]

    row_cosine = np.array(orientation[:3])
    col_cosine = np.array(orientation[3:])
    slice_cosine = np.cross(row_cosine, col_cosine)

    if n_slices > 1:
        last_pos = [float(v) for v in slices[-1].ImagePositionPatient]
        slice_spacing = np.linalg.norm(np.array(last_pos) - np.array(position)) / (n_slices - 1)
    else:
        slice_spacing = float(getattr(first, "SliceThickness", 1.0))

    affine_lps = np.eye(4)
    affine_lps[:3, 0] = row_cosine * pixel_spacing[1]
    affine_lps[:3, 1] = col_cosine * pixel_spacing[0]
    affine_lps[:3, 2] = slice_cosine * slice_spacing
    affine_lps[:3, 3] = position

    lps_to_ras = np.diag([-1, -1, 1, 1])
    affine_ras = lps_to_ras @ affine_lps

    volume = np.zeros((cols, rows, n_slices), dtype=np.int16)
    for i, ds in enumerate(slices):
        arr = ds.pixel_array
        slope = float(getattr(ds, "RescaleSlope", 1))
        intercept = float(getattr(ds, "RescaleIntercept", 0))
        volume[:, :, i] = (arr.T * slope + intercept).astype(np.int16)

    nib.save(nib.Nifti1Image(volume, affine_ras), output_path)
    logger.info("Fallback CT conversion: wrote %s (%s)", output_path, volume.shape)


def _fix_missing_contour_data(contour_data):
    """Fix contour coordinate arrays that have missing values (ported from platipy)."""
    import numpy as np  # noqa: PLC0415 — subprocess-only

    contour_data = np.array(contour_data)
    if contour_data.any() == "":
        missing_values = np.where(contour_data == "")[0]
        if missing_values.shape[0] > 1:
            return contour_data
        missing_index = missing_values[0]
        missing_axis = missing_index % 3
        if missing_axis == 0:
            if missing_index > len(contour_data) - 3:
                lower = contour_data[missing_index - 3]
                upper = contour_data[0]
            elif missing_index == 0:
                lower = contour_data[-3]
                upper = contour_data[3]
            else:
                lower = contour_data[missing_index - 3]
                upper = contour_data[missing_index + 3]
            contour_data[missing_index] = 0.5 * (float(lower) + float(upper))
        elif missing_axis == 1:
            if missing_index > len(contour_data) - 2:
                lower = contour_data[missing_index - 3]
                upper = contour_data[1]
            elif missing_index == 0:
                lower = contour_data[-2]
                upper = contour_data[4]
            else:
                lower = contour_data[missing_index - 3]
                upper = contour_data[missing_index + 3]
            contour_data[missing_index] = 0.5 * (float(lower) + float(upper))
        else:
            temp = contour_data[2::3].tolist()
            temp.remove("")
            contour_data[missing_index] = np.min(np.array(temp, dtype=np.double))
    return contour_data


def _rasterize_rtstruct(rtstruct_file, affine, shape, output_dir, prefix="Mask_"):
    """Rasterize RTSTRUCT contours into individual NIfTI mask files, one ROI at a time.

    RTSTRUCT contour points are in DICOM patient coordinates (LPS+).  The NIfTI
    affine from dcm2niix maps voxel (i, j, k) to RAS+ world coordinates, so we
    negate x and y before applying the inverse affine.
    """
    import nibabel as nib  # noqa: PLC0415 — subprocess-only
    import numpy as np  # noqa: PLC0415
    import pydicom  # noqa: PLC0415
    from nibabel.affines import apply_affine  # noqa: PLC0415
    from skimage.draw import polygon  # noqa: PLC0415

    inv_affine = np.linalg.inv(affine)
    lps_to_ras = np.array([-1, -1, 1])

    ds = pydicom.dcmread(rtstruct_file, force=True)
    if not hasattr(ds, "ROIContourSequence") or not hasattr(ds, "StructureSetROISequence"):
        return []

    contour_lookup = {cs.ReferencedROINumber: cs for cs in ds.ROIContourSequence}
    masks_written = []

    for roi_seq in ds.StructureSetROISequence:
        roi_name = "_".join(roi_seq.ROIName.split())
        roi_number = roi_seq.ROINumber

        if roi_number not in contour_lookup:
            continue
        contour_item = contour_lookup[roi_number]
        if not hasattr(contour_item, "ContourSequence"):
            continue
        if len(contour_item.ContourSequence) == 0:
            continue
        if contour_item.ContourSequence[0].ContourGeometricType != "CLOSED_PLANAR":
            continue

        mask = np.zeros(shape[:3], dtype=np.uint8)
        skip_contour = False

        for contour in contour_item.ContourSequence:
            contour_data = _fix_missing_contour_data(contour.ContourData)
            pts_lps = np.array(contour_data, dtype=np.float64).reshape(-1, 3)
            pts_ras = pts_lps * lps_to_ras
            pts_voxel = apply_affine(inv_affine, pts_ras)

            k_indices = np.round(pts_voxel[:, 2]).astype(int)
            k_index = k_indices[0]
            if np.any(k_indices != k_index):
                logger.debug("Axial slice index varies in contour, skipping ROI %s", roi_name)
                skip_contour = True
                break

            if k_index < 0 or k_index >= shape[2]:
                logger.debug(
                    "Slice %d outside bounds for ROI %s, skipping slice",
                    k_index,
                    roi_name,
                )
                continue

            i_coords = np.round(pts_voxel[:, 0]).astype(int)
            j_coords = np.round(pts_voxel[:, 1]).astype(int)

            slice_mask = np.zeros((shape[0], shape[1]), dtype=np.uint8)
            rr, cc = polygon(i_coords, j_coords, shape=slice_mask.shape)
            slice_mask[rr, cc] = 1
            mask[:, :, k_index] ^= slice_mask

        if skip_contour:
            continue

        roi_name_clean = roi_name.replace("/", "").replace("\\", "")
        out_path = Path(output_dir) / f"{prefix}{roi_name_clean}.nii.gz"
        nib.save(nib.Nifti1Image(mask, affine), str(out_path))
        masks_written.append(roi_name_clean)
        logger.debug("Wrote mask: %s", out_path.name)
        del mask

    return masks_written


def _convert_in_process(ct_folder, rtstruct_file, tmp_dir):
    """Convert CT DICOM + RTSTRUCT to NIfTI using dcm2niix and custom rasterizer."""
    import nibabel as nib  # noqa: PLC0415 — subprocess-only

    _run_dcm2niix(ct_folder, tmp_dir)

    img = nib.load(str(Path(tmp_dir) / "image.nii.gz"))
    affine = img.affine.copy()
    shape = img.shape[:3]
    del img

    _rasterize_rtstruct(rtstruct_file, affine, shape, tmp_dir)


def _make_db(db_params):
    """Create a fresh database connection for use inside a subprocess."""
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
        self._executor = ProcessPoolExecutor(max_workers=_NIFTI_MAX_WORKERS, max_tasks_per_child=1)

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
