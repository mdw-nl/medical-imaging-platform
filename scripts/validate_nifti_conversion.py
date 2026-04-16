"""Compare new dcm2niix-based NIfTI conversion against existing platipy reference output.

Run inside the imaging-hub Docker container with the data volume mounted:

    docker run --rm -v ./deploy/data/associationdata:/data \
        -v ./scripts:/scripts \
        --entrypoint python imaging-hub:test /scripts/validate_nifti_conversion.py

Both implementations may store arrays in different orientations (platipy uses
LPS voxel ordering, dcm2niix uses a different canonical form).  We reorient both
to RAS+ canonical orientation before comparing so the arrays are element-wise
comparable.
"""

import sys
import tempfile
import traceback
from pathlib import Path

import nibabel as nib
import numpy as np

sys.path.insert(0, "/app")

from imaging_hub.nifti_converter import _convert_in_process


def to_canonical(img):
    """Reorient a NIfTI image to RAS+ canonical orientation."""
    return nib.as_closest_canonical(img)


def compare_patient(patient_dir: Path):
    """Run new conversion on one patient and compare against reference NIFTI."""
    study_dir = next(patient_dir.iterdir())
    ct_folder = str(study_dir / "CT")
    rtstruct_dir = study_dir / "RTSTRUCT"
    rtstruct_file = str(next(rtstruct_dir.glob("*.dcm")))
    ref_nifti_dir = next((study_dir / "NIFTI").iterdir())

    ref_masks = {p.name for p in ref_nifti_dir.iterdir() if p.name.startswith("Mask_")}

    print(f"\n{'=' * 70}")
    print(f"Patient: {patient_dir.name}")
    print(f"  CT slices: {len(list((study_dir / 'CT').glob('*.dcm')))}")
    print(f"  Reference masks: {len(ref_masks)}")

    with tempfile.TemporaryDirectory() as tmp_dir:
        _convert_in_process(ct_folder, rtstruct_file, tmp_dir)

        new_image = Path(tmp_dir) / "image.nii.gz"
        if not new_image.exists():
            print("  ERROR: dcm2niix produced no image.nii.gz")
            return False

        new_masks = {p.name for p in Path(tmp_dir).iterdir() if p.name.startswith("Mask_")}

        ref_img = to_canonical(nib.load(str(ref_nifti_dir / "image.nii.gz")))
        new_img = to_canonical(nib.load(str(new_image)))

        print("\n  --- Image comparison (both reoriented to RAS+ canonical) ---")
        print(f"  Reference shape: {ref_img.shape}, New shape: {new_img.shape}")
        affine_close = np.allclose(ref_img.affine, new_img.affine, atol=1e-2)
        print(f"  Affine match (atol=0.01): {affine_close}")
        if not affine_close:
            print(f"  Ref affine:\n{ref_img.affine}")
            print(f"  New affine:\n{new_img.affine}")

        ref_data = ref_img.get_fdata()
        new_data = new_img.get_fdata()
        if ref_data.shape == new_data.shape:
            hu_close = np.allclose(ref_data, new_data, atol=1.0)
            hu_max_diff = np.max(np.abs(ref_data - new_data))
            print(f"  Pixel data match (atol=1.0 HU): {hu_close}, max diff: {hu_max_diff:.2f}")
        else:
            print("  WARNING: Shape mismatch after reorient, cannot compare pixel data")

        print("\n  --- Mask comparison ---")
        only_ref = ref_masks - new_masks
        only_new = new_masks - ref_masks
        common = ref_masks & new_masks
        print(f"  Common masks: {len(common)}")
        if only_ref:
            print(f"  Only in reference: {sorted(only_ref)}")
        if only_new:
            print(f"  Only in new: {sorted(only_new)}")

        all_pass = True
        for mask_name in sorted(common):
            ref_mask = to_canonical(nib.load(str(ref_nifti_dir / mask_name))).get_fdata()
            new_mask = to_canonical(nib.load(str(Path(tmp_dir) / mask_name))).get_fdata()

            if ref_mask.shape != new_mask.shape:
                print(f"  {mask_name}: SHAPE MISMATCH {ref_mask.shape} vs {new_mask.shape}")
                all_pass = False
                continue

            ref_bin = ref_mask > 0
            new_bin = new_mask > 0

            exact = np.array_equal(ref_bin, new_bin)
            ref_sum = ref_bin.sum()
            new_sum = new_bin.sum()

            if ref_sum + new_sum == 0:
                dice = 1.0
            else:
                intersection = (ref_bin & new_bin).sum()
                dice = 2 * intersection / (ref_sum + new_sum)

            disagreeing = np.sum(ref_bin != new_bin)

            if exact:
                status = "EXACT"
            elif dice > 0.99:
                status = f"CLOSE (dice={dice:.6f}, diff_voxels={disagreeing})"
            elif dice > 0.95:
                status = f"WARN  (dice={dice:.4f}, diff_voxels={disagreeing})"
                all_pass = False
            else:
                status = f"FAIL  (dice={dice:.4f}, diff_voxels={disagreeing})"
                all_pass = False

            print(f"  {mask_name}: {status}")

        if only_ref or only_new:
            all_pass = False

        return all_pass


def main():
    data_root = Path("/data")
    if not data_root.exists():
        print("ERROR: /data not mounted. Run inside Docker with -v ./deploy/data/associationdata:/data")
        sys.exit(1)

    patients = sorted(data_root.iterdir())
    patients_with_nifti = [
        p for p in patients if p.is_dir() and any((p / s / "NIFTI").exists() for s in p.iterdir() if s.is_dir())
    ]

    sample = patients_with_nifti[:5]
    print(f"Validating {len(sample)} patients out of {len(patients_with_nifti)} with NIFTI reference data")

    results = {}
    for patient in sample:
        try:
            results[patient.name] = compare_patient(patient)
        except Exception as exc:
            print(f"\n  EXCEPTION for {patient.name}: {exc}")
            traceback.print_exc()
            results[patient.name] = False

    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")
    for name, passed in results.items():
        print(f"  {name}: {'PASS' if passed else 'FAIL'}")

    all_passed = all(results.values())
    print(f"\nOverall: {'ALL PASSED' if all_passed else 'SOME FAILED'}")
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
