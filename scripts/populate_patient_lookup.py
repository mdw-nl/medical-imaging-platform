#!/usr/bin/env python3
"""Scan a directory for DICOM files and interactively add missing PatientIDs to the lookup CSV."""

import argparse
import csv
import os
from pathlib import Path

from pydicom import dcmread

DEFAULT_CSV = Path(__file__).resolve().parent.parent / "deploy" / "recipes" / "patient_lookup.csv"


def collect_patient_ids(folder: Path) -> dict[str, str]:
    """Return a dict of {patient_id: first_file_path} for all DICOM files under *folder*."""
    patient_ids: dict[str, str] = {}
    file_count = 0
    for root, _, files in os.walk(folder):
        for fname in files:
            if not fname.lower().endswith(".dcm"):
                continue
            fpath = Path(root) / fname
            try:
                ds = dcmread(fpath, stop_before_pixels=True, force=True)
                pid = getattr(ds, "PatientID", None)
                if pid and pid not in patient_ids:
                    patient_ids[pid] = str(fpath)
            except Exception as exc:
                print(f"  [WARN] Could not read {fpath}: {exc}")
            file_count += 1
            if file_count % 500 == 0:
                print(f"  Scanned {file_count} files, {len(patient_ids)} unique PatientIDs so far...")
    print(f"  Scanned {file_count} files total, {len(patient_ids)} unique PatientIDs found.\n")
    return patient_ids


def load_existing_csv(csv_path: Path) -> dict[str, str]:
    """Load existing original->new mappings from the CSV."""
    if not csv_path.exists():
        return {}
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        return {row["original"]: row["new"] for row in reader}


def generate_anon_id() -> str:
    """Generate a random anonymized patient ID."""
    import uuid  # noqa: PLC0415 — inline to prevent ruff from moving to TYPE_CHECKING

    return uuid.uuid4().hex[:12]


def append_to_csv(csv_path: Path, entries: list[tuple[str, str]]) -> None:
    """Append new original,new pairs to the CSV."""
    with csv_path.open("a", newline="") as f:
        writer = csv.writer(f)
        for original, new in entries:
            writer.writerow([original, new])


def main():
    parser = argparse.ArgumentParser(
        description="Scan DICOM files and populate patient_lookup.csv with missing PatientIDs",
    )
    parser.add_argument("folder", help="Root directory to scan for .dcm files")
    parser.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_CSV,
        help=f"Path to patient_lookup.csv (default: {DEFAULT_CSV})",
    )
    parser.add_argument(
        "--keep-same",
        action="store_true",
        help="Map each PatientID to itself instead of generating a random value",
    )
    args = parser.parse_args()

    folder = Path(args.folder)
    if not folder.is_dir():
        print(f"[ERROR] Not a directory: {folder}")
        raise SystemExit(1)

    print(f"Scanning {folder} for DICOM files...")
    found_ids = collect_patient_ids(folder)
    if not found_ids:
        print("No DICOM files with PatientID found.")
        return

    existing = load_existing_csv(args.csv)
    new_ids = {pid: path for pid, path in found_ids.items() if pid not in existing}

    if not new_ids:
        print("All PatientIDs already present in the lookup CSV. Nothing to add.")
        return

    print(f"{len(new_ids)} new PatientID(s) to add (out of {len(found_ids)} found):\n")

    entries: list[tuple[str, str]] = []
    for pid in sorted(new_ids):
        new_value = pid if args.keep_same else generate_anon_id()
        print(f"  {pid}  ->  {new_value}")
        entries.append((pid, new_value))

    append_to_csv(args.csv, entries)
    print(f"\nAdded {len(entries)} entries to {args.csv}")


if __name__ == "__main__":
    main()
