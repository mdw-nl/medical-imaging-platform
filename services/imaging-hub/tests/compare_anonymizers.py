"""Run both anonymizers on the same input, then compare their outputs.

The OLD anonymizer is the deid-library-based version (embedded below).
The NEW anonymizer is `imaging_hub.anonymization.anonymizer.Anonymizer`.

Usage:
    uv run python compare_anonymizers.py \\
        --input     C:\\path\\to\\original_dicom \\
        --out-dir   C:\\path\\to\\workdir \\
        --report    comparison_report.csv

"""

import argparse
import contextlib
import csv
import gc
import hashlib
import logging
import os
import re
import sys
import tempfile
from collections import Counter, defaultdict
from collections.abc import Generator
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml
from pydicom import dcmread
from pydicom.datadict import add_private_dict_entries

logger = logging.getLogger("compare_anonymizers")


def _repo_root() -> Path:
    here = Path(__file__).resolve().parent
    for candidate in (here, *here.parents):
        if (candidate / "pyproject.toml").is_file() and (candidate / "services").is_dir():
            return candidate
    raise RuntimeError(
        f"Could not locate repo root (pyproject.toml + services/) from {here}"
    )

# Shared configuration — both anonymizers receive these identical values.
SETTINGS = {
    "patient_name": "ANON",
    "profile_name": "test-profile",
    "project_name": "test-project",
    "trial_name": "test-trial",
    "site_name": "test-site",
    "site_id": "SITE01",
    "uid_secret": "change-me-to-a-stable-project-secret",
    "uid_prefix": "1.2.826.0.1.3680043.10.1338.",
}

# Old anonymizer expects CamelCase keys; map flat -> CamelCase.
OLD_VAR_KEYS = {
    "PatientName": "patient_name",
    "ProfileName": "profile_name",
    "ProjectName": "project_name",
    "TrialName": "trial_name",
    "SiteName": "site_name",
    "SiteID": "site_id",
}


# ============================================================================
# OLD anonymizer (embedded, deid-library-based)
# The ONLY change vs. the original: __init__ accepts a `variables` dict so
# we can feed it the same config as the new anonymizer without needing a
# `variables.yaml` file on disk.
# ============================================================================

@contextlib.contextmanager
def _suppress_output() -> Generator[None, None, None]:
    devnull = Path(os.devnull)
    with (
        devnull.open("w") as out,
        devnull.open("w") as err,
        contextlib.redirect_stdout(out),
        contextlib.redirect_stderr(err),
    ):
        yield


class OldAnonymizer:
    """Legacy deid-based anonymizer."""

    def __init__(self, path_files, variables=None):
        from deid.config import DeidRecipe  # noqa: PLC0415

        path_base = Path(path_files)
        if variables is None:
            path_var = path_base / "variables.yaml"
            with path_var.open() as f:
                config_data = yaml.safe_load(f)
            variables = config_data.get("variables", {})

        self.PatientName = variables.get("PatientName")
        self.ProfileName = variables.get("ProfileName")
        self.ProjectName = variables.get("ProjectName")
        self.TrialName = variables.get("TrialName")
        self.SiteName = variables.get("SiteName")
        self.SiteID = variables.get("SiteID")

        self.recipe_path = str(path_base / "old_recipe.dicom")
        self.patient_lookup_csv = str(path_base / "patient_lookup.csv")
        df = pd.read_csv(self.patient_lookup_csv, dtype=str)
        self._patient_map = dict(zip(df["original"], df["new"], strict=False))

        self.ROI_normalization_path = str(path_base / "ROI_normalization.yaml")
        with Path(self.ROI_normalization_path).open() as f:
            roi_map = yaml.safe_load(f) or {}
        self._compiled_roi_map = {
            canonical: [re.compile(p, re.IGNORECASE) for p in patterns]
            for canonical, patterns in roi_map.items()
        }

        self._recipe = DeidRecipe(deid=self.recipe_path)

        private_entries = {
            0x10011001: ("SH", "1", "ProfileName"),
            0x10031001: ("SH", "1", "ProjectName"),
            0x10051001: ("SH", "1", "TrialName"),
            0x10071001: ("SH", "1", "SiteName"),
            0x10091001: ("SH", "1", "SiteID"),
        }
        add_private_dict_entries("Deid", private_entries)

    @staticmethod
    def hash_func(item, value, field, dicom):  # noqa: ARG004
        return hashlib.md5(value.encode()).hexdigest()[:16]  # noqa: S324

    @staticmethod
    def current_date(field, value, item, dicom):  # noqa: ARG004
        return f"deid: {datetime.now().strftime('%d%m%Y:%H%M%S')}"

    def csv_lookup_func(self, item, value, field, dicom):  # noqa: ARG002
        patient_id = getattr(dicom, "PatientID", None)
        if patient_id is None:
            raise ValueError("PatientID missing")
        try:
            return self._patient_map[patient_id]
        except KeyError as e:
            raise ValueError(f"PatientID '{patient_id}' not found in lookup CSV") from e

    def ROI_normalization(self, rtstruct):
        for roi in rtstruct.StructureSetROISequence:
            original_raw = roi.ROIName
            normalized = None
            for canonical, regex_list in self._compiled_roi_map.items():
                if any(regex.search(original_raw.strip()) for regex in regex_list):
                    normalized = canonical
                    break
            if normalized and original_raw != normalized:
                roi.ROIName = normalized
            elif normalized is None:
                logger.warning("No ROI map for '%s' (old anonymizer)", original_raw)
        return rtstruct

    def anonymize(self, dicom_obj):
        from deid.dicom import get_identifiers, replace_identifiers  # noqa: PLC0415

        with _suppress_output(), tempfile.TemporaryDirectory() as tmpdir:
            temp_path = str(Path(tmpdir) / "temp.dcm")
            dicom_obj.save_as(temp_path, write_like_original=False)
            del dicom_obj

            items = get_identifiers([temp_path], expand_sequences=False)
            for key in items:
                items[key].update({
                    "CSV_lookup_func": self.csv_lookup_func,
                    "hash_func": self.hash_func,
                    "DeIdentificationMethod": self.current_date,
                    "PatientName": self.PatientName,
                })
            updated = replace_identifiers(dicom_files=[temp_path], deid=self._recipe, ids=items)
            dicom_obj = updated[0]
            del items, updated

        dicom_obj.remove_private_tags()
        dicom_obj.private_block(0x1001, "Deid", create=True).add_new(0x01, "SH", self.ProfileName)
        dicom_obj.private_block(0x1003, "Deid", create=True).add_new(0x01, "SH", self.ProjectName)
        dicom_obj.private_block(0x1005, "Deid", create=True).add_new(0x01, "SH", self.TrialName)
        dicom_obj.private_block(0x1007, "Deid", create=True).add_new(0x01, "SH", self.SiteName)
        dicom_obj.private_block(0x1009, "Deid", create=True).add_new(0x01, "SH", self.SiteID)
        gc.collect()
        return dicom_obj

    def run(self, dicomdata):
        try:
            if getattr(dicomdata, "Modality", None) == "RTSTRUCT":
                self.ROI_normalization(dicomdata)
            return self.anonymize(dicomdata)
        except Exception:
            logger.exception("Old anonymizer failed")
            return None


# Run both anonymizers

def find_dicom_files(root: Path):
    if root.is_file():
        yield Path(root.name), root
        return
    for p in root.rglob("*"):
        if p.is_file() and (p.suffix.lower() in {".dcm", ""}):
            yield p.relative_to(root), p


def make_new_anonymizer(recipes_dir: Path):
    import importlib.util  
    from imaging_common import AnonymizationSettings 

    anon_path = (
        _repo_root()
        / "services" / "imaging-hub" / "src" / "imaging_hub"
        / "anonymization" / "anonymizer.py"
    )
    if not anon_path.is_file():
        raise FileNotFoundError(
            f"Could not locate the new anonymizer at {anon_path}. "
            "Is this script sitting at the repo root?"
        )
    spec = importlib.util.spec_from_file_location("standalone_anonymizer", anon_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    NewAnonymizer = mod.Anonymizer

    return NewAnonymizer(AnonymizationSettings(**SETTINGS), recipes_dir)


def make_old_anonymizer(recipes_dir: Path):
    variables = {old_key: SETTINGS[flat_key] for old_key, flat_key in OLD_VAR_KEYS.items()}
    return OldAnonymizer(recipes_dir, variables=variables)


def run_both(input_root: Path, old_out: Path, new_out: Path, recipes_dir: Path, limit: int = 0):
    logger.info("Initializing NEW anonymizer (recipes: %s)", recipes_dir)
    new_anon = make_new_anonymizer(recipes_dir)
    logger.info("Initializing OLD anonymizer (recipes: %s)", recipes_dir)
    old_anon = make_old_anonymizer(recipes_dir)

    files = list(find_dicom_files(input_root))
    if limit:
        files = files[:limit]
    logger.info("Running both anonymizers on %d file(s)", len(files))

    n_new = n_old = n_fail_new = n_fail_old = 0
    for i, (rel, src) in enumerate(files, 1):
        if i % 25 == 0 or i == len(files):
            logger.info("Progress: %d / %d", i, len(files))

        try:
            ds_new = new_anon.run_dataset(str(src))
        except Exception:
            logger.exception("NEW crashed on %s", rel)
            ds_new = None

        if ds_new is not None:
            dst = new_out / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            ds_new.save_as(str(dst))
            n_new += 1
        else:
            n_fail_new += 1

        try:
            ds_in = dcmread(str(src))
            ds_old = old_anon.run(ds_in)
        except Exception:
            logger.exception("OLD crashed on %s", rel)
            ds_old = None

        if ds_old is not None:
            dst = old_out / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            ds_old.save_as(str(dst))
            n_old += 1
        else:
            n_fail_old += 1

    logger.info("NEW: %d ok, %d failed | OLD: %d ok, %d failed",
                n_new, n_fail_new, n_old, n_fail_old)


# Comparison logic

AGREE_KEPT = "AGREE_KEPT"
AGREE_REMOVED = "AGREE_REMOVED"
AGREE_BLANKED = "AGREE_BLANKED"
AGREE_REPLACED = "AGREE_REPLACED"
AGREE_HASHED_UID = "AGREE_HASHED_UID"
AGREE_HASHED_VAL = "AGREE_HASHED_VAL"
ONLY_IN_OUTPUT = "ONLY_IN_OUTPUT"
DISAGREE_PRESENCE = "DISAGREE_PRESENCE"
DISAGREE_VALUE = "DISAGREE_VALUE"

CATEGORY_ORDER = [
    AGREE_KEPT, AGREE_REMOVED, AGREE_BLANKED, AGREE_REPLACED,
    AGREE_HASHED_UID, AGREE_HASHED_VAL, ONLY_IN_OUTPUT,
    DISAGREE_PRESENCE, DISAGREE_VALUE,
]


def safe_read(path: Path):
    try:
        return dcmread(str(path))
    except Exception:
        logger.exception("Failed to read %s", path)
        return None


def values_equal(a, b) -> bool:
    try:
        return a == b
    except Exception:
        return repr(a) == repr(b)


def truncate(val, n=120) -> str:
    s = repr(val)
    return s if len(s) <= n else s[: n - 3] + "..."


def categorize(in_elem, old_elem, new_elem):
    in_in = in_elem is not None
    in_old = old_elem is not None
    in_new = new_elem is not None

    if in_old != in_new:
        return DISAGREE_PRESENCE
    if not in_old:
        return AGREE_REMOVED if in_in else None

    v_old, v_new = old_elem.value, new_elem.value
    vr = old_elem.VR or new_elem.VR
    same = values_equal(v_old, v_new)

    if not in_in:
        return ONLY_IN_OUTPUT

    v_in = in_elem.value
    if same:
        if values_equal(v_old, v_in):
            return AGREE_KEPT
        if v_old in (None, "", b""):
            return AGREE_BLANKED
        return AGREE_REPLACED

    if vr == "UI":
        return AGREE_HASHED_UID
    if not values_equal(v_old, v_in) and not values_equal(v_new, v_in):
        return AGREE_HASHED_VAL
    return DISAGREE_VALUE


@dataclass
class UidTracker:
    old: dict = field(default_factory=lambda: defaultdict(set))
    new: dict = field(default_factory=lambda: defaultdict(set))


def compare_pair(rel_path: Path, in_path: Path | None, old_path: Path, new_path: Path,
                 uid_tracker: UidTracker):
    ds_in = safe_read(in_path) if in_path else None
    ds_old = safe_read(old_path)
    ds_new = safe_read(new_path)
    if ds_old is None or ds_new is None:
        return None, []

    counts: Counter = Counter()
    disagreements = []

    tags = set()
    for ds in (ds_in, ds_old, ds_new):
        if ds is None:
            continue
        for elem in ds:
            tags.add(elem.tag)

    for tag in sorted(tags):
        e_in = ds_in[tag] if (ds_in is not None and tag in ds_in) else None
        e_old = ds_old[tag] if tag in ds_old else None
        e_new = ds_new[tag] if tag in ds_new else None

        cat = categorize(e_in, e_old, e_new)
        if cat is None:
            continue
        counts[cat] += 1

        if e_in is not None and e_old is not None and e_new is not None:
            vr = e_old.VR or e_new.VR
            if vr == "UI" and isinstance(e_in.value, str) and e_in.value:
                key = (str(tag), e_in.value)
                uid_tracker.old[key].add(str(e_old.value))
                uid_tracker.new[key].add(str(e_new.value))

        if cat in (DISAGREE_PRESENCE, DISAGREE_VALUE):
            disagreements.append({
                "file": str(rel_path),
                "tag": str(tag),
                "keyword": (e_old.keyword if e_old else (e_new.keyword if e_new else "")) or "",
                "VR": (e_old.VR if e_old else (e_new.VR if e_new else "")) or "",
                "category": cat,
                "v_input": truncate(e_in.value) if e_in is not None else "<absent>",
                "v_old": truncate(e_old.value) if e_old is not None else "<absent>",
                "v_new": truncate(e_new.value) if e_new is not None else "<absent>",
            })

    return counts, disagreements


def compare_outputs(input_root: Path | None, old_out: Path, new_out: Path,
                    report_path: Path, limit: int = 0) -> int:
    pairs = []
    for rel, new_path in find_dicom_files(new_out):
        old_path = old_out / rel
        if not old_path.exists():
            logger.debug("No OLD output for %s — skipping", rel)
            continue
        in_path = (input_root / rel) if input_root else None
        if in_path and not in_path.exists():
            in_path = None
        pairs.append((rel, in_path, old_path, new_path))

    if limit:
        pairs = pairs[:limit]

    if not pairs:
        logger.error("No comparable file pairs found.")
        return 2

    logger.info("Comparing %d file pair(s)", len(pairs))

    total: Counter = Counter()
    all_disagreements = []
    skipped = 0
    uid_tracker = UidTracker()

    for rel, in_path, old_path, new_path in pairs:
        counts, disagreements = compare_pair(rel, in_path, old_path, new_path, uid_tracker)
        if counts is None:
            skipped += 1
            continue
        total.update(counts)
        all_disagreements.extend(disagreements)

    print()
    print("=" * 60)
    print("Anonymization Comparison Summary")
    print("=" * 60)
    print(f"File pairs compared : {len(pairs) - skipped}")
    print(f"File pairs skipped  : {skipped}")
    print(f"Original used       : {'yes' if input_root else 'no (2-way comparison)'}")
    print()
    print(f"{'Category':25s} {'Count':>10s}")
    print("-" * 40)
    for cat in CATEGORY_ORDER:
        n = total.get(cat, 0)
        marker = "  <-- review" if (cat.startswith("DISAGREE") and n) else ""
        print(f"{cat:25s} {n:>10d}{marker}")
    print("-" * 40)
    print(f"{'TOTAL':25s} {sum(total.values()):>10d}")
    print()

    if input_root:
        old_viol = {k: v for k, v in uid_tracker.old.items() if len(v) > 1}
        new_viol = {k: v for k, v in uid_tracker.new.items() if len(v) > 1}
        print("UID consistency (same input UID -> single output UID across all files):")
        print(f"  OLD: {'OK' if not old_viol else f'{len(old_viol)} violation(s)'}")
        print(f"  NEW: {'OK' if not new_viol else f'{len(new_viol)} violation(s)'}")
        print()

    if all_disagreements:
        with report_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["file", "tag", "keyword", "VR", "category",
                               "v_input", "v_old", "v_new"])
            writer.writeheader()
            writer.writerows(all_disagreements)
        print(f"{len(all_disagreements)} disagreement row(s) written to {report_path}")

        # Deduplicated companion report: one row per distinct (tag, keyword, VR,
        # category, direction). Direction categorizes which side has the tag for
        # presence mismatches, or "BOTH_DIFFER" for value mismatches.
        def direction(row):
            if row["category"] == DISAGREE_PRESENCE:
                if row["v_old"] != "<absent>" and row["v_new"] == "<absent>":
                    return "OLD_HAS_NEW_MISSING"
                if row["v_new"] != "<absent>" and row["v_old"] == "<absent>":
                    return "NEW_HAS_OLD_MISSING"
                return "UNKNOWN"
            return "BOTH_DIFFER"

        bucket_keys: list[tuple] = []
        bucket_examples: dict = {}
        bucket_files: dict = defaultdict(set)
        for row in all_disagreements:
            key = (row["tag"], row["keyword"], row["VR"],
                   row["category"], direction(row))
            if key not in bucket_examples:
                bucket_keys.append(key)
                bucket_examples[key] = row
            bucket_files[key].add(row["file"])

        distinct_path = report_path.with_name(
            report_path.stem + "_distinct" + report_path.suffix)
        with distinct_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["tag", "keyword", "VR", "category", "direction",
                               "n_files", "example_file", "example_v_input",
                               "example_v_old", "example_v_new"])
            writer.writeheader()
            # Sort: most frequent first
            for key in sorted(bucket_keys, key=lambda k: -len(bucket_files[k])):
                ex = bucket_examples[key]
                writer.writerow({
                    "tag": key[0],
                    "keyword": key[1],
                    "VR": key[2],
                    "category": key[3],
                    "direction": key[4],
                    "n_files": len(bucket_files[key]),
                    "example_file": ex["file"],
                    "example_v_input": ex["v_input"],
                    "example_v_old": ex["v_old"],
                    "example_v_new": ex["v_new"],
                })
        print(f"{len(bucket_keys)} distinct mismatch type(s) written to {distinct_path}")
        return 1

    print("No disagreements found.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", type=Path, required=True, help="Original DICOM directory.")
    parser.add_argument("--out-dir", type=Path, required=True,
                        help="Working directory; subdirs 'old/' and 'new/' will be created here.")
    parser.add_argument("--recipes-dir", type=Path,
                        default=_repo_root() / "services" / "imaging-hub" / "recipes",
                        help="Directory with recipe.dicom, patient_lookup.csv, ROI_normalization.yaml.")
    parser.add_argument("--report", type=Path, default=Path("comparison_report.csv"))
    parser.add_argument("--skip-anonymize", action="store_true",
                        help="Skip running the anonymizers; just re-compare existing outputs.")
    parser.add_argument("--limit", type=int, default=0, help="Process only the first N files.")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s")

    if not args.recipes_dir.is_dir():
        logger.error("Recipes dir not found: %s", args.recipes_dir)
        return 2

    old_out = args.out_dir / "old"
    new_out = args.out_dir / "new"

    if not args.skip_anonymize:
        old_out.mkdir(parents=True, exist_ok=True)
        new_out.mkdir(parents=True, exist_ok=True)
        run_both(args.input, old_out, new_out, args.recipes_dir, limit=args.limit)
    else:
        logger.info("Skipping anonymization step; comparing existing outputs.")

    return compare_outputs(args.input, old_out, new_out, args.report, limit=args.limit)


if __name__ == "__main__":
    sys.exit(main())