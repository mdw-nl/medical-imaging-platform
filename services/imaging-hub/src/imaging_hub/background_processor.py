"""Background processor that anonymizes staged DICOM files in a multiprocessing pool."""

import contextlib
import logging
import multiprocessing
import os
import queue
import threading
import uuid as _uuid
from collections import deque
from dataclasses import dataclass
from multiprocessing.pool import AsyncResult

from pydicom import Dataset

from imaging_hub.anonymization import Anonymizer
from imaging_hub.dicom_data import create_folder, return_dicom_data
from imaging_hub.queries import INSERT_QUERY_DICOM_META
from imaging_hub.staging import StagedFile, StagingManager

logger = logging.getLogger(__name__)


def get_or_create_generated_patient_id(db, original_patient_id: str) -> str:
    """Return the generated patient ID for *original_patient_id*, creating one if absent."""
    row = db.fetch_one(
        "SELECT generated_patient_id FROM patient_id_map WHERE original_patient_id = %s",
        (original_patient_id,),
    )
    if row:
        return row[0]
    generated = f"PAT-{_uuid.uuid4().hex[:12].upper()}"
    db.execute_query(
        """
        INSERT INTO patient_id_map (original_patient_id, generated_patient_id)
        VALUES (%s, %s)
        ON CONFLICT (original_patient_id) DO NOTHING
        """,
        (original_patient_id, generated),
    )
    row = db.fetch_one(
        "SELECT generated_patient_id FROM patient_id_map WHERE original_patient_id = %s",
        (original_patient_id,),
    )
    return row[0]


QUEUE_MAX_SIZE = int(os.getenv("QUEUE_MAX_SIZE", "0"))
_POOL_MAX_WORKERS = int(os.getenv("POOL_MAX_WORKERS", "4"))

_mp_context = multiprocessing.get_context("fork")

_worker_anonymizer: Anonymizer | None = None


def _init_worker(anonymization_settings, path_recipes: str) -> None:
    global _worker_anonymizer
    _worker_anonymizer = Anonymizer(settings=anonymization_settings, recipes_dir=path_recipes)


def _anonymize_file_in_worker(file_path: str, patient_map_entry: dict) -> Dataset | None:
    _worker_anonymizer._patient_map.update(patient_map_entry)
    return _worker_anonymizer.run_dataset(file_path)


@dataclass
class WorkItem:
    """Unit of work queued for background anonymization."""

    staged: StagedFile
    assoc_id: str
    original_patient_id: str | None = None
    generated_patient_id: str | None = None
    sop_uid: str = "UNKNOWN"
    project: str | None = None


@dataclass
class _InFlightItem:
    future: AsyncResult
    assoc_id: str
    original_patient_id: str | None
    sop_uid: str
    staged: StagedFile
    worker_path: str
    project: str | None = None


class BackgroundProcessor:
    """Consume staged DICOM files from a queue, anonymize them in worker processes, and persist results."""

    def __init__(self, anonymizer, db, tracker, path_recipes, staging: StagingManager, anonymization_settings):
        self._queue: queue.Queue[WorkItem] = queue.Queue(maxsize=QUEUE_MAX_SIZE)
        self._stop = threading.Event()
        self._anonymizer = anonymizer
        self._db = db
        self._tracker = tracker
        self._path_recipes = path_recipes
        self._staging = staging
        self._anonymization_settings = anonymization_settings
        self._pool = self._make_pool()
        self._thread = threading.Thread(target=self._worker_loop, daemon=True, name="bg-processor")
        self._thread.start()
        logger.info(
            "BackgroundProcessor started (workers=%d, queue=%d)",
            _POOL_MAX_WORKERS,
            QUEUE_MAX_SIZE,
        )

    def _make_pool(self):
        return _mp_context.Pool(
            processes=_POOL_MAX_WORKERS,
            initializer=_init_worker,
            initargs=(self._anonymization_settings, self._path_recipes),
            maxtasksperchild=50,
        )

    def enqueue(
        self,
        staged: StagedFile,
        assoc_id: str,
        original_patient_id: str | None,
        generated_patient_id: str | None,
        sop_uid: str,
        project: str | None = None,
    ):
        """Add a staged file to the processing queue (blocks up to 5 s if full)."""
        item = WorkItem(
            staged=staged,
            assoc_id=assoc_id,
            original_patient_id=original_patient_id,
            generated_patient_id=generated_patient_id,
            sop_uid=sop_uid,
            project=project,
        )
        self._queue.put(item, timeout=5)
        logger.debug("Enqueued work item for assoc %s (SOP %s), queue size ~%s", assoc_id, sop_uid, self._queue.qsize())

    def _submit_item(self, item: WorkItem, in_flight: deque):
        patient_id = item.original_patient_id
        if patient_id is None or item.generated_patient_id is None:
            logger.error("No generated patient ID for '%s', rejecting SOP %s", patient_id, item.sop_uid)
            self._tracker.record_error(item.assoc_id, patient_id)
            self._staging.cleanup(item.staged)
            return
        patient_map_entry = {patient_id: item.generated_patient_id}
        worker_path = self._staging.read_to_tempfile(item.staged)
        future = self._pool.apply_async(_anonymize_file_in_worker, (worker_path, patient_map_entry))
        in_flight.append(
            _InFlightItem(
                future=future,
                assoc_id=item.assoc_id,
                original_patient_id=patient_id,
                sop_uid=item.sop_uid,
                staged=item.staged,
                worker_path=worker_path,
                project=item.project,
            )
        )

    def _collect_one(self, inf: _InFlightItem):
        try:
            anonymised_ds = inf.future.get()
            if anonymised_ds is None:
                logger.error("Anonymization failed for SOP %s", inf.sop_uid)
                raise RuntimeError("Anonymization returned None")

            meta = return_dicom_data(anonymised_ds)

            filename = create_folder(meta.patient_id, meta.study_uid, meta.modality, meta.sop_uid)
            anonymised_ds.save_as(filename, write_like_original=False)
            logger.info("Stored %s file for patient %s: %s", meta.modality, meta.patient_id, filename)

            del anonymised_ds

            params = (
                meta.patient_name,
                meta.patient_id,
                meta.study_uid,
                meta.series_uid,
                meta.modality,
                meta.sop_uid,
                meta.sop_class_uid,
                meta.instance_number,
                filename,
                meta.referenced_rt_plan_uid,
                meta.referenced_sop_class_uid,
                meta.referenced_rtstruct_sop_uid,
                meta.referenced_ct_series_uid,
                meta.modality_type,
                inf.assoc_id,
                inf.project,
            )
            self._db.execute_query(INSERT_QUERY_DICOM_META, params)
            self._tracker.record_processed(inf.assoc_id, inf.original_patient_id)
        except Exception:
            logger.exception("Worker failed processing item for assoc %s", inf.assoc_id)
            self._tracker.record_error(inf.assoc_id, inf.original_patient_id)
        finally:
            self._staging.cleanup(inf.staged, inf.worker_path)
            del inf

    def _worker_loop(self):
        in_flight: deque[_InFlightItem] = deque()

        while True:
            if not self._stop.is_set():
                while len(in_flight) < _POOL_MAX_WORKERS:
                    try:
                        item = self._queue.get_nowait()
                        self._submit_item(item, in_flight)
                    except queue.Empty:
                        break

            if in_flight:
                collected = False
                remaining: deque[_InFlightItem] = deque()
                while in_flight:
                    inf = in_flight.popleft()
                    if inf.future.ready():
                        self._collect_one(inf)
                        collected = True
                    else:
                        remaining.append(inf)
                in_flight = remaining

                if not collected and in_flight:
                    inf = in_flight.popleft()
                    with contextlib.suppress(multiprocessing.TimeoutError):
                        inf.future.wait(timeout=0.1)
                    if inf.future.ready():
                        self._collect_one(inf)
                    else:
                        in_flight.appendleft(inf)

            elif self._stop.is_set():
                break
            else:
                try:
                    item = self._queue.get(timeout=0.5)
                    self._submit_item(item, in_flight)
                except queue.Empty:
                    continue

        self._drain(in_flight)

    def _drain(self, in_flight: deque):
        while in_flight:
            inf = in_flight.popleft()
            inf.future.wait()
            self._collect_one(inf)
        while True:
            try:
                item = self._queue.get_nowait()
                self._submit_item(item, in_flight)
            except queue.Empty:
                break
            if in_flight:
                inf = in_flight.popleft()
                inf.future.wait()
                self._collect_one(inf)

    def shutdown(self):
        """Stop the worker loop, drain remaining items, and terminate the process pool."""
        logger.info("BackgroundProcessor shutting down...")
        self._stop.set()
        self._thread.join(timeout=300)
        if self._thread.is_alive():
            logger.warning("BackgroundProcessor thread did not exit in time")
        self._pool.close()
        self._pool.join()
        logger.info("BackgroundProcessor stopped")
