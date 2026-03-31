import gc
import logging
import queue
import uuid
from datetime import datetime
from pathlib import Path

from pynetdicom import AE

from dicom_ingest.anonymization import Anonymizer
from dicom_ingest.association_tracker import AssociationTracker
from dicom_ingest.background_processor import BackgroundProcessor, get_or_create_generated_patient_id
from dicom_ingest.queries import INSERT_QUERY_DICOM_ASS
from dicom_ingest.settings import (
    SCP_AE_TITLE,
    STAGING_OVERFLOW_DIR,
    STAGING_TMPFS_DIR,
    STAGING_TMPFS_THRESHOLD_PCT,
    USE_FAST_ANONYMIZER,
)
from dicom_ingest.staging import StagingManager

logger = logging.getLogger(__name__)


class DicomStoreHandler:
    def __init__(self, db, path_recipes, nifti_converter=None):
        self.db = db
        self.ae = AE(ae_title=SCP_AE_TITLE)
        self._nifti_converter = nifti_converter

        self.anonymizer = Anonymizer(path_files=path_recipes)
        uuids_file = Path(path_recipes) / "uuids.txt"
        with uuids_file.open() as f:
            self.valid_uuids = [line.strip() for line in f if line.strip()]

        self.staging = StagingManager(
            tmpfs_dir=STAGING_TMPFS_DIR,
            overflow_dir=STAGING_OVERFLOW_DIR,
            tmpfs_threshold_pct=STAGING_TMPFS_THRESHOLD_PCT,
        )

        self.tracker = AssociationTracker(
            on_complete_callback=self._on_association_complete,
            on_patient_complete_callback=self._on_patient_complete,
        )
        self.processor = BackgroundProcessor(
            anonymizer=self.anonymizer,
            db=self.db,
            tracker=self.tracker,
            path_recipes=path_recipes,
            staging=self.staging,
            use_fast_anonymizer=USE_FAST_ANONYMIZER,
        )

    def handle_assoc_open(self, event):
        assoc_id = str(uuid.uuid4())
        ae_title = event.assoc.requestor.ae_title
        ae_address = event.assoc.requestor.address
        ae_port = event.assoc.requestor.port
        event.assoc.assoc_id = assoc_id
        self.tracker.register(assoc_id)
        params = (assoc_id, ae_title, ae_address, ae_port, datetime.now())
        logger.debug("\n%s", "=" * 70)
        logger.debug("NEW ASSOCIATION OPENED")
        logger.debug("Association ID: %s", assoc_id)
        logger.debug("Client: %s (%s:%s)", ae_title, ae_address, ae_port)
        logger.debug("Time: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        logger.debug("%s", "=" * 70)
        self.db.execute_query(INSERT_QUERY_DICOM_ASS, params)

    def handle_assoc_close(self, event):
        assoc_id = getattr(event.assoc, "assoc_id", None)
        if assoc_id is None:
            logger.warning("Association closed without an assoc_id")
            return
        logger.debug("\n%s", "=" * 70)
        logger.debug("ASSOCIATION CLOSED")
        logger.debug("Association ID: %s", assoc_id)
        logger.debug("Time: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        logger.debug("%s", "=" * 70)
        self.tracker.mark_closed(assoc_id)

    def handle_store(self, event):
        ds = event.dataset
        ds.file_meta = event.file_meta
        assoc_id = event.assoc.assoc_id
        study_uid = getattr(ds, "StudyInstanceUID", None)
        if self.valid_uuids:
            if study_uid not in self.valid_uuids:
                logger.error(
                    "REJECTED: Study UID %s not in allowed list. Client: %s@%s",
                    study_uid,
                    event.assoc.requestor.ae_title,
                    event.assoc.requestor.address,
                )
                return 0xC211

        patient_id = getattr(ds, "PatientID", None)
        sop_uid = getattr(ds, "SOPInstanceUID", "UNKNOWN")

        generated_patient_id = None
        if patient_id:
            try:
                generated_patient_id = get_or_create_generated_patient_id(self.db, patient_id)
            except Exception:
                logger.exception("Failed to get/create generated patient ID for %s", patient_id)

        try:
            staged = self.staging.stage(ds, assoc_id, sop_uid)
        except OSError:
            logger.exception("Staging failed (disk full?) for SOP %s", sop_uid)
            return 0xA700

        self.tracker.record_file(assoc_id, patient_id)

        try:
            self.processor.enqueue(staged, assoc_id, patient_id, generated_patient_id, sop_uid)
        except (queue.Full, TimeoutError):
            logger.warning("Processing queue full after timeout, returning 0xA700 for SOP %s", sop_uid)
            self.staging.cleanup(staged)
            return 0xA700

        return 0x0000

    def _on_patient_complete(self, assoc_id, original_patient_id):
        row = self.db.fetch_one(
            "SELECT generated_patient_id FROM patient_id_map WHERE original_patient_id = %s",
            (original_patient_id,),
        )
        if row is None:
            logger.warning("No generated patient ID for patient %s in assoc %s", original_patient_id, assoc_id)
            return
        anon_patient_id = row[0]

        query = """
            SELECT DISTINCT study_instance_uid
            FROM dicom_insert
            WHERE assoc_id = %s AND patient_id = %s
        """
        studies = self.db.fetch_all(query, (assoc_id, anon_patient_id))
        if not studies:
            logger.warning("Patient %s complete but no studies found", original_patient_id)
            return

        for (study_uid,) in studies:
            if self._nifti_converter is not None:
                self._nifti_converter.schedule(study_uid, anon_patient_id)

        gc.collect()

    def _on_association_complete(self, assoc_id, state):
        logger.info(
            "Association %s complete — processed=%s, errors=%s",
            assoc_id,
            state.processed_count,
            state.error_count,
        )
        if state.error_count > 0:
            logger.warning("Association %s finished with %s errors", assoc_id, state.error_count)
