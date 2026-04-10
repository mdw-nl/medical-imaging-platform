"""DICOM C-STORE SCP handler — receives, validates, stages, and enqueues DICOM files."""

import gc
import logging
import queue
import uuid
from datetime import datetime
from pathlib import Path

from pynetdicom import AE

from imaging_hub.anonymization import Anonymizer
from imaging_hub.association_tracker import AssociationTracker
from imaging_hub.background_processor import BackgroundProcessor, get_or_create_generated_patient_id
from imaging_hub.queries import INSERT_QUERY_DICOM_ASS
from imaging_hub.settings import (
    STAGING_OVERFLOW_DIR,
    STAGING_TMPFS_DIR,
    STAGING_TMPFS_THRESHOLD_PCT,
)
from imaging_hub.staging import StagingManager

logger = logging.getLogger(__name__)

DIMSE_SUCCESS = 0x0000
DIMSE_OUT_OF_RESOURCES = 0xA700
DIMSE_STUDY_UID_REJECTED = 0xC211


class DicomStoreHandler:
    """Coordinate DICOM association lifecycle, staging, and background processing."""

    def __init__(
        self, db, path_recipes, anonymization_settings, nifti_converter=None, ae_titles: list[str] | None = None
    ):
        self.db = db
        ae_titles = ae_titles or ["MY_SCP"]
        self._accepted_ae_titles = {t.strip().upper() for t in ae_titles}
        self.ae = AE(ae_title=ae_titles[0])
        self._nifti_converter = nifti_converter

        self.anonymizer = Anonymizer(settings=anonymization_settings, recipes_dir=path_recipes)
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
            anonymization_settings=anonymization_settings,
        )

    def handle_assoc_open(self, event):
        """Assign a unique ID to a newly opened association and register it."""
        assoc_id = str(uuid.uuid4())
        event.assoc.assoc_id = assoc_id
        self.tracker.register(assoc_id)

    def handle_assoc_requested(self, event):
        """Validate the Called AE Title and log the association request to the database."""
        assoc_id = getattr(event.assoc, "assoc_id", str(uuid.uuid4()))
        primitive = event.assoc.requestor.primitive
        called_aet = primitive.called_ae_title.strip().upper()
        calling_aet = primitive.calling_ae_title.strip()

        if called_aet not in self._accepted_ae_titles:
            logger.warning(
                "REJECTED association %s: Called AE Title '%s' not in accepted list %s",
                assoc_id,
                called_aet,
                self._accepted_ae_titles,
            )
            event.assoc.abort()
            return

        event.assoc.called_project = called_aet
        ae_address = event.assoc.requestor.address
        ae_port = event.assoc.requestor.port
        logger.debug("\n%s", "=" * 70)
        logger.debug("ASSOCIATION REQUESTED")
        logger.debug("Association ID: %s", assoc_id)
        logger.debug("Calling AE: %s, Called AE (project): %s", calling_aet, called_aet)
        logger.debug("Client: %s:%s", ae_address, ae_port)
        logger.debug("%s", "=" * 70)
        params = (assoc_id, calling_aet, ae_address, ae_port, datetime.now(), called_aet)
        self.db.execute_query(INSERT_QUERY_DICOM_ASS, params)

    def handle_assoc_close(self, event):
        """Mark the association as closed in the tracker."""
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
        """Stage an incoming DICOM dataset and enqueue it for anonymization."""
        ds = event.dataset
        ds.file_meta = event.file_meta
        assoc_id = event.assoc.assoc_id
        project = getattr(event.assoc, "called_project", None)
        study_uid = getattr(ds, "StudyInstanceUID", None)
        if self.valid_uuids:
            if study_uid not in self.valid_uuids:
                logger.error(
                    "REJECTED: Study UID %s not in allowed list. Client: %s@%s",
                    study_uid,
                    event.assoc.requestor.ae_title,
                    event.assoc.requestor.address,
                )
                return DIMSE_STUDY_UID_REJECTED

        patient_id = getattr(ds, "PatientID", None)
        sop_uid = getattr(ds, "SOPInstanceUID", "UNKNOWN")

        if patient_id and not self.anonymizer.is_patient_known(patient_id):
            logger.warning(
                "SKIPPED: unknown PatientID (not in patient_lookup.csv). SOP %s from %s",
                sop_uid,
                event.assoc.requestor.ae_title,
            )
            return DIMSE_SUCCESS

        generated_patient_id = None
        if patient_id:
            try:
                generated_patient_id = get_or_create_generated_patient_id(self.db, patient_id)
            except Exception:
                logger.exception("Failed to get/create generated patient ID for incoming SOP %s", sop_uid)

        try:
            staged = self.staging.stage(ds, assoc_id, sop_uid)
        except OSError:
            logger.exception("Staging failed (disk full?) for SOP %s", sop_uid)
            return DIMSE_OUT_OF_RESOURCES

        self.tracker.record_file(assoc_id, patient_id)

        try:
            self.processor.enqueue(staged, assoc_id, patient_id, generated_patient_id, sop_uid, project)
        except (queue.Full, TimeoutError):
            logger.warning("Processing queue full after timeout, returning 0xA700 for SOP %s", sop_uid)
            self.staging.cleanup(staged)
            return DIMSE_OUT_OF_RESOURCES

        return DIMSE_SUCCESS

    def _on_patient_complete(self, assoc_id, original_patient_id):
        row = self.db.fetch_one(
            "SELECT generated_patient_id FROM patient_id_map WHERE original_patient_id = %s",
            (original_patient_id,),
        )
        if row is None:
            logger.warning("No generated patient ID found in assoc %s", assoc_id)
            return
        anon_patient_id = row[0]

        query = """
            SELECT DISTINCT study_instance_uid
            FROM dicom_insert
            WHERE assoc_id = %s AND patient_id = %s
        """
        studies = self.db.fetch_all(query, (assoc_id, anon_patient_id))
        if not studies:
            logger.warning("Patient complete in assoc %s but no studies found (anon_id=%s)", assoc_id, anon_patient_id)
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
