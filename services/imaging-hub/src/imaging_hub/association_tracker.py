"""Track DICOM association and per-patient progress to fire completion callbacks."""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class PatientState:
    """Per-patient file reception and processing counters within an association."""

    expected_count: int = 0
    processed_count: int = 0
    error_count: int = 0
    reception_complete: bool = False
    completed: bool = False


@dataclass
class AssociationState:
    """Aggregate file reception and processing counters for a DICOM association."""

    expected_count: int = 0
    processed_count: int = 0
    error_count: int = 0
    closed: bool = False
    completed: bool = False
    patient_states: dict = field(default_factory=dict)
    current_patient_id: str | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)


class AssociationTracker:
    """Monitor DICOM associations and invoke callbacks when all files are processed."""

    def __init__(self, on_complete_callback, on_patient_complete_callback=None):
        self._associations: dict[str, AssociationState] = {}
        self._global_lock = threading.Lock()
        self._on_complete = on_complete_callback
        self._on_patient_complete = on_patient_complete_callback
        self._completion_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="assoc-complete")

    def register(self, assoc_id: str):
        """Create a fresh AssociationState for *assoc_id*."""
        with self._global_lock:
            self._associations[assoc_id] = AssociationState()
        logger.debug("Tracker: registered association %s", assoc_id)

    def record_file(self, assoc_id: str, patient_id: str | None):
        """Increment the expected file count and detect patient-boundary transitions."""
        with self._global_lock:
            state = self._associations.get(assoc_id)
        if state is None:
            logger.error("Tracker: unknown association %s", assoc_id)
            return
        with state.lock:
            state.expected_count += 1
            if patient_id is not None:
                old_patient = state.current_patient_id
                if old_patient != patient_id:
                    state.current_patient_id = patient_id
                    if old_patient is not None:
                        old_pstate = state.patient_states.get(old_patient)
                        if old_pstate is not None and not old_pstate.reception_complete:
                            old_pstate.reception_complete = True
                            logger.debug(
                                "Tracker: %s/%s reception complete (patient boundary)",
                                assoc_id,
                                old_patient,
                            )
                            self._check_patient_complete(assoc_id, old_patient, old_pstate)
                if patient_id not in state.patient_states:
                    state.patient_states[patient_id] = PatientState()
                state.patient_states[patient_id].expected_count += 1
            logger.debug("Tracker: %s expected=%s", assoc_id, state.expected_count)

    def record_processed(self, assoc_id: str, patient_id: str | None = None):
        """Record a successfully processed file and check for completion."""
        with self._global_lock:
            state = self._associations.get(assoc_id)
        if state is None:
            logger.error("Tracker: unknown association %s", assoc_id)
            return
        with state.lock:
            state.processed_count += 1
            logger.debug(
                "Tracker: %s processed=%s/%s",
                assoc_id,
                state.processed_count,
                state.expected_count,
            )
            if patient_id is not None:
                patient_state = state.patient_states.get(patient_id)
                if patient_state is not None:
                    patient_state.processed_count += 1
                    self._check_patient_complete(assoc_id, patient_id, patient_state)
            self._check_complete(assoc_id, state)

    def record_error(self, assoc_id: str, patient_id: str | None = None):
        """Record a processing error and check for completion."""
        with self._global_lock:
            state = self._associations.get(assoc_id)
        if state is None:
            logger.error("Tracker: unknown association %s", assoc_id)
            return
        with state.lock:
            state.error_count += 1
            logger.debug(
                "Tracker: %s errors=%s/%s",
                assoc_id,
                state.error_count,
                state.expected_count,
            )
            if patient_id is not None:
                patient_state = state.patient_states.get(patient_id)
                if patient_state is not None:
                    patient_state.error_count += 1
                    self._check_patient_complete(assoc_id, patient_id, patient_state)
            self._check_complete(assoc_id, state)

    def mark_closed(self, assoc_id: str):
        """Mark the association as closed and finalize any remaining patient state."""
        with self._global_lock:
            state = self._associations.get(assoc_id)
        if state is None:
            logger.error("Tracker: unknown association %s", assoc_id)
            return
        with state.lock:
            state.closed = True
            logger.debug("Tracker: %s marked closed", assoc_id)
            current = state.current_patient_id
            if current is not None:
                patient_state = state.patient_states.get(current)
                if patient_state is not None and not patient_state.reception_complete:
                    patient_state.reception_complete = True
                    logger.debug(
                        "Tracker: %s/%s reception complete (association closed)",
                        assoc_id,
                        current,
                    )
                    self._check_patient_complete(assoc_id, current, patient_state)
            self._check_complete(assoc_id, state)

    def _check_patient_complete(self, assoc_id: str, patient_id: str, patient_state: PatientState):
        if (
            patient_state.reception_complete
            and patient_state.processed_count + patient_state.error_count >= patient_state.expected_count
            and not patient_state.completed
        ):
            patient_state.completed = True
            logger.info(
                "Tracker: %s/%s patient complete — processed=%s, errors=%s, expected=%s",
                assoc_id,
                patient_id,
                patient_state.processed_count,
                patient_state.error_count,
                patient_state.expected_count,
            )
            if self._on_patient_complete is not None:
                self._completion_pool.submit(self._run_patient_callback, assoc_id, patient_id)

    def _check_complete(self, assoc_id: str, state: AssociationState):
        if state.closed and state.processed_count + state.error_count >= state.expected_count and not state.completed:
            state.completed = True
            logger.info(
                "Tracker: %s complete — processed=%s, errors=%s, expected=%s",
                assoc_id,
                state.processed_count,
                state.error_count,
                state.expected_count,
            )
            with self._global_lock:
                self._associations.pop(assoc_id, None)
            self._completion_pool.submit(self._run_callback, assoc_id, state)

    def _run_callback(self, assoc_id: str, state: AssociationState):
        try:
            self._on_complete(assoc_id, state)
        except Exception:
            logger.exception("Tracker: on_complete callback failed for %s", assoc_id)

    def _run_patient_callback(self, assoc_id: str, patient_id: str):
        try:
            self._on_patient_complete(assoc_id, patient_id)
        except Exception:
            logger.exception("Tracker: on_patient_complete callback failed for %s/%s", assoc_id, patient_id)

    def shutdown(self, wait=True):
        """Shut down the completion callback thread pool."""
        self._completion_pool.shutdown(wait=wait)
