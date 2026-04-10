"""DICOM C-STORE SCU for sending files to a PACS SCP."""

import logging
import os
from pathlib import Path

from pydicom import dcmread
from pydicom.uid import ExplicitVRLittleEndian
from pynetdicom import AE, StoragePresentationContexts

logger = logging.getLogger(__name__)

DIMSE_SUCCESS = 0x0000

PACS_SCP_HOST = os.getenv("PACS_SCP_HOST", "xnat-web")
PACS_SCP_PORT = int(os.getenv("PACS_SCP_PORT", "8104"))
PACS_SCP_AE_TITLE = os.getenv("PACS_SCP_AE_TITLE")
PACS_SCU_AE_TITLE = os.getenv("PACS_SCU_AE_TITLE", "DICOM_SORTER_SCU")


class DICOMtoPACS:
    """DICOM SCU that opens an association and C-STOREs files to a remote SCP."""

    def __init__(
        self,
        ae_title: str,
        host: str = PACS_SCP_HOST,
        port: int = PACS_SCP_PORT,
        scu_ae_title: str = PACS_SCU_AE_TITLE,
    ):
        self.host = host
        self.port = port
        self.ae_title = ae_title
        self.ae = AE(ae_title=scu_ae_title)
        for context in StoragePresentationContexts:
            self.ae.add_requested_context(context.abstract_syntax, ExplicitVRLittleEndian)

    def send_files(self, file_paths: list[str], on_sent=None):
        """Open an association and C-STORE each file, calling *on_sent* with the SOP UID on success."""
        assoc = self.ae.associate(self.host, self.port, ae_title=self.ae_title)
        if not assoc.is_established:
            raise ConnectionError(f"Failed to associate with PACS SCP at {self.host}:{self.port}")
        try:
            for file_path in file_paths:
                ds = dcmread(str(file_path), defer_size="2 MB")
                sop_uid = str(ds.SOPInstanceUID)
                status = assoc.send_c_store(ds)
                del ds
                if not status or status.Status != DIMSE_SUCCESS:
                    raise RuntimeError(
                        f"C-STORE failed for {Path(file_path).name}: status={getattr(status, 'Status', None)}"
                    )
                logger.debug("C-STORE succeeded: %s", Path(file_path).name)
                if on_sent:
                    on_sent(sop_uid)
        finally:
            assoc.release()
