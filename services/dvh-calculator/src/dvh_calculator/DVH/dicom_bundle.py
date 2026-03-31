import logging
import os

from dicompylercore.dicomparser import DicomParser


class DicomBundle:
    def __init__(self, patient_id, rt_plan: str, rt_struct: str, rt_dose: list, rt_ct: str, read=True):
        self.patient_id = patient_id
        self.rt_plan_path = rt_plan
        self.rt_struct_path: str = rt_struct
        self.rt_ct_path: str | None = rt_ct[: rt_ct.rindex("/") + 1] if rt_ct else None
        if read:
            try:
                self.rt_plan: DicomParser = DicomParser(rt_plan)
                self.rt_struct: DicomParser = DicomParser(rt_struct)
                self.rt_dose: list = [DicomParser(rt) for rt in rt_dose]
                self.rt_dose_path: list = rt_dose
            except Exception as e:
                logging.exception(f"Error reading DICOM files: {e}")
                raise e
        logging.info(f"Ct path is {self.rt_ct_path}")

    def __eq__(self, other):
        if not isinstance(other, DicomBundle):
            return False
        if (
            self.rt_plan_path == other.rt_plan_path
            and self.rt_ct_path == other.rt_ct_path
            and self.rt_struct_path == other.rt_struct_path
        ):
            return True
        return False

    # function to delete all the elemnt using the path of each element
    def rm_data_patient(self):
        try:
            logging.info(f"Removing data for patient {self.patient_id}")
            os.remove(self.rt_plan_path)
            logging.info(f"Removing rt plan  {self.rt_plan_path}")
            os.remove(self.rt_struct_path)
            logging.info(f"Removing data rt struct {self.rt_struct_path}")
            for rt in self.rt_dose_path:
                logging.info(f"Removing data rt dose {self.rt_dose_path}")
                os.remove(rt)
            if self.rt_plan_path is not None and self.rt_ct_path is not None:
                for f in os.listdir(self.rt_ct_path):
                    os.remove(os.path.join(self.rt_ct_path, f))
        except Exception as e:
            logging.warning(f"Error deleting files: {e}")
