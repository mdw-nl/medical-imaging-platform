import json
import logging
import os

import pydicom
import requests
import xmltodict
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


class upload_XNAT:
    def __init__(self):
        self.path = "DVH_data"
        self.message_folder = "messages"
        self.output_file = "message.json"

        os.makedirs(self.path, exist_ok=True)

    def create_json_metadata(self, dicom_bundle):
        ds = pydicom.dcmread(dicom_bundle.rt_struct_path, stop_before_pixels=True)

        info_dict = {
            "project": str(ds.BodyPartExamined),
            "subject": str(ds.PatientName),
            "experiment": str(ds.StudyInstanceUID).replace(".", "_"),
        }

        info_path = os.path.join(self.path, "metadata_xnat.json")
        with open(info_path, "w") as f:
            json.dump(info_dict, f, indent=4)

        logger.info(f"Metadata saved to {info_path}")

    def save_DVH(self, output):
        dvh_path = os.path.join(self.path, "DVH.json")
        with open(dvh_path, "w") as f:
            json.dump(output, f, indent=4)

        logger.info(f"DVH saved to {dvh_path}")

    def run(self, output, dicom_bundle):
        self.create_json_metadata(dicom_bundle)
        self.save_DVH(output)


class XNATRetriever:
    """Retrieve RTDOSE, RTPLAN, RTSTRUCT, and CTs from XNAT
    using patient ID and SOPInstanceUID.
    """

    def __init__(self, username="admin", password="admin", base_url="http://localhost:8080"):
        self.username = username
        self.password = password
        self.base_url = base_url
        self.base_url_projects = f"{self.base_url}/data/projects"

    def _get(self, url):
        """Authenticated GET request"""
        resp = requests.get(url, auth=HTTPBasicAuth(self.username, self.password))
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "").lower()

        # dict for JSON responses
        if "application/json" in content_type:
            return resp.json()

        # dict for XML respnses
        if "xml" in content_type:
            return xmltodict.parse(resp.text)

        raise ValueError(f"Unsupported Content-Type '{content_type}' for URL {url}")

    def get_projects(self):
        """Return dict of project nameL"""
        data = self._get(self.base_url_projects)
        projects = data.get("ResultSet", {}).get("Result", [])
        project_urls = {proj["name"]: f"{self.base_url_projects}/{proj['ID']}/subjects" for proj in projects}
        return project_urls

    def get_subjects(self, project_url):
        """Return dict of subject label"""
        subjects_data = self._get(project_url).get("ResultSet", {}).get("Result", [])
        subjects = {}
        for subj in subjects_data:
            subject_url = f"{project_url}/{subj['ID']}/experiments"
            subjects[subj["label"]] = subject_url
        return subjects

    def get_experiments(self, subject_url):
        """Return dict of experiment label → scans URL"""
        experiments_data = self._get(subject_url).get("ResultSet", {}).get("Result", [])
        experiments = {}
        for exp in experiments_data:
            scans_url = f"{subject_url}/{exp['ID']}/scans"
            experiments[exp["label"]] = scans_url
        return experiments

    def get_scans(self, scans_url):
        """Return list of scan info dicts"""
        scans_data = self._get(scans_url).get("ResultSet", {}).get("Result", [])
        return scans_data

    def get_dicom_catalog(self, scan_uri):
        """Retrieve the DICOM catalog XML for a scan"""
        url = f"{self.base_url}{scan_uri}/resources/DICOM"
        resp = requests.get(url, auth=HTTPBasicAuth(self.username, self.password))
        resp.raise_for_status()
        return resp.text

    def extract_and_check_sopinstance_entries(self, catalog_dict, SOPinstanceUID):
        """Extract SOPInstanceUIDs and file URIs from an XNAT DICOM catalog. Also checks if SOPinstanceUID correspond"""
        try:
            entries = catalog_dict.get("cat:DCMCatalog", {}).get("cat:entries", {}).get("cat:entry", [])
        except (KeyError, TypeError) as exc:
            raise ValueError("Invalid catalog structure") from exc

        if isinstance(entries, dict):
            entries = [entries]

        for entry in entries:
            sop_uid = entry.get("@UID")
            file_uri = entry.get("@URI")

            if not sop_uid or not file_uri:
                continue

            if sop_uid == SOPinstanceUID:
                return {
                    "sop_uid": sop_uid,
                    "file_uri": file_uri,
                    "filename": entry.get("@ID"),
                    "format": entry.get("@format"),
                }

        # No matching SOPInstanceUID found
        return False

    def download_dicom_to_file(self, url, out_dir, filename):
        """Download a DICOM file from XNAT"""
        os.makedirs(out_dir, exist_ok=True)

        out_path = os.path.join(out_dir, filename)

        with requests.get(url, auth=HTTPBasicAuth(self.username, self.password), stream=True) as r:
            r.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        return out_path

    def check_patient_location(self, patient_name, study_instance_uid):
        """Return a list of all the urls where the patient name and is found in xnat"""
        self.patient_urls = []

        projects = self.get_projects()
        for project in projects:
            subjects = self.get_subjects(projects[project])

            if patient_name not in subjects:
                continue

            experiments = self.get_experiments(subjects[patient_name])

            if study_instance_uid not in experiments:
                continue

            url = experiments[study_instance_uid]
            self.patient_urls.append(url)

    def get_rtdose(self, SOPinstanceUID):
        """Download the RTDOSE based on patient_name, study_instance_uid, and SOPInstanceUID from XNAT"""
        # Loop over all patient experiment URLs where the patient exists
        for url in self.patient_urls:
            scans = self.get_scans(url)
            # Iterate through each scan looking for RTDOSE series
            for scan in scans:
                # Build the URL to the DICOM resource for this scan
                uri = scan["URI"]
                url = self.base_url + uri + "/resources/DICOM"

                try:
                    catalog = self._get(url)
                except Exception:
                    continue

                uid = self.extract_and_check_sopinstance_entries(catalog, SOPinstanceUID)
                if uid is False:  # Skip if SOPInstanceUID not found
                    continue

                folder_path = self.current_out_dir
                file_name = f"rt_dose_{uid['filename']}"

                donwload_url = f"{url}/files/{uid['file_uri']}"

                # Download the RTDOSE file to the local directory
                self.download_dicom_to_file(donwload_url, folder_path, file_name)

                return os.path.join(folder_path, file_name)
        raise FileNotFoundError(f"SOPInstanceUID {SOPinstanceUID} not found for this patient.")

    def download_by_sop(self, sop):
        """Download the RTPLAN file corresponding to a given RTDOSE."""
        # Read RTDOSE to get the referenced RTPLAN SOPInstanceUID

        for patient_url in self.patient_urls:
            scans = self.get_scans(patient_url)

            for scan in scans:
                scan_uri = scan["URI"]

                # Fetch available resources for this scan
                resources_url = f"{self.base_url}{scan_uri}/resources"
                resources_catalog = self._get(resources_url)
                resources = resources_catalog.get("ResultSet", {}).get("Result", [])
                for resource in resources:
                    resource_uri = resource["content"]
                    dicom_resource_url = f"{self.base_url}{scan_uri}/resources/{resource_uri}"
                    try:
                        catalog_dict = self._get(dicom_resource_url)
                    except Exception:
                        continue

                    uid_entry = self.extract_and_check_sopinstance_entries(catalog_dict, sop)

                    if not uid_entry:
                        continue
                    # Build download URL and save locally
                    download_url = f"{dicom_resource_url}/files/{uid_entry['file_uri']}"
                    filename = f"{uid_entry['filename']}"
                    folder_path = self.current_out_dir

                    self.download_dicom_to_file(download_url, folder_path, filename)
                    return os.path.join(folder_path, filename)
        raise FileNotFoundError(f"SOPInstanceUID {sop} not found for this patient.")

    def extract_ct_sop_uids_from_rtstruct(self, ds_rtstruct):
        """Return a set of CT SOPInstanceUIDs referenced by the RTSTRUCT."""
        ct_sops = set()

        try:
            for ref_for in ds_rtstruct.ReferencedFrameOfReferenceSequence:
                for ref_study in ref_for.RTReferencedStudySequence:
                    for ref_series in ref_study.RTReferencedSeriesSequence:
                        for img in ref_series.ContourImageSequence:
                            ct_sops.add(img.ReferencedSOPInstanceUID)
        except AttributeError:
            raise ValueError("RTSTRUCT does not contain ContourImageSequence references")

        if not ct_sops:
            raise ValueError("No CT SOPInstanceUIDs found in RTSTRUCT")

        return ct_sops

    def download_ct_series_from_rtstruct(self, rtstruct_path):
        ds_struct = pydicom.dcmread(rtstruct_path)
        ct_sops = self.extract_ct_sop_uids_from_rtstruct(ds_struct)

        downloaded = []

        for patient_url in self.patient_urls:
            scans = self.get_scans(patient_url)

            for scan in scans:
                scan_uri = scan["URI"]
                dicom_resource_url = f"{self.base_url}{scan_uri}/resources/DICOM"

                try:
                    catalog = self._get(dicom_resource_url)
                except Exception:
                    continue

                for sop in ct_sops:
                    uid_entry = self.extract_and_check_sopinstance_entries(catalog, sop)
                    if not uid_entry:
                        continue

                    download_url = f"{dicom_resource_url}/files/{uid_entry['file_uri']}"
                    out_dir = os.path.join(self.current_out_dir, "ct")
                    filename = uid_entry["filename"]

                    path = self.download_dicom_to_file(download_url, out_dir, filename)
                    downloaded.append(path)

            if len(downloaded) == len(ct_sops):
                return True

        if not downloaded:
            raise FileNotFoundError("No CT images found for RTSTRUCT")

        return False

    def run(self, API, modality):

        url = API
        payload = {"modality": modality}
        headers = {"Content-Type": "application/json"}

        response = requests.post(url, json=payload, headers=headers)
        response_data = response.json()

        for entry in response_data.get("new_sop_instances", []):
            sop_instance_uid = entry["sop_instance_uid"]
            study_instance_uid = entry["study_instance_uid"]
            xnat_experiment_label = study_instance_uid.replace(".", "_")
            patient_name = entry["patient_name"]

            self.current_out_dir = os.path.join("data/xnat_listener", sop_instance_uid)
            os.makedirs(self.current_out_dir, exist_ok=True)

            self.check_patient_location(patient_name, xnat_experiment_label)
            rtdose_path = self.get_rtdose(sop_instance_uid)

            ds_dose = pydicom.dcmread(rtdose_path)
            try:
                rtplan_sop = ds_dose.ReferencedRTPlanSequence[0].ReferencedSOPInstanceUID
            except (AttributeError, IndexError):
                raise ValueError("RTDOSE does not reference any RTPLAN.")

            rtplan_path = self.download_by_sop(rtplan_sop)

            ds_plan = pydicom.dcmread(rtplan_path)
            try:
                rtstruct_sop = ds_plan.ReferencedStructureSetSequence[0].ReferencedSOPInstanceUID
            except (AttributeError, IndexError):
                raise ValueError("RTPLAN does not reference any RTSTRUCT.")

            rtstruct_path = self.download_by_sop(rtstruct_sop)
            self.download_ct_series_from_rtstruct(rtstruct_path)


if __name__ == "__main__":
    retriever = XNATRetriever(base_url="http://localhost:8080", username="admin", password="admin")
    # patient_name = "SEDI_TEST001"
    # study_instance_uid = "99999_8088316119225601241627216725805872478376234007905444525746"
    # SOPinstanceUID = "99999.1254976680351246889122584535917886712943150884553757806011"
    retriever.run("http://localhost:9000/sop_instance_uids", "RTDOSE")
