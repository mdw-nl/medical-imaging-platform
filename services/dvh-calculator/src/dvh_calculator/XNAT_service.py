"""Upload DVH results to XNAT and retrieve RT DICOM files from XNAT."""

import json
import logging
import os
from pathlib import Path

import pydicom
import requests
import xmltodict
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


class XNATUploader:
    """Save DVH output and DICOM metadata as JSON files for XNAT upload."""

    def __init__(self):
        self.path = Path("DVH_data")
        self.path.mkdir(parents=True, exist_ok=True)

    def create_json_metadata(self, dicom_bundle):
        """Extract project/subject/experiment metadata from the RT struct and save as JSON."""
        ds = pydicom.dcmread(dicom_bundle.rt_struct_path, stop_before_pixels=True)

        info_dict = {
            "project": str(ds.BodyPartExamined),
            "subject": str(ds.PatientName),
            "experiment": str(ds.StudyInstanceUID).replace(".", "_"),
        }

        info_path = self.path / "metadata_xnat.json"
        with info_path.open("w") as f:
            json.dump(info_dict, f, indent=4)

        logger.info("Metadata saved to %s", info_path)

    def save_DVH(self, output):
        """Write DVH calculation output to a JSON file."""
        dvh_path = self.path / "DVH.json"
        with dvh_path.open("w") as f:
            json.dump(output, f, indent=4)

        logger.info("DVH saved to %s", dvh_path)

    def run(self, output, dicom_bundle):
        """Save metadata and DVH output for a DICOM bundle."""
        self.create_json_metadata(dicom_bundle)
        self.save_DVH(output)


class XNATRetriever:
    """Retrieve RTDOSE, RTPLAN, RTSTRUCT, and CTs from XNAT
    using patient ID and SOPInstanceUID.
    """

    def __init__(
        self,
        username: str | None = None,
        password: str | None = None,
        base_url: str = "http://localhost:8080",
    ):
        self.username = username or os.getenv("XNAT_API_USER")
        self.password = password or os.getenv("XNAT_API_PASSWORD")
        if not self.username or not self.password:
            raise ValueError("XNAT credentials required: pass username/password or set XNAT_API_USER/XNAT_API_PASSWORD")
        self.base_url = base_url
        self.base_url_projects = f"{self.base_url}/data/projects"

    def _get(self, url):
        """Authenticated GET request."""
        resp = requests.get(url, auth=HTTPBasicAuth(self.username, self.password), timeout=30)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "").lower()

        if "application/json" in content_type:
            return resp.json()

        if "xml" in content_type:
            return xmltodict.parse(resp.text)

        raise ValueError(f"Unsupported Content-Type '{content_type}' for URL {url}")

    def get_projects(self):
        """Return dict of project names."""
        data = self._get(self.base_url_projects)
        projects = data.get("ResultSet", {}).get("Result", [])
        return {proj["name"]: f"{self.base_url_projects}/{proj['ID']}/subjects" for proj in projects}

    def get_subjects(self, project_url):
        """Return dict of subject labels."""
        subjects_data = self._get(project_url).get("ResultSet", {}).get("Result", [])
        subjects = {}
        for subj in subjects_data:
            subject_url = f"{project_url}/{subj['ID']}/experiments"
            subjects[subj["label"]] = subject_url
        return subjects

    def get_experiments(self, subject_url):
        """Return dict of experiment label to scans URL."""
        experiments_data = self._get(subject_url).get("ResultSet", {}).get("Result", [])
        experiments = {}
        for exp in experiments_data:
            scans_url = f"{subject_url}/{exp['ID']}/scans"
            experiments[exp["label"]] = scans_url
        return experiments

    def get_scans(self, scans_url):
        """Return list of scan info dicts."""
        return self._get(scans_url).get("ResultSet", {}).get("Result", [])

    def get_dicom_catalog(self, scan_uri):
        """Retrieve the DICOM catalog XML for a scan."""
        url = f"{self.base_url}{scan_uri}/resources/DICOM"
        resp = requests.get(url, auth=HTTPBasicAuth(self.username, self.password), timeout=30)
        resp.raise_for_status()
        return resp.text

    def extract_and_check_sopinstance_entries(self, catalog_dict, SOPinstanceUID):
        """Extract SOPInstanceUIDs and file URIs from an XNAT DICOM catalog."""
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

        return False

    def download_dicom_to_file(self, url, out_dir, filename):
        """Download a DICOM file from XNAT."""
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / filename

        with requests.get(url, auth=HTTPBasicAuth(self.username, self.password), stream=True, timeout=60) as r:
            r.raise_for_status()
            with out_path.open("wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        return str(out_path)

    def check_patient_location(self, patient_name, study_instance_uid):
        """Return a list of all the urls where the patient is found in XNAT."""
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
        """Download the RTDOSE based on SOPInstanceUID from XNAT."""
        for url in self.patient_urls:
            scans = self.get_scans(url)
            for scan in scans:
                uri = scan["URI"]
                resource_url = self.base_url + uri + "/resources/DICOM"

                try:
                    catalog = self._get(resource_url)
                except requests.RequestException:
                    logger.debug("Failed to fetch resource %s", resource_url)
                    continue

                uid = self.extract_and_check_sopinstance_entries(catalog, SOPinstanceUID)
                if uid is False:
                    continue

                folder_path = self.current_out_dir
                file_name = f"rt_dose_{uid['filename']}"

                download_url = f"{resource_url}/files/{uid['file_uri']}"

                self.download_dicom_to_file(download_url, folder_path, file_name)

                return str(Path(folder_path) / file_name)
        raise FileNotFoundError(f"SOPInstanceUID {SOPinstanceUID} not found for this patient.")

    def download_by_sop(self, sop):
        """Download a DICOM file corresponding to a given SOP UID."""
        for patient_url in self.patient_urls:
            scans = self.get_scans(patient_url)

            for scan in scans:
                scan_uri = scan["URI"]

                resources_url = f"{self.base_url}{scan_uri}/resources"
                resources_catalog = self._get(resources_url)
                resources = resources_catalog.get("ResultSet", {}).get("Result", [])
                for resource in resources:
                    resource_uri = resource["content"]
                    dicom_resource_url = f"{self.base_url}{scan_uri}/resources/{resource_uri}"
                    try:
                        catalog_dict = self._get(dicom_resource_url)
                    except requests.RequestException:
                        logger.debug("Failed to fetch resource %s", dicom_resource_url)
                        continue

                    uid_entry = self.extract_and_check_sopinstance_entries(catalog_dict, sop)

                    if not uid_entry:
                        continue
                    download_url = f"{dicom_resource_url}/files/{uid_entry['file_uri']}"
                    filename = uid_entry["filename"]
                    folder_path = self.current_out_dir

                    self.download_dicom_to_file(download_url, folder_path, filename)
                    return str(Path(folder_path) / filename)
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
        except AttributeError as exc:
            raise ValueError("RTSTRUCT does not contain ContourImageSequence references") from exc

        if not ct_sops:
            raise ValueError("No CT SOPInstanceUIDs found in RTSTRUCT")

        return ct_sops

    def download_ct_series_from_rtstruct(self, rtstruct_path):
        """Download all CT slices referenced by the given RTSTRUCT file."""
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
                except requests.RequestException:
                    logger.debug("Failed to fetch resource %s", dicom_resource_url)
                    continue

                for sop in ct_sops:
                    uid_entry = self.extract_and_check_sopinstance_entries(catalog, sop)
                    if not uid_entry:
                        continue

                    download_url = f"{dicom_resource_url}/files/{uid_entry['file_uri']}"
                    out_dir = str(Path(self.current_out_dir) / "ct")
                    filename = uid_entry["filename"]

                    path = self.download_dicom_to_file(download_url, out_dir, filename)
                    downloaded.append(path)

            if len(downloaded) == len(ct_sops):
                return True

        if not downloaded:
            raise FileNotFoundError("No CT images found for RTSTRUCT")

        return False

    def run(self, API, modality):
        """Poll for new SOP instances and download the full RT file set from XNAT."""
        url = API
        payload = {"modality": modality}
        headers = {"Content-Type": "application/json"}

        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response_data = response.json()

        for entry in response_data.get("new_sop_instances", []):
            sop_instance_uid = entry["sop_instance_uid"]
            study_instance_uid = entry["study_instance_uid"]
            xnat_experiment_label = study_instance_uid.replace(".", "_")
            patient_name = entry["patient_name"]

            self.current_out_dir = str(Path("data/xnat_listener") / sop_instance_uid)
            Path(self.current_out_dir).mkdir(parents=True, exist_ok=True)

            self.check_patient_location(patient_name, xnat_experiment_label)
            rtdose_path = self.get_rtdose(sop_instance_uid)

            ds_dose = pydicom.dcmread(rtdose_path)
            try:
                rtplan_sop = ds_dose.ReferencedRTPlanSequence[0].ReferencedSOPInstanceUID
            except (AttributeError, IndexError) as exc:
                raise ValueError("RTDOSE does not reference any RTPLAN.") from exc

            rtplan_path = self.download_by_sop(rtplan_sop)

            ds_plan = pydicom.dcmread(rtplan_path)
            try:
                rtstruct_sop = ds_plan.ReferencedStructureSetSequence[0].ReferencedSOPInstanceUID
            except (AttributeError, IndexError) as exc:
                raise ValueError("RTPLAN does not reference any RTSTRUCT.") from exc

            rtstruct_path = self.download_by_sop(rtstruct_sop)
            self.download_ct_series_from_rtstruct(rtstruct_path)
