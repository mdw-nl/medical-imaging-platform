import os

from rt_utils import RTStructBuilder


def load_mask(dicom_folder, rtstruct_filename, ROI_label):
    # Load the RTSTRUCT file
    rtstruct_path = os.path.join(dicom_folder, rtstruct_filename)
    dicom_series_path = os.path.join(dicom_folder)
    rtstruct = RTStructBuilder.create_from(dicom_series_path, rtstruct_path)

    mask_3d = rtstruct.get_roi_mask_by_name(ROI_label)

    return mask_3d
