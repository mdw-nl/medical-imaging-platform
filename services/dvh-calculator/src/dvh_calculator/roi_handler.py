"""Combine and inspect ROIs within an RT structure set."""

import numpy as np
from rt_utils import RTStruct


def combine_rois(rtstruct: RTStruct, rois: list[str], operators: list[str]) -> list[bool]:
    """Combine multiple ROI masks using the given boolean operators (+/-)."""
    if len(rois) - 1 != len(operators):
        raise ValueError("There should be exactly one operator less than the number of ROIs.")

    roi_operator_pairs = list(zip(rois[1:], operators, strict=True))
    sorted_pairs = sorted(roi_operator_pairs, key=lambda x: x[1] == "-")
    sorted_rois, sorted_operators = zip(*sorted_pairs, strict=True) if sorted_pairs else ([], [])

    combined_mask = rtstruct.get_roi_mask_by_name(rois[0])

    for roi, operator in zip(sorted_rois, sorted_operators, strict=True):
        roi_mask = rtstruct.get_roi_mask_by_name(roi)

        if operator == "+":
            combined_mask = np.logical_or(combined_mask, roi_mask)
        elif operator == "-":
            combined_mask = np.logical_and(combined_mask, np.logical_not(roi_mask))
        else:
            raise ValueError(f"Invalid operator '{operator}'. Expected '+' or '-'.")

    return combined_mask


def roi_operation(roi_string):
    """Extract operators from alternating token list (e.g. ['A', '+', 'B'] -> ['+'])."""
    return [roi_string[i] for i in range(1, len(roi_string), 2)]


def roi_list(roi_string):
    """Extract ROI names from alternating token list (e.g. ['A', '+', 'B'] -> ['A', 'B'])."""
    return [roi_string[i] for i in range(0, len(roi_string), 2)]


def check_if_roi_exist(roi, rtstruct_roi):
    """Return True if the ROI name exists in the structure set's ROI list."""
    return roi in rtstruct_roi
