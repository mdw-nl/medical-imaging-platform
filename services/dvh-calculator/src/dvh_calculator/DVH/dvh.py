"""DVH calculation engine using dicompyler-core."""

import logging
from uuid import uuid4

import numpy as np
from dicompylercore import dicomparser, dvh, dvhcalc

from dvh_calculator.DVH.dicom_bundle import DicomBundle

logger = logging.getLogger(__name__)


def prepare_output(dvh_points, structure, calc_dvh, dict_value):
    """Build a JSON-LD DVH result dictionary for a single structure."""
    id_data = "http://data.local/ldcm-rt/" + str(uuid4())
    return {
        "@id": id_data,
        "structureName": structure["name"],
        "min": {"@id": f"{id_data}/min", "unit": "Gray", "value": calc_dvh.min},
        "mean": {"@id": f"{id_data}/mean", "unit": "Gray", "value": calc_dvh.mean},
        "max": {"@id": f"{id_data}/max", "unit": "Gray", "value": calc_dvh.max},
        "volume": {"@id": f"{id_data}/volume", "unit": "cc", "value": int(calc_dvh.volume)},
        "D2": {"@id": f"{id_data}/D2", "unit": "Gray", "value": float(calc_dvh.D2.value)},
        "D50": {"@id": f"{id_data}/D50", "unit": "Gray", "value": float(calc_dvh.D50.value)},
        "D95": {"@id": f"{id_data}/D95", "unit": "Gray", "value": float(calc_dvh.D95.value)},
        "D98": {"@id": f"{id_data}/D98", "unit": "Gray", "value": float(calc_dvh.D98.value)},
        "V0": {"@id": f"{id_data}/V0", "unit": "Gray", "value": dict_value["V0value"]},
        "V15": {"@id": f"{id_data}/V15", "unit": "Gray", "value": dict_value["V15value"]},
        "V35": {"@id": f"{id_data}/V35", "unit": "Gray", "value": dict_value["V35value"]},
        "color": ",".join(str(e) for e in structure.get("color", np.array([])).tolist()),
        "dvh_curve": {"@id": f"{id_data}/dvh_curve", "dvh_points": dvh_points},
    }


class DVHCalculation:
    """Starting point for the DVH calculation."""

    def process_dvh_result(self, calculation_r, index, structures):
        """Extract dose-volume points and summary statistics from a DVH result."""
        dvh_d = calculation_r.bincenters.tolist()
        dvh_v = calculation_r.counts.tolist()
        dvh_points = [{"d_point": dvh_d[i], "v_point": dvh_v[i]} for i in range(len(dvh_d))]
        dict_values = {}

        for v in [0, 5, 10, 15, 20, 30, 35]:
            key = f"V{v}value"
            try:
                dict_values[key] = float(getattr(calculation_r, f"V{v}").value)
            except (AttributeError, ValueError, TypeError):
                logger.warning("Value not available for %s, setting to None.", key)
                dict_values[key] = None

        structOut = prepare_output(dvh_points, structures[index], calculation_r, dict_values)
        logger.info("Structure: %s", structOut["structureName"])
        return structOut

    def calculate_dvh_all(self, dicom_bundle: DicomBundle, structures, str_name=None):
        """Calculate DVH for all structures in the bundle, optionally filtered by name."""
        output = []
        if len(structures) > 0:
            for index in structures:
                logger.warning("Calculating structures %s", structures[index])
                if (str_name and structures[index]["name"] == str_name) or not str_name:
                    try:
                        calc_dvh = self.calculate_dvh(index, dicom_bundle)
                    except Exception:
                        logger.exception("Error calculating DVH for structure %s, skipping", structures[index])
                        continue
                    try:
                        logger.info("DVH calculation complete. Processing output...")
                        result = self.process_dvh_result(calc_dvh, index, structures)
                        output.append(result)
                    except Exception:
                        logger.exception("Error processing DVH result for structure %s, skipping", structures[index])
                        continue
                else:
                    logger.info("Skipping structure %s as it is not in the list.", structures[index]["name"])
        else:
            logger.info("NO structures")
        return output

    def calculate_dvh(self, index, dicom_bundle: DicomBundle):
        """Calculate DVH for a single structure index."""
        return self.get_dvh_v(
            structure=dicom_bundle.rt_struct,
            dose_data=dicom_bundle.rt_dose[0],
            roi=index,
            rt_plan_p=dicom_bundle.rt_plan,
        )

    def get_dvh_v(
        self,
        structure,
        dose_data,
        roi,
        rt_plan_p=None,
        limit=None,
        calculate_full_volume=True,
        use_structure_extents=False,
        interpolation_resolution=None,
        interpolation_segments_between_planes=0,
        thickness=None,
        memmap_rtdose=False,
        callback=None,
    ):
        """Calculate a cumulative DVH in Gy from a DICOM RT Structure Set & Dose."""
        rt_str = structure
        if isinstance(dose_data, str):
            rt_dose = dicomparser.DicomParser(dose_data, memmap_pixel_array=memmap_rtdose)
        else:
            rt_dose = dose_data
        structures = rt_str.GetStructures()
        s = structures[roi]
        logger.debug("Structure selected %s", s)
        s["planes"] = rt_str.GetStructureCoordinates(roi)
        s["thickness"] = thickness or rt_str.CalculatePlaneThickness(s["planes"])

        calc_dvh = dvhcalc._calculate_dvh(
            s,
            rt_dose,
            limit,
            calculate_full_volume,
            use_structure_extents,
            interpolation_resolution,
            interpolation_segments_between_planes,
            callback,
        )

        bins = np.arange(0, 2) if calc_dvh.histogram.size == 1 else np.arange(0, calc_dvh.histogram.size + 1) / 100

        dvh_kwargs = {
            "counts": calc_dvh.histogram,
            "bins": bins,
            "dvh_type": "differential",
            "dose_units": "Gy",
            "notes": calc_dvh.notes,
            "name": s["name"],
        }

        if rt_plan_p is not None:
            plan = rt_plan_p.GetPlan()
            if plan["rxdose"] is not None:
                logger.debug("rx dose does exist in the rt plan")
                dvh_kwargs["rx_dose"] = plan["rxdose"] / 100

        return dvh.DVH(**dvh_kwargs).cumulative
