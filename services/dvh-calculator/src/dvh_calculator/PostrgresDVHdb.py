import os
from pathlib import Path

from imaging_common import PostgresInterface, load_yaml_config

_CONFIG_PATH = Path(os.getenv("CONFIG_PATH", str(Path(__file__).parents[2] / "config" / "config.yaml")))
_config = load_yaml_config(_CONFIG_PATH)


def create_dvh_tables(db: PostgresInterface):
    """This function is to create tables when docker compose up"""
    db.create_table(
        "dvh_result",
        {
            "result_id": "SERIAL PRIMARY KEY",
            "json_id": "TEXT UNIQUE NOT NULL",  # store your ROI @id
            "dose_bins": "DOUBLE PRECISION[] NOT NULL",
            "volume_bins": "DOUBLE PRECISION[] NOT NULL",
            "D2": "DOUBLE PRECISION",
            "D50": "DOUBLE PRECISION",
            "D95": "DOUBLE PRECISION",
            "D98": "DOUBLE PRECISION",
            "min_dose": "DOUBLE PRECISION",
            "mean_dose": "DOUBLE PRECISION",
            "max_dose": "DOUBLE PRECISION",
            "V0": "DOUBLE PRECISION",
            "V15": "DOUBLE PRECISION",
            "V35": "DOUBLE PRECISION",
        },
    )

    db.create_table(
        "dvh_package",
        {
            "sop_instance_uid": "TEXT NOT NULL",
            "roi_name": "TEXT NOT NULL",
            "result_id": "INTEGER NOT NULL REFERENCES dvh_result(result_id) ON DELETE CASCADE",
        },
    )


# A class to upload actual data to postgress
class upload_pg:
    def __init__(self):
        # file_d = read_config()
        # file_d = Config("postgres").config
        self.postgres_config = _config["postgres"]
        # file_d.get("postgres", {})

    def SOP_UID_rtose(self, dicom_bundle):
        """Get SOP UID for rtdose"""
        ds = dicom_bundle.rt_dose[0].ds  # get the underlying pydicom Dataset
        return ds.SOPInstanceUID

    def extract_roi_dvh(self, roi_dvh):
        """Extract all relevant info from a single ROI DVH dictionary."""
        roi_name = roi_dvh["structureName"]
        json_id = roi_dvh["@id"]

        d_points = [pt["d_point"] for pt in roi_dvh["dvh_curve"]["dvh_points"]]
        v_points = [pt["v_point"] for pt in roi_dvh["dvh_curve"]["dvh_points"]]

        D2 = roi_dvh["D2"]["value"]
        D50 = roi_dvh["D50"]["value"]
        D95 = roi_dvh["D95"]["value"]
        D98 = roi_dvh["D98"]["value"]
        min_dose = roi_dvh["min"]["value"]
        mean_dose = roi_dvh["mean"]["value"]
        max_dose = roi_dvh["max"]["value"]

        V0 = roi_dvh.get("V0", {}).get("value")
        V15 = roi_dvh.get("V15", {}).get("value")
        V35 = roi_dvh.get("V35", {}).get("value")

        return {
            "roi_name": roi_name,
            "json_id": json_id,
            "d_points": d_points,
            "v_points": v_points,
            "D2": D2,
            "D50": D50,
            "D95": D95,
            "D98": D98,
            "min_dose": min_dose,
            "mean_dose": mean_dose,
            "max_dose": max_dose,
            "V0": V0,
            "V15": V15,
            "V35": V35,
        }

    def run(self, output, dicom_bundle):
        sop_uid = self.SOP_UID_rtose(dicom_bundle)

        pg = PostgresInterface(
            self.postgres_config["host"],
            self.postgres_config["db"],
            self.postgres_config["username"],
            self.postgres_config["password"],
            self.postgres_config["port"],
        )

        pg.connect()

        inserted_rois = set()

        for roi_dvh in output:
            roi_data = self.extract_roi_dvh(roi_dvh)

            # If the roi_key has been used before it skips so there will be no double entries
            roi_key = (roi_data["json_id"], sop_uid)
            if roi_key in inserted_rois:
                continue
            inserted_rois.add(roi_key)

            # Convert DVH arrays to Python floats
            d_points = [float(x) for x in roi_data["d_points"]]
            v_points = [float(x) for x in roi_data["v_points"]]

            # Convert metrics to Python float (or None)
            D2 = float(roi_data["D2"]) if roi_data["D2"] is not None else None
            D50 = float(roi_data["D50"]) if roi_data["D50"] is not None else None
            D95 = float(roi_data["D95"]) if roi_data["D95"] is not None else None
            D98 = float(roi_data["D98"]) if roi_data["D98"] is not None else None
            min_dose = float(roi_data["min_dose"]) if roi_data["min_dose"] is not None else None
            mean_dose = float(roi_data["mean_dose"]) if roi_data["mean_dose"] is not None else None
            max_dose = float(roi_data["max_dose"]) if roi_data["max_dose"] is not None else None
            V0 = float(roi_data["V0"]) if roi_data["V0"] is not None else None
            V15 = float(roi_data["V15"]) if roi_data["V15"] is not None else None
            V35 = float(roi_data["V35"]) if roi_data["V35"] is not None else None

            # Insert into dvh_result
            pg.cursor.execute(
                """
                INSERT INTO dvh_result (
                    json_id, dose_bins, volume_bins,
                    D2, D50, D95, D98,
                    min_dose, mean_dose, max_dose,
                    V0, V15, V35
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING result_id
                """,
                (
                    roi_data["json_id"],
                    d_points,
                    v_points,
                    D2,
                    D50,
                    D95,
                    D98,
                    min_dose,
                    mean_dose,
                    max_dose,
                    V0,
                    V15,
                    V35,
                ),
            )
            result_id = pg.cursor.fetchone()[0]

            # Insert into dvh_package
            pg.insert(
                "dvh_package", {"sop_instance_uid": sop_uid, "roi_name": roi_data["roi_name"], "result_id": result_id}
            )

        pg.disconnect()


# This script is run everytime docker compose up to create the correct tables
# if __name__ == "__main__":
#    file_data = read_config()
#    postgres_config = file_data.get("postgres", {})
#
#    pg = PostgresInterface(postgres_config["host"], postgres_config["db"], postgres_config["username"],
#                           postgres_config["password"], postgres_config["port"])
#    pg.connect()
#    create_dvh_tables(pg)
#    pg.disconnect()
