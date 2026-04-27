"""Postgres schema setup and result persistence for radiomics feature data."""

import csv
import io
import logging
import uuid

logger = logging.getLogger(__name__)


class setup_radiomics_db:
    """Create the radiomics_manager and radiomics_results tables if they do not exist."""

    def create_radiomics_manager(self, db):
        """Create the radiomics_manager table that links extraction runs to SOP UIDs."""
        columns = {
            "radiomics_id": "UUID PRIMARY KEY",
            "sop_instance_uid": "TEXT NOT NULL",
            "created_at": "TIMESTAMPTZ NOT NULL DEFAULT NOW()",
        }
        db.create_table("radiomics_manager", columns)

    def create_results_table(self, db):
        """Create the standard radiomics results table.

        Structure:
          - Metadata: identifiers, timestamps, settings tracking
          - Diagnostics: key PyRadiomics diagnostics stored as TEXT (variable formats from PyRadiomics)
          - Features: 115 float columns matching your current settings.yaml
              - Shape (16): original_shape_*
              - First Order (18): original_firstorder_*
              - GLCM (22): original_glcm_*
              - GLRLM (16): original_glrlm_*
              - GLSZM (16): original_glszm_*
              - GLDM (14): original_gldm_*
              - NGTDM (5): original_ngdtm

        If you add image filters (wavelet, LoG, etc.) to settings.yaml, use
        add_filter_columns() to extend the table without recreating it.
        """
        columns = {
            # ----------------------------------------------------------------
            # Metadata
            # ----------------------------------------------------------------
            "id": "SERIAL PRIMARY KEY",
            "radiomics_id": "UUID NOT NULL REFERENCES radiomics_manager(radiomics_id)",
            "roi_name": "TEXT NOT NULL",  # normalized ROI name
            # ----------------------------------------------------------------
            # Diagnostics
            # ----------------------------------------------------------------
            "diag_pyradiomics_version": "TEXT",
            "diag_image_hash": "TEXT",
            "diag_image_dimensionality": "TEXT",
            "diag_image_spacing": "TEXT",
            "diag_image_size": "TEXT",
            "diag_image_mean": "FLOAT",
            "diag_image_minimum": "FLOAT",
            "diag_image_maximum": "FLOAT",
            "diag_mask_hash": "TEXT",
            "diag_mask_spacing": "TEXT",
            "diag_mask_size": "TEXT",
            "diag_mask_bounding_box": "TEXT",
            "diag_mask_voxel_num": "INTEGER",
            "diag_mask_volume_num": "INTEGER",
            "diag_mask_center_of_mass": "TEXT",
            # ----------------------------------------------------------------
            # Shape features (17)
            # ----------------------------------------------------------------
            "original_shape_Elongation": "FLOAT",
            "original_shape_Flatness": "FLOAT",
            "original_shape_LeastAxisLength": "FLOAT",
            "original_shape_MajorAxisLength": "FLOAT",
            "original_shape_Maximum2DDiameterColumn": "FLOAT",
            "original_shape_Maximum2DDiameterRow": "FLOAT",
            "original_shape_Maximum2DDiameterSlice": "FLOAT",
            "original_shape_Maximum3DDiameter": "FLOAT",
            "original_shape_MeshVolume": "FLOAT",
            "original_shape_MinorAxisLength": "FLOAT",
            "original_shape_Sphericity": "FLOAT",
            "original_shape_SurfaceArea": "FLOAT",
            "original_shape_SurfaceVolumeRatio": "FLOAT",
            "original_shape_VoxelVolume": "FLOAT",
            # ----------------------------------------------------------------
            # First Order features (19)
            # ----------------------------------------------------------------
            "original_firstorder_10Percentile": "FLOAT",
            "original_firstorder_90Percentile": "FLOAT",
            "original_firstorder_Energy": "FLOAT",
            "original_firstorder_Entropy": "FLOAT",
            "original_firstorder_InterquartileRange": "FLOAT",
            "original_firstorder_Kurtosis": "FLOAT",
            "original_firstorder_Maximum": "FLOAT",
            "original_firstorder_Mean": "FLOAT",
            "original_firstorder_MeanAbsoluteDeviation": "FLOAT",
            "original_firstorder_Median": "FLOAT",
            "original_firstorder_Minimum": "FLOAT",
            "original_firstorder_Range": "FLOAT",
            "original_firstorder_RobustMeanAbsoluteDeviation": "FLOAT",
            "original_firstorder_RootMeanSquared": "FLOAT",
            "original_firstorder_Skewness": "FLOAT",
            "original_firstorder_TotalEnergy": "FLOAT",
            "original_firstorder_Uniformity": "FLOAT",
            "original_firstorder_Variance": "FLOAT",
            # ----------------------------------------------------------------
            # GLCM features (26)
            # ----------------------------------------------------------------
            "original_glcm_Autocorrelation": "FLOAT",
            "original_glcm_ClusterProminence": "FLOAT",
            "original_glcm_ClusterShade": "FLOAT",
            "original_glcm_ClusterTendency": "FLOAT",
            "original_glcm_Contrast": "FLOAT",
            "original_glcm_Correlation": "FLOAT",
            "original_glcm_DifferenceAverage": "FLOAT",
            "original_glcm_DifferenceEntropy": "FLOAT",
            "original_glcm_DifferenceVariance": "FLOAT",
            "original_glcm_JointEnergy": "FLOAT",
            "original_glcm_JointEntropy": "FLOAT",
            "original_glcm_JointAverage": "FLOAT",
            "original_glcm_Imc1": "FLOAT",
            "original_glcm_Imc2": "FLOAT",
            "original_glcm_Id": "FLOAT",
            "original_glcm_Idn": "FLOAT",
            "original_glcm_Idm": "FLOAT",
            "original_glcm_Idmn": "FLOAT",
            "original_glcm_InverseVariance": "FLOAT",
            "original_glcm_MaximumProbability": "FLOAT",
            "original_glcm_SumAverage": "FLOAT",
            "original_glcm_SumEntropy": "FLOAT",
            "original_glcm_SumSquares": "FLOAT",
            "original_glcm_MCC": "FLOAT",
            "original_glcm_Homogeneity": "FLOAT",
            # ----------------------------------------------------------------
            # GLRLM features (16)
            # ----------------------------------------------------------------
            "original_glrlm_ShortRunEmphasis": "FLOAT",
            "original_glrlm_LongRunEmphasis": "FLOAT",
            "original_glrlm_GrayLevelNonUniformity": "FLOAT",
            "original_glrlm_RunLengthNonUniformity": "FLOAT",
            "original_glrlm_RunPercentage": "FLOAT",
            "original_glrlm_LowGrayLevelRunEmphasis": "FLOAT",
            "original_glrlm_HighGrayLevelRunEmphasis": "FLOAT",
            "original_glrlm_ShortRunLowGrayLevelEmphasis": "FLOAT",
            "original_glrlm_ShortRunHighGrayLevelEmphasis": "FLOAT",
            "original_glrlm_LongRunLowGrayLevelEmphasis": "FLOAT",
            "original_glrlm_LongRunHighGrayLevelEmphasis": "FLOAT",
            "original_glrlm_RunLengthNonUniformityNormalized": "FLOAT",
            "original_glrlm_GrayLevelNonUniformityNormalized": "FLOAT",
            "original_glrlm_RunVariance": "FLOAT",
            "original_glrlm_GrayLevelVariance": "FLOAT",
            "original_glrlm_RunEntropy": "FLOAT",
            # ----------------------------------------------------------------
            # GLSZM features (16)
            # ----------------------------------------------------------------
            "original_glszm_SmallAreaEmphasis": "FLOAT",
            "original_glszm_LargeAreaEmphasis": "FLOAT",
            "original_glszm_GrayLevelNonUniformity": "FLOAT",
            "original_glszm_GrayLevelNonUniformityNormalized": "FLOAT",
            "original_glszm_SizeZoneNonUniformity": "FLOAT",
            "original_glszm_SizeZoneNonUniformityNormalized": "FLOAT",
            "original_glszm_ZonePercentage": "FLOAT",
            "original_glszm_GrayLevelVariance": "FLOAT",
            "original_glszm_ZoneVariance": "FLOAT",
            "original_glszm_SmallAreaLowGrayLevelEmphasis": "FLOAT",
            "original_glszm_SmallAreaHighGrayLevelEmphasis": "FLOAT",
            "original_glszm_LargeAreaLowGrayLevelEmphasis": "FLOAT",
            "original_glszm_LargeAreaHighGrayLevelEmphasis": "FLOAT",
            "original_glszm_ZoneEntropy": "FLOAT",
            "original_glszm_lowgraylevelzoneemphasis": "FLOAT",
            "original_glszm_highgraylevelzoneemphasis": "FLOAT",
            # ----------------------------------------------------------------
            # GLDM features (14)
            # ----------------------------------------------------------------
            "original_gldm_SmallDependenceEmphasis": "FLOAT",
            "original_gldm_LargeDependenceEmphasis": "FLOAT",
            "original_gldm_GrayLevelNonUniformity": "FLOAT",
            "original_gldm_GrayLevelNonUniformityNormalized": "FLOAT",
            "original_gldm_DependenceNonUniformity": "FLOAT",
            "original_gldm_DependenceNonUniformityNormalized": "FLOAT",
            "original_gldm_DependenceVariance": "FLOAT",
            "original_gldm_DependenceEntropy": "FLOAT",
            "original_gldm_LowGrayLevelEmphasis": "FLOAT",
            "original_gldm_HighGrayLevelEmphasis": "FLOAT",
            "original_gldm_SmallDependenceHighGrayLevelEmphasis": "FLOAT",
            "original_gldm_SmallDependenceLowGrayLevelEmphasis": "FLOAT",
            "original_gldm_LargeDependenceHighGrayLevelEmphasis": "FLOAT",
            "original_gldm_LargeDependenceLowGrayLevelEmphasis": "FLOAT",
            "original_gldm_GrayLevelVariance": "FLOAT",
            # ----------------------------------------------------------------
            # NGTDM features (5)
            # ----------------------------------------------------------------
            "original_ngtdm_Coarseness": "FLOAT",
            "original_ngtdm_Contrast": "FLOAT",
            "original_ngtdm_Busyness": "FLOAT",
            "original_ngtdm_Complexity": "FLOAT",
            "original_ngtdm_Strength": "FLOAT",
        }

        db.create_table("radiomics_results", columns)
        logger.info("radiomics_results table created with %d columns", len(columns))

    def run(self, db):
        """Run all table-creation steps."""
        self.create_radiomics_manager(db)
        self.create_results_table(db)


_DIAG_CSV_TO_DB = {
    "diagnostics_Versions_PyRadiomics": "diag_pyradiomics_version",
    "diagnostics_Image-original_Hash": "diag_image_hash",
    "diagnostics_Image-original_Dimensionality": "diag_image_dimensionality",
    "diagnostics_Image-original_Spacing": "diag_image_spacing",
    "diagnostics_Image-original_Size": "diag_image_size",
    "diagnostics_Image-original_Mean": "diag_image_mean",
    "diagnostics_Image-original_Minimum": "diag_image_minimum",
    "diagnostics_Image-original_Maximum": "diag_image_maximum",
    "diagnostics_Mask-original_Hash": "diag_mask_hash",
    "diagnostics_Mask-original_Spacing": "diag_mask_spacing",
    "diagnostics_Mask-original_Size": "diag_mask_size",
    "diagnostics_Mask-original_BoundingBox": "diag_mask_bounding_box",
    "diagnostics_Mask-original_VoxelNum": "diag_mask_voxel_num",
    "diagnostics_Mask-original_VolumeNum": "diag_mask_volume_num",
    "diagnostics_Mask-original_CenterOfMass": "diag_mask_center_of_mass",
}


def send_postgress(db, csv_content, metadata):
    """Parse a PyRadiomics CSV result and insert rows into the database."""

    def clean(value):
        """This nested function is a small helper that return None when values are missing"""
        if value is None or value == "":
            return None
        return value

    # Insert in the radiomics manager
    radiomics_id = str(uuid.uuid4())  # generates a unique ID as a string
    manager_data = {}

    manager_data["radiomics_id"] = radiomics_id
    manager_data["sop_instance_uid"] = metadata["sop_instance_uid"]

    try:
        db.insert("radiomics_manager", manager_data)
        logger.info("Inserted radiomics_manager record for radiomics_id=%s.", radiomics_id)
    except Exception as e:
        logger.error(
            "Failed to insert radiomics_manager record (radiomics_id=%s): %s",
            radiomics_id,
            e,
        )
        raise

    # Parse and insert each CSV row
    try:
        reader = csv.DictReader(io.StringIO(csv_content))
    except Exception as e:
        logger.error("Failed to parse CSV content: %s", e)
        raise ValueError(f"Could not parse csv_content as CSV: {e}") from e

    for row in reader:
        results_data = {}

        # link to manager
        results_data["radiomics_id"] = radiomics_id

        # ROI name
        results_data["roi_name"] = clean(row.pop("id", "unknown_roi"))

        # diagnostics
        for csv_key, db_col in _DIAG_CSV_TO_DB.items():
            results_data[db_col] = clean(row.get(csv_key))

        # radiomics features
        for key, value in row.items():
            if key.startswith("original_"):
                results_data[key] = clean(value)

        db.insert("radiomics_results", results_data)
