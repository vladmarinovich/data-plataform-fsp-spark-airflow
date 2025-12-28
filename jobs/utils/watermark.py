'''Utility for handling a single pipeline watermark stored in a BigQuery control table.'''

from pyspark.sql import SparkSession
import config

CONTROL_TABLE = "spdp_control.pipeline_watermark"
PIPELINE_NAME = "spdp_data_platform_main"


def get_watermark(spark: SparkSession) -> "datetime | None":
    """Read the current watermark (max last_modified_at) for the pipeline.
    Returns None if no entry exists yet.
    """
    df = (
        spark.read.format("bigquery")
        .option("table", CONTROL_TABLE)
        .load()
        .filter(f"pipeline_name = '{PIPELINE_NAME}'")
        .select("watermark")
        .limit(1)
    )
    rows = df.collect()
    return rows[0]["watermark"] if rows else None


def update_watermark(spark: SparkSession, new_watermark):
    """Upsert the watermark with the latest timestamp.
    `new_watermark` must be a Python datetime or string compatible with TIMESTAMP.
    """
    # Use MERGE via Spark SQL (BigQuery connector supports DML)
    spark.sql(
        f"""
        MERGE `{CONTROL_TABLE}` T
        USING (SELECT '{PIPELINE_NAME}' AS pipeline_name, TIMESTAMP('{new_watermark}') AS watermark) S
        ON T.pipeline_name = S.pipeline_name
        WHEN MATCHED THEN UPDATE SET watermark = S.watermark, updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (pipeline_name, watermark) VALUES (S.pipeline_name, S.watermark)
        """
    )
