# Existing config variables (example)
GOLD_PATH = "data/lake/gold"
SILVER_PATH = "data/lake/silver"
# New bucket path for external storage (local MinIO or GCS)
# Adjust to your actual bucket URL, e.g., "s3://my-bucket/spdp"
BUCKET_PATH = "s3://spdp-data-bucket"
# BigQuery settings
RAW_BQ_DATASET = "spdp_raw"
BQ_DATASET = "spdp_gold"
BIGQUERY_NATIVE_TABLES = [
    "dashboard_donaciones",
    "dashboard_gastos",
    "dashboard_financiero",
    "feat_casos",
    "feat_donantes",
    "feat_proveedores",
]
# ---- Test mode ----
# Set to "YYYY-MM" to process only that month (e.g., "2023-01").
# Set to None to process all data.
TEST_MONTH = "2023-01"
