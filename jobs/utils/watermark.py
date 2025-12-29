'''Utility for handling the GLOBAL pipeline watermark stored in GCS.
Uses Python SDK to read/write state to gs://salvando-patitas-spark/state/watermarks.json
'''

from pyspark.sql import SparkSession
import json
import tempfile
import os
from datetime import datetime
from pathlib import Path
import config
from config.credentials import get_gcs_client

# Global Watermark Key
PIPELINE_NAME = "spdp_data_platform_main"

# GCS Path
BUCKET_NAME = "salvando-patitas-spark"
STATE_BLOB_PATH = "state/watermarks.json"


def _read_from_gcs():
    """Helper to read JSON from GCS using Python SDK."""
    try:
        client = get_gcs_client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(STATE_BLOB_PATH)
        
        if not blob.exists():
            print(f"⚠️ Watermark file not found in GCS (gs://{BUCKET_NAME}/{STATE_BLOB_PATH}). Assuming clean slate.")
            return {}
        
        content = blob.download_as_text()
        return json.loads(content)
    except json.JSONDecodeError:
        print(f"⚠️ Corrupt watermark file in GCS.")
        return {}
    except Exception as e:
        print(f"⚠️ Error reading watermark from GCS: {e}")
        return {}

def _write_to_gcs(data):
    """Helper to write JSON to GCS using Python SDK."""
    try:
        client = get_gcs_client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(STATE_BLOB_PATH)
        
        # Upload as JSON string
        blob.upload_from_string(
            json.dumps(data, indent=2),
            content_type='application/json'
        )
        print(f"☁️  Estado sincronizado a GCS: gs://{BUCKET_NAME}/{STATE_BLOB_PATH}")
    except Exception as e:
        print(f"❌ Error escribiendo watermark a GCS: {e}")
        raise


def get_watermark(spark: SparkSession, pipeline_name: str = PIPELINE_NAME) -> "datetime | None":
    """Read the GLOBAL watermark from GCS.
    Ignores pipeline_name arg to enforce 'One Global Watermark' policy if needed, 
    but we keep flexibility to look up keys if they exist in the global object.
    
    For this robust implementation: We look for the GLOBAL key always, 
    unless specific logic overrides.
    """
    # Always read fresh from Cloud
    data = _read_from_gcs()
    
    # Priority: 
    # 1. Look for global key PIPELINE_NAME (The "One Watermark")
    # 2. Or fallback to specific key if provided (compatibility)
    
    # Strict Global Strategy:
    watermark_str = data.get(PIPELINE_NAME, {}).get("watermark")

    if watermark_str:
        return datetime.fromisoformat(watermark_str.replace('Z', '+00:00'))
    
    return None


def update_watermark(spark: SparkSession, new_watermark, pipeline_name: str = PIPELINE_NAME):
    """Upsert the GLOBAL watermark to GCS.
    WARNING: This updates the shared state. 
    Ideally only called by the Orchestrator/Finalizer.
    """
    data = _read_from_gcs()
    
    if isinstance(new_watermark, datetime):
        watermark_str = new_watermark.isoformat()
    else:
        watermark_str = str(new_watermark)
    
    # Update Global Key
    data[PIPELINE_NAME] = {
        "watermark": watermark_str,
        "updated_at": datetime.now().isoformat()
    }
    
    _write_to_gcs(data)
    print(f"✅ GLOBAL Watermark actualizado (GCS): {watermark_str}")

