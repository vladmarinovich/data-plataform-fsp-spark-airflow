#!/usr/bin/env python3
"""
Script para limpiar bucket GCS antes de re-extracción.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from google.cloud import storage
from config.credentials import get_gcs_client

def clean_bucket():
    """Borra todo el contenido del bucket raw/silver/gold"""
    client = get_gcs_client()
    bucket_name = "salvando-patitas-spark"
    
    print(f"🗑️  Limpiando bucket: {bucket_name}")
    
    prefixes = ["lake/raw/", "lake/silver/", "lake/gold/"]
    
    for prefix in prefixes:
        print(f"\n📂 Borrando: gs://{bucket_name}/{prefix}")
        blobs = client.list_blobs(bucket_name, prefix=prefix)
        
        count = 0
        for blob in blobs:
            blob.delete()
            count += 1
            if count % 100 == 0:
                print(f"   Borrados: {count} archivos...")
        
        print(f"   ✅ Total borrados: {count} archivos")
    
    print("\n✅ Bucket limpio!")

if __name__ == "__main__":
    clean_bucket()
