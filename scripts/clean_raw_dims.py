from google.cloud import storage

BUCKET_NAME = "salvando-patitas-spark"
FOLDERS_TO_CLEAN = [
    "lake/raw/casos",
    "lake/raw/donantes",
    "lake/raw/hogar_de_paso",
    "lake/raw/proveedores"
]

def clean_gcs_folders():
    print(f"🧹 Iniciando limpieza de dimensiones corruptas en {BUCKET_NAME}...")
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        
        for folder in FOLDERS_TO_CLEAN:
            print(f"   Searching in: {folder}/")
            blobs = list(bucket.list_blobs(prefix=f"{folder}/"))
            
            if not blobs:
                print(f"   ⚠️  Folder {folder} is already empty.")
                continue
                
            print(f"   🗑️  Deleting {len(blobs)} files in {folder}...")
            # Batch usage for speed implies iterating, simpler to just delete one by one or allow library to handle
            for blob in blobs:
                blob.delete()
            print(f"   ✅ Cleaned: {folder}")
            
        print("✨ Limpieza completada exitosamente.")
        
    except Exception as e:
        print(f"❌ Error durante la limpieza: {e}")

if __name__ == "__main__":
    clean_gcs_folders()
