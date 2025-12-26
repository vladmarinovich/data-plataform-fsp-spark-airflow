
import os
import glob
from pathlib import Path
from google.cloud import storage
import config

def rename_spark_output(layer: str, table_name: str, output_path: str):
    """
    Renombra los archivos generados por Spark (part-*) al formato:
    {layer}_{table_name}_{index}.parquet
    
    Funciona tanto en LOCAL como en GCS.
    """
    print(f"🏷️  Renombrando archivos en {output_path}...")
    
    if config.ENV == "local" or not output_path.startswith("gs://"):
        _rename_local(layer, table_name, output_path)
    else:
        _rename_gcs(layer, table_name, output_path)

def _rename_local(layer: str, table_name: str, output_path: str):
    # Asegurar que la ruta sea absoluta para evitar problemas con glob
    abs_output_path = os.path.abspath(output_path)
    # Agrupamos archivos por su directorio
    files_by_dir = {}
    for root, dirs, filenames in os.walk(abs_output_path):
        for f in filenames:
            if f.startswith("part-") and f.endswith(".parquet"):
                if root not in files_by_dir:
                    files_by_dir[root] = []
                files_by_dir[root].append(os.path.join(root, f))
    
    total_renamed = 0
    for directory, files in files_by_dir.items():
        # Reiniciar contador para cada carpeta
        for i, full_path in enumerate(sorted(files), 1):
            new_name = f"{layer}_{table_name}_{str(i).zfill(3)}.parquet"
            new_full_path = os.path.join(directory, new_name)
            
            os.rename(full_path, new_full_path)
            total_renamed += 1
            
        # Limpieza de metadata en esta carpeta
        _cleanup_spark_metadata_local(directory)
        
    print(f"   ✅ {total_renamed} archivos renombrados localmente (reinicio de contador por carpeta).")

def _rename_gcs(layer: str, table_name: str, output_path: str):
    # Configuración de GCS
    # gs://bucket-name/folder/path -> bucket_name, prefix
    path_parts = output_path.replace("gs://", "").split("/", 1)
    bucket_name = path_parts[0]
    prefix = path_parts[1] if len(path_parts) > 1 else ""
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    blobs = list(bucket.list_blobs(prefix=prefix))
    part_files = [b for b in blobs if "part-" in b.name and b.name.endswith(".parquet")]
    
    for i, blob in enumerate(sorted(part_files, key=lambda x: x.name), 1):
        directory = os.path.dirname(blob.name)
        new_name = f"{layer}_{table_name}_{str(i).zfill(3)}.parquet"
        new_blob_name = os.path.join(directory, new_name)
        
        # En GCS el "rename" es un copy + delete
        bucket.copy_blob(blob, bucket, new_blob_name)
        blob.delete()
        
    # Limpiar archivos _SUCCESS
    success_files = [b for b in blobs if "_SUCCESS" in b.name]
    for b in success_files:
        b.delete()

    print(f"   ✅ {len(part_files)} archivos renombrados en GCS: gs://{bucket_name}/{prefix}")

def _cleanup_spark_metadata_local(directory: str):
    """Borra archivos temporales y de éxito de Spark en local."""
    for meta in ["_SUCCESS", ".part-*.crc", "_committed_*", "_started_*"]:
        for f in glob.glob(os.path.join(directory, meta)):
            try: os.remove(f)
            except: pass
