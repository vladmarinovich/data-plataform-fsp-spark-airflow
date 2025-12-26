"""
Script para subir archivos Parquet a GCS con partición Hive.

Estrategia:
- Tablas con fechas (donaciones, gastos, casos): Particionadas por anio/mes
- Tablas snapshot (donantes, proveedores): Sin partición
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from google.cloud import storage
import config

# ============================================
# CONFIGURACIÓN
# ============================================

PROJECT_ID = "salvando-patitas-de-spark"
BUCKET_NAME = "salvando-patitas-spark-raw"

# Tablas con partición Hive (CDC pattern: particionado por fecha de modificación)
PARTITIONED_TABLES = {
    "donaciones": "last_modified_at",
    "gastos": "last_modified_at",
    "casos": "last_modified_at",
    "donantes": "last_modified_at",
}

# Tablas sin partición (snapshot)
SNAPSHOT_TABLES = ["proveedores", "hogar_de_paso"]


# ============================================
# FUNCIONES DE UPLOAD
# ============================================

def upload_partitioned_table(
    storage_client: storage.Client,
    bucket: storage.Bucket,
    table_name: str,
    date_column: str
) -> None:
    """
    Sube tabla particionada por anio/mes a GCS.
    
    Args:
        storage_client: Cliente de GCS
        bucket: Bucket de destino
        table_name: Nombre de la tabla
        date_column: Columna de fecha para particionar
    """
    print(f"\n{'='*70}")
    print(f"📤 Subiendo tabla particionada: {table_name}")
    print(f"{'='*70}")
    
    # Leer Parquet local
    local_path = config.DATA_RAW_DIR / f"{table_name}.parquet"
    
    if not local_path.exists():
        print(f"   ⚠️  Archivo no existe: {local_path}")
        return
    
    df = pd.read_parquet(local_path)
    print(f"   📊 Registros totales: {len(df)}")
    
    # ---------------------------------------------------------
    # CORRECCIÓN DE TIPOS: Forzar timestamps a datetime real
    # ---------------------------------------------------------
    TIMESTAMP_COLS = {
        "created_at", "last_modified_at", "fecha_donacion", 
        "fecha_pago", "fecha_ingreso", "fecha_salida", "ultimo_contacto"
    }
    
    for col in df.columns:
        if col in TIMESTAMP_COLS:
            # Convertir a datetime (evita errores BYTE_ARRAY en BigQuery)
            # IMPORTANTE: 
            # 1. Convertir a UTC (timezone aware)
            # 2. Quitar timezone (timezone naive) manteniendo la hora UTC -> dt.tz_localize(None)
            # 3. Forzar precisión 'us' -> .astype('datetime64[us]')
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.tz_localize(None).astype('datetime64[us]')
            
    # ---------------------------------------------------------
    # CORRECCIÓN DE IDS: Forzar enteros nullable (Int64)
    # Evita que Pandas convierta IDs con nulos a Float (DOUBLE)
    # ---------------------------------------------------------
    for col in df.columns:
        if col.startswith("id_"):
            print(f"   🔧 Casting ID a Int64: {col}")
            df[col] = df[col].astype("Int64")
    
    # Convertir columna de fecha a datetime
    df[date_column] = pd.to_datetime(df[date_column], utc=True)
    
    # Crear columnas de partición
    df['anio'] = df[date_column].dt.year.astype("Int64")
    df['mes'] = df[date_column].dt.month.astype("Int64").astype(str).str.zfill(2)
    df['dia'] = df[date_column].dt.day.astype("Int64").astype(str).str.zfill(2)
    
    # Agrupar por partición
    partitions = df.groupby(['anio', 'mes', 'dia'])
    
    print(f"   📁 Particiones encontradas: {len(partitions)}")
    
    files_uploaded = 0
    
    for (anio, mes, dia), group in partitions:
        # Ignorar particiones nulas si las hubiera
        if pd.isna(anio) or pd.isna(mes) or pd.isna(dia):
            continue
            
        # Nombre de archivo: raw_{table}/anio=YYYY/mes=MM/dia=DD/{table}-{partition_idx}.parquet
        blob_name = f"raw_{table_name}/anio={anio}/mes={mes}/dia={dia}/{table_name}-001.parquet"
        
        # Eliminar columnas de partición del Parquet físico (Hive style lo infiere del path)
        # O podemos dejarlas si preferimos redundancia. Hive standard es quitarlas.
        # Pero BigQuery External Tables prefiere que NO estén en el archivo si usas hive_partitioning_options
        group_to_write = group.drop(columns=['anio', 'mes', 'dia'])
        
        # Convertir a Parquet en memoria
        parquet_buffer = group_to_write.to_parquet(
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        
        # Subir a GCS
        blob = bucket.blob(blob_name)
        blob.upload_from_string(parquet_buffer, content_type='application/octet-stream')
        
        files_uploaded += 1
        print(f"   ✅ {blob_name} ({len(group_to_write)} registros)")
    
    print(f"\n   💾 Total archivos subidos: {files_uploaded}")


def upload_snapshot_table(
    storage_client: storage.Client,
    bucket: storage.Bucket,
    table_name: str
) -> None:
    """
    Sube tabla snapshot (sin partición) a GCS.
    
    Args:
        storage_client: Cliente de GCS
        bucket: Bucket de destino
        table_name: Nombre de la tabla
    """
    print(f"\n{'='*70}")
    print(f"📤 Subiendo tabla snapshot: {table_name}")
    print(f"{'='*70}")
    
    # Leer Parquet local
    local_path = config.DATA_RAW_DIR / f"{table_name}.parquet"
    
    if not local_path.exists():
        print(f"   ⚠️  Archivo no existe: {local_path}")
        return
    
    df = pd.read_parquet(local_path)
    print(f"   📊 Registros totales: {len(df)}")
    
    # ---------------------------------------------------------
    # NORMALIZACIÓN DE TIPOS DE DATOS
    # Objetivo: Garantizar compatibilidad con BigQuery y evitar conflictos de esquema
    # ---------------------------------------------------------

    # 1. IDs a Int64 (Nullable Integer)
    # Evita que Pandas infiera IDs con nulos como Float (DOUBLE en BQ), lo cual rompe JOINs
    for col in df.columns:
        if col.startswith("id_"):
            print(f"   🔧 Normalizando ID a Int64: {col}")
            df[col] = df[col].astype("Int64")

    # 2. Fechas a Timestamp Microseconds (UTC)
    # BigQuery requiere timestamps con precisión de microsegundos.
    # Pandas por defecto usa nanosegundos (ns), lo que causa error: "Invalid timestamp nanoseconds value".
    TIMESTAMP_COLS = [
        'created_at', 'last_modified_at', 
        'fecha_ingreso', 'fecha_salida', 
        'fecha_donacion', 'fecha_pago', 
        'ultimo_contacto'
    ]
    
    for col in df.columns:
        if col in TIMESTAMP_COLS:
             # Estrategia: 
             # 1. to_datetime(utc=True) -> Asegura zona horaria UTC
             # 2. tz_localize(None) -> Elimina info de zona horaria (naive) manteniendo la hora UTC
             # 3. astype('datetime64[us]') -> Fuerza precisión a microsegundos para compatibilidad con Parquet/BQ
             df[col] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.tz_localize(None).astype('datetime64[us]')

    # ---------------------------------------------------------
    # UPLOAD A GCS
    # ---------------------------------------------------------

    # Nombre del objeto en GCS: raw_<table>/<table>-001.parquet
    blob_name = f"raw_{table_name}/{table_name}-001.parquet"

    # Convertir a Parquet en memoria
    parquet_buffer = df.to_parquet(
        engine='pyarrow',
        compression='snappy',
        index=False
    )
    
    # Subir a GCS
    blob = bucket.blob(blob_name)
    blob.upload_from_string(parquet_buffer, content_type='application/octet-stream')
    
    print(f"   ✅ {blob_name} ({len(df)} registros)")


# ============================================
# PIPELINE PRINCIPAL
# ============================================

def main():
    """Función principal del script de upload."""
    print("="*70)
    print("🚀 SUBIENDO DATOS A GCS CON PARTICIÓN HIVE")
    print("="*70)
    
    # Inicializar cliente de GCS
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    
    print(f"\n📦 Bucket: gs://{BUCKET_NAME}")
    
    # 1. Subir tablas particionadas
    print(f"\n{'='*70}")
    print("📊 TABLAS PARTICIONADAS (anio/mes)")
    print(f"{'='*70}")
    
    for table_name, date_column in PARTITIONED_TABLES.items():
        upload_partitioned_table(storage_client, bucket, table_name, date_column)
    
    # 2. Subir tablas snapshot
    print(f"\n{'='*70}")
    print("📊 TABLAS SNAPSHOT (sin partición)")
    print(f"{'='*70}")
    
    for table_name in SNAPSHOT_TABLES:
        upload_snapshot_table(storage_client, bucket, table_name)
    
    # 3. Verificar archivos en bucket
    print(f"\n{'='*70}")
    print("📋 VERIFICACIÓN - Archivos en bucket")
    print(f"{'='*70}")
    
    blobs = list(bucket.list_blobs())
    
    if blobs:
        # Agrupar por tabla
        tables = {}
        for blob in blobs:
            table = blob.name.split('/')[0]
            if table not in tables:
                tables[table] = []
            tables[table].append(blob)
        
        for table, table_blobs in sorted(tables.items()):
            print(f"\n   📁 {table}/ ({len(table_blobs)} archivos)")
            for blob in sorted(table_blobs, key=lambda x: x.name)[:5]:  # Mostrar primeros 5
                size_mb = blob.size / (1024 * 1024)
                print(f"      - {blob.name.split('/', 1)[1]} ({size_mb:.2f} MB)")
            if len(table_blobs) > 5:
                print(f"      ... y {len(table_blobs) - 5} archivos más")
    else:
        print("   ⚠️  No hay archivos en el bucket")
    
    print(f"\n{'='*70}")
    print("✅ UPLOAD COMPLETADO")
    print(f"{'='*70}")
    
    print(f"\n💡 Próximo paso:")
    print(f"   Crear external tables en BigQuery apuntando a:")
    print(f"   gs://{BUCKET_NAME}/raw_*/")


if __name__ == "__main__":
    main()
