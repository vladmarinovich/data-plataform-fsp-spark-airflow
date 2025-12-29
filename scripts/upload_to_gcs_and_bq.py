#!/usr/bin/env python3
"""
Script para subir datos procesados a GCP:
1. Sube RAW, SILVER, GOLD (dims/facts) a GCS (source of truth)
2. Carga GOLD (feat_*, dashboard_*) como tablas nativas en BigQuery (consumo)

Arquitectura:
- GCS: Histórico completo (barato, Spark-friendly)
- BigQuery: Solo capas de consumo (rápido, queries complejos)

Usage:
    python scripts/upload_to_gcs_and_bq.py
"""

import sys
from pathlib import Path
import subprocess

# Agregar raíz del proyecto al path
sys.path.insert(0, str(Path(__file__).parent.parent))

from google.cloud import bigquery
from google.cloud import storage
import config

# Configuración
PROJECT_ID = "salvando-patitas-de-spark"
BQ_DATASET = "fsp_gold"

# Buckets GCS
BUCKET_RAW = "salvando-patitas-spark-raw"
BUCKET_SILVER = "salvando-patitas-spark-silver"
BUCKET_GOLD = "salvando-patitas-spark-gold"

# Tablas que van SOLO a BigQuery (no al bucket)
BQ_NATIVE_TABLES = [
    "feat_donantes",
    "feat_casos", 
    "feat_proveedores",
    "dashboard_donaciones",
    "dashboard_gastos",
    "dashboard_financiero",
]


def upload_to_gcs(local_path: str, bucket_name: str, blob_prefix: str):
    """Sube archivos locales a GCS manteniendo estructura de carpetas."""
    print(f"📤 Subiendo {local_path} → gs://{bucket_name}/{blob_prefix}/")
    
    path_obj = Path(local_path)
    
    # Determinar si es archivo o directorio
    if path_obj.is_file():
        # Subir archivo individual
        cmd = [
            "gsutil", "cp",
            str(local_path),
            f"gs://{bucket_name}/{blob_prefix}/"
        ]
    else:
        # Subir directorio recursivamente
        cmd = [
            "gsutil", "-m", "rsync", "-r",
            str(local_path),
            f"gs://{bucket_name}/{blob_prefix}/"
        ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"   ✅ Subido exitosamente")
        return True
    else:
        # Mostrar warning pero continuar (el bucket puede no existir aún)
        print(f"   ⚠️  Warning: {result.stderr}")
        print(f"   💡 Asegúrate de que el bucket gs://{bucket_name} existe")
        return False



def upload_to_bigquery(local_path: str, table_id: str):
    """Carga tabla local (Parquet) como tabla nativa en BigQuery."""
    print(f"📊 Cargando {local_path} → BigQuery: {table_id}")
    
    client = bigquery.Client(project=PROJECT_ID)
    
    # Configuración del job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite
        autodetect=True,  # Auto-detectar schema desde Parquet
    )
    
    # Encontrar archivos parquet en la carpeta
    parquet_files = list(Path(local_path).rglob("*.parquet"))
    
    if not parquet_files:
        print(f"   ⚠️  No se encontraron archivos Parquet en {local_path}")
        return
    
    print(f"   📦 Encontrados {len(parquet_files)} archivos Parquet")
    
    # Cargar cada archivo (BigQuery los mergea automáticamente)
    for file_path in parquet_files:
        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(
                source_file,
                f"{PROJECT_ID}.{BQ_DATASET}.{table_id}",
                job_config=job_config,
            )
        
        job.result()  # Esperar a que termine
    
    print(f"   ✅ Cargado en BigQuery: {BQ_DATASET}.{table_id}")


def ensure_bq_dataset_exists():
    """Crea el dataset de BigQuery si no existe."""
    client = bigquery.Client(project=PROJECT_ID)
    dataset_id = f"{PROJECT_ID}.{BQ_DATASET}"
    
    try:
        client.get_dataset(dataset_id)
        print(f"✅ Dataset ya existe: {BQ_DATASET}")
    except:
        print(f"📂 Creando dataset: {BQ_DATASET}")
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"   ✅ Dataset creado")


def main():
    """Pipeline principal de upload."""
    
    print("="*80)
    print("🚀 INICIANDO UPLOAD A GCP")
    print("="*80)
    
    # Verificar paths locales
    local_raw = Path(config.DATA_DIR) / "lake" / "raw"
    local_silver = Path(config.DATA_DIR) / "lake" / "silver"
    local_gold = Path(config.DATA_DIR) / "lake" / "gold"
    
    if not local_raw.exists():
        print("❌ No existe data/lake/raw/. Ejecuta el pipeline primero.")
        return
    
    # =========================================================================
    # PASO 1: SUBIR RAW A GCS
    # =========================================================================
    print("\n" + "="*80)
    print("📦 PASO 1: Subiendo RAW layer a GCS")
    print("="*80)
    
    for table_dir in local_raw.iterdir():
        if table_dir.is_dir() or table_dir.suffix == ".parquet":
            table_name = table_dir.stem.replace("raw_", "")
            upload_to_gcs(
                str(table_dir) if table_dir.is_dir() else str(table_dir),
                BUCKET_RAW,
                table_name
            )
    
    # =========================================================================
    # PASO 2: SUBIR SILVER A GCS
    # =========================================================================
    print("\n" + "="*80)
    print("🔧 PASO 2: Subiendo SILVER layer a GCS")
    print("="*80)
    
    for table_dir in local_silver.iterdir():
        if table_dir.is_dir():
            upload_to_gcs(str(table_dir), BUCKET_SILVER, table_dir.name)
    
    # =========================================================================
    # PASO 3: SUBIR GOLD (dims/facts) A GCS
    # =========================================================================
    print("\n" + "="*80)
    print("💎 PASO 3: Subiendo GOLD (dims/facts) a GCS")
    print("="*80)
    
    for table_dir in local_gold.iterdir():
        if not table_dir.is_dir():
            continue
        
        table_name = table_dir.name
        
        # Solo dims y facts van al bucket
        if table_name.startswith(("dim_", "fact_")):
            # Verificar si tiene particiones Hive (y=/m=/d=/)
            has_partitions = any(
                p.name.startswith(("y=", "year=", "anio=")) 
                for p in table_dir.iterdir() if p.is_dir()
            )
            
            if has_partitions:
                print(f"   📁 {table_name} (particionada)")
            else:
                print(f"   📄 {table_name} (no particionada)")
            
            upload_to_gcs(str(table_dir), BUCKET_GOLD, table_name)
    
    # =========================================================================
    # PASO 4: CARGAR GOLD (feat_*, dashboard_*) A BIGQUERY
    # =========================================================================
    print("\n" + "="*80)
    print("⚡ PASO 4: Cargando GOLD (consumo) a BigQuery")
    print("="*80)
    
    # Asegurar que el dataset existe
    ensure_bq_dataset_exists()
    
    for table_dir in local_gold.iterdir():
        if not table_dir.is_dir():
            continue
        
        table_name = table_dir.name
        
        # Solo feat_* y dashboard_* van a BigQuery
        if table_name in BQ_NATIVE_TABLES:
            upload_to_bigquery(str(table_dir), table_name)
    
    # =========================================================================
    # RESUMEN
    # =========================================================================
    print("\n" + "="*80)
    print("✅ UPLOAD COMPLETADO")
    print("="*80)
    print(f"""
📦 GCS Buckets:
   - gs://{BUCKET_RAW}/
   - gs://{BUCKET_SILVER}/
   - gs://{BUCKET_GOLD}/

⚡ BigQuery Dataset:
   - {PROJECT_ID}.{BQ_DATASET}
   - {len(BQ_NATIVE_TABLES)} tablas nativas (feat_*, dashboard_*)

🎯 Próximos pasos:
   1. Validar datos en BigQuery: bq ls {BQ_DATASET}
   2. Ejecutar queries de prueba
   3. Conectar dashboard de BI
""")


if __name__ == "__main__":
    main()
