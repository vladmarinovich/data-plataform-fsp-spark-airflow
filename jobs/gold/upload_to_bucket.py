# jobs/gold/upload_to_bucket.py
"""
Job Gold: Exportar todas las tablas Gold a un bucket externo (S3/MinIO) y crear tablas externas
particionadas por año (y), mes (m) y día (d) al estilo Hive.

Requisitos:
- Cada tabla Gold debe contener una columna `anio_mes` con formato "yyyy-MM".
- Si la tabla tiene una columna de día real, reemplaza el valor fijo `1` por esa columna.
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import config
from jobs.utils.spark_session import get_spark_session
from jobs.utils.file_utils import rename_spark_output


def _write_to_bucket(df, bucket_path, table_name):
    """Escribe el DataFrame en el bucket usando particionado Hive (y,m,d)."""
    out_path = f"{bucket_path}/{table_name}"
    (
        df.write
        .mode("overwrite")
        .partitionBy("y", "m", "d")
        .parquet(out_path)
    )
    return out_path


def _create_external_table(spark: SparkSession, table_name: str, location: str):
    """Crea (o reemplaza) una tabla externa Spark‑SQL particionada por y,m,d.
    Asume que los archivos en `location` fueron escritos con `partitionBy('y','m','d')`.
    """
    spark.sql(f"""
        DROP TABLE IF EXISTS gold.{table_name}
    """)
    spark.sql(f"""
        CREATE EXTERNAL TABLE gold.{table_name}
        PARTITIONED BY (y INT, m INT, d INT)
        STORED AS PARQUET
        LOCATION '{location}'
    """)
    # Hace que Spark reconozca las particiones existentes
    spark.sql(f"""
        MSCK REPAIR TABLE gold.{table_name}
    """)


def run_upload_to_bucket():
    spark = get_spark_session("GoldUploadToBucket")
    try:
        print("🚀 JOB GOLD: UPLOAD TO BUCKET (particionado y/m/d)")

        gold_root = Path(config.GOLD_PATH)
        subfolders = [p for p in gold_root.iterdir() if p.is_dir()]

        for folder in subfolders:
            table_name = folder.name
            print(f"📂 Procesando {table_name}")

            # Lectura del parquet local
            df = spark.read.parquet(str(folder))

            # Añadir columnas de partición a partir de anio_mes (formato yyyy-MM)
            # Si la tabla ya tiene una columna de día, reemplaza el lit(1) por esa columna.
            df = (
                df.withColumn("y", F.year(F.to_date(F.col("anio_mes"), "yyyy-MM")))
                .withColumn("m", F.month(F.to_date(F.col("anio_mes"), "yyyy-MM")))
                .withColumn("d", F.lit(1))
            )

            # Escritura en el bucket con particionado Hive‑style
            bucket_location = _write_to_bucket(df, config.BUCKET_PATH, table_name)
            print(f"   → Escrito en bucket (particionado): {bucket_location}")

            # Registro de tabla externa particionada
            _create_external_table(spark, table_name, bucket_location)
            print(f"   → Tabla externa particionada creada: gold.{table_name}")

        print("✅ Upload completo y tablas externas registradas.")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_upload_to_bucket()
