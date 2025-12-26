from pyspark.sql import SparkSession
import sys

print("🚀 Iniciando Test de Conectividad GCS...")

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Test GCS") \
    .getOrCreate()

try:
    path = "gs://salvando-patitas-spark-raw/raw_donaciones"
    print(f"🔍 Intentando leer: {path}")
    
    # Intenta leer solo el esquema primero (más rápido)
    df = spark.read.parquet(path)
    
    print("✅ Esquema detectado:")
    df.printSchema()
    
    # Intenta una acción real (count)
    print("🔢 Contando registros...")
    count = df.count()
    print(f"✅ Éxito Total! Count: {count}")
    
except Exception as e:
    print(f"❌ Error Fatal: {e}")

spark.stop()
