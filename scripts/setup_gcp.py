"""
Script para configurar GCP: crear datasets de BigQuery y verificar bucket.
"""
from google.cloud import bigquery, storage

PROJECT_ID = "salvando-patitas-de-spark"
LOCATION = "us-central1"
BUCKET_NAME = "salvando-patitas-spark-raw"

print("="*70)
print("🚀 CONFIGURANDO GCP")
print("="*70)

# ============================================
# 1. CREAR DATASETS DE BIGQUERY
# ============================================
print("\n📊 Creando datasets de BigQuery...")

bq_client = bigquery.Client(project=PROJECT_ID)

datasets = ["raw", "silver", "gold"]

for dataset_name in datasets:
    dataset_id = f"{PROJECT_ID}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = LOCATION
    
    try:
        dataset = bq_client.create_dataset(dataset, exists_ok=True)
        print(f"   ✅ Dataset creado: {dataset_id}")
    except Exception as e:
        print(f"   ⚠️  Dataset {dataset_id}: {e}")

# ============================================
# 2. VERIFICAR BUCKET
# ============================================
print("\n🪣 Verificando bucket de GCS...")

storage_client = storage.Client(project=PROJECT_ID)

try:
    bucket = storage_client.get_bucket(BUCKET_NAME)
    print(f"   ✅ Bucket existe: gs://{BUCKET_NAME}")
    print(f"   📍 Location: {bucket.location}")
    print(f"   📦 Storage class: {bucket.storage_class}")
except Exception as e:
    print(f"   ❌ Error: {e}")

# ============================================
# 3. LISTAR DATASETS
# ============================================
print("\n📋 Datasets disponibles:")
datasets = list(bq_client.list_datasets())
if datasets:
    for dataset in datasets:
        print(f"   - {dataset.dataset_id}")
else:
    print("   ⚠️  No hay datasets")

print("\n" + "="*70)
print("✅ CONFIGURACIÓN COMPLETADA")
print("="*70)
print(f"\n💡 Bucket: gs://{BUCKET_NAME}")
print(f"💡 Datasets: {PROJECT_ID}:raw, {PROJECT_ID}:silver, {PROJECT_ID}:gold")
