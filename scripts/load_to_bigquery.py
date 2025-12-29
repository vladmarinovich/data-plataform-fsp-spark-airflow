import os
from google.cloud import bigquery

# Configuración
PROJECT_ID = "salvando-patitas-de-spark"
DATASET_ID = "gold"
BUCKET_NAME = "salvando-patitas-spark"

def load_tables():
    client = bigquery.Client(project=PROJECT_ID)
    
    # Asegurar que el dataset existe
    dataset_ref = client.dataset(DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
        print(f"✅ Dataset {DATASET_ID} verificado.")
    except Exception:
        print(f"🔨 Creando dataset {DATASET_ID}...")
        client.create_dataset(bigquery.Dataset(dataset_ref))

    tables = [
        "feat_casos", "feat_donantes", "feat_proveedores",
        "dashboard_donaciones", "dashboard_gastos", "dashboard_financiero"
    ]

    for table_name in tables:
        print(f"🚀 Cargando {table_name} desde GCS...")
        table_ref = dataset_ref.table(table_name)
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        
        uri = f"gs://{BUCKET_NAME}/lake/gold/{table_name}/*.parquet"
        
        load_job = client.load_table_from_uri(
            uri, table_ref, job_config=job_config
        )
        
        load_job.result()  # Esperar a que termine
        print(f"✅ Tabla {table_name} cargada exitosamente.")

if __name__ == "__main__":
    load_tables()
