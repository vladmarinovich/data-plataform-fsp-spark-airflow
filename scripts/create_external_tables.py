"""
Script para crear external tables en BigQuery apuntando a GCS.

Las external tables permiten consultar datos en GCS sin importarlos a BigQuery.
Se crean una sola vez y automáticamente detectan archivos nuevos.
"""
from google.cloud import bigquery

PROJECT_ID = "salvando-patitas-de-spark"
DATASET_ID = "raw"
BUCKET_NAME = "salvando-patitas-spark-raw"

print("="*70)
print("🔗 CREANDO EXTERNAL TABLES EN BIGQUERY")
print("="*70)

# Inicializar cliente
client = bigquery.Client(project=PROJECT_ID)

# ============================================
# DEFINICIÓN DE EXTERNAL TABLES
# ============================================

EXTERNAL_TABLES = {
    "raw_donaciones": {
        "uri": f"gs://{BUCKET_NAME}/raw_donaciones/*",
        "partitioned": True,
        "hive_partition_uri_prefix": f"gs://{BUCKET_NAME}/raw_donaciones",
        "schema": [
            bigquery.SchemaField("id_donacion", "INTEGER"),
            bigquery.SchemaField("id_donante", "INTEGER"),
            bigquery.SchemaField("id_caso", "INTEGER"),
            bigquery.SchemaField("fecha_donacion", "TIMESTAMP"),
            bigquery.SchemaField("monto", "FLOAT"),
            bigquery.SchemaField("medio_pago", "STRING"),
            bigquery.SchemaField("estado", "STRING"),
            bigquery.SchemaField("comprobante", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("last_modified_at", "TIMESTAMP"),
        ]
    },
    "raw_gastos": {
        "uri": f"gs://{BUCKET_NAME}/raw_gastos/*",
        "partitioned": True,
        "hive_partition_uri_prefix": f"gs://{BUCKET_NAME}/raw_gastos",
        "schema": [
            bigquery.SchemaField("id_gasto", "INTEGER"),
            bigquery.SchemaField("nombre_gasto", "STRING"),
            bigquery.SchemaField("fecha_pago", "TIMESTAMP"),
            bigquery.SchemaField("id_caso", "INTEGER"),
            bigquery.SchemaField("medio_pago", "STRING"),
            bigquery.SchemaField("monto", "FLOAT"),
            bigquery.SchemaField("estado", "STRING"),
            bigquery.SchemaField("comprobante", "STRING"),
            bigquery.SchemaField("id_proveedor", "INTEGER"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("last_modified_at", "TIMESTAMP"),
        ]
    },
    "raw_casos": {
        "uri": f"gs://{BUCKET_NAME}/raw_casos/*",
        "partitioned": True,
        "hive_partition_uri_prefix": f"gs://{BUCKET_NAME}/raw_casos",
        "schema": [
            bigquery.SchemaField("id_caso", "INTEGER"),
            bigquery.SchemaField("nombre_caso", "STRING"),
            bigquery.SchemaField("estado", "STRING"),
            bigquery.SchemaField("fecha_ingreso", "TIMESTAMP"),
            bigquery.SchemaField("fecha_salida", "TIMESTAMP"),
            bigquery.SchemaField("veterinaria", "STRING"),
            bigquery.SchemaField("diagnostico", "STRING"),
            bigquery.SchemaField("archivo", "STRING"),
            bigquery.SchemaField("id_hogar_de_paso", "INTEGER"),
            bigquery.SchemaField("presupuesto_estimado", "FLOAT"),
            bigquery.SchemaField("ciudad", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("last_modified_at", "TIMESTAMP"),
        ]
    },
    "raw_donantes": {
        "uri": f"gs://{BUCKET_NAME}/raw_donantes/*",
        "partitioned": True,
        "hive_partition_uri_prefix": f"gs://{BUCKET_NAME}/raw_donantes",
        "schema": [
            bigquery.SchemaField("id_donante", "INTEGER"),
            bigquery.SchemaField("donante", "STRING"),
            bigquery.SchemaField("tipo_id", "STRING"),
            bigquery.SchemaField("identificacion", "STRING"),
            bigquery.SchemaField("correo", "STRING"),
            bigquery.SchemaField("telefono", "STRING"),
            bigquery.SchemaField("ciudad", "STRING"),
            bigquery.SchemaField("tipo_donante", "STRING"),
            bigquery.SchemaField("pais", "STRING"),
            bigquery.SchemaField("canal_origen", "STRING"),
            bigquery.SchemaField("consentimiento", "BOOLEAN"),
            bigquery.SchemaField("notas", "STRING"),
            bigquery.SchemaField("archivos", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("last_modified_at", "TIMESTAMP"),
        ]
    },
    "raw_proveedores": {
        "uri": f"gs://{BUCKET_NAME}/raw_proveedores/*.parquet",
        "partitioned": False,
        "schema": [
            bigquery.SchemaField("id_proveedor", "INTEGER"),
            bigquery.SchemaField("nombre_proveedor", "STRING"),
            bigquery.SchemaField("tipo_proveedor", "STRING"),
            bigquery.SchemaField("nit", "STRING"),
            bigquery.SchemaField("nombre_contacto", "STRING"),
            bigquery.SchemaField("correo", "STRING"),
            bigquery.SchemaField("telefono", "STRING"),
            bigquery.SchemaField("ciudad", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("last_modified_at", "TIMESTAMP"),
        ]
    },
    "raw_hogar_de_paso": {
        "uri": f"gs://{BUCKET_NAME}/raw_hogar_de_paso/*.parquet",
        "partitioned": False,
        "schema": [
            bigquery.SchemaField("id_hogar_de_paso", "INTEGER"),
            bigquery.SchemaField("nombre_hogar", "STRING"),
            bigquery.SchemaField("tipo_hogar", "STRING"),
            bigquery.SchemaField("nombre_contacto", "STRING"),
            bigquery.SchemaField("correo", "STRING"),
            bigquery.SchemaField("telefono", "STRING"),
            bigquery.SchemaField("ciudad", "STRING"),
            bigquery.SchemaField("pais", "STRING"),
            bigquery.SchemaField("cupo_maximo", "INTEGER"),
            bigquery.SchemaField("tarifa_diaria", "FLOAT"),
            bigquery.SchemaField("desempeno", "STRING"),
            bigquery.SchemaField("ultimo_contacto", "TIMESTAMP"),
            bigquery.SchemaField("notas", "STRING"),
        ]
    },
}


# ============================================
# CREAR EXTERNAL TABLES
# ============================================

for table_name, config in EXTERNAL_TABLES.items():
    print(f"\n{'='*70}")
    print(f"📊 Creando external table: {table_name}")
    print(f"{'='*70}")
    
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    # Configurar external data
    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [config["uri"]]
    external_config.autodetect = False  # Usamos schema explícito
    
    # Configurar particionado Hive si aplica
    if config["partitioned"]:
        hive_partitioning = bigquery.HivePartitioningOptions()
        hive_partitioning.mode = "AUTO"
        hive_partitioning.source_uri_prefix = config["hive_partition_uri_prefix"]
        hive_partitioning.require_partition_filter = False  # Permitir queries sin filtro de partición
        external_config.hive_partitioning = hive_partitioning
        print(f"   🗂️  Particionado Hive: {config['hive_partition_uri_prefix']}")
    
    # Crear tabla
    table = bigquery.Table(table_id, schema=config["schema"])
    table.external_data_configuration = external_config
    
    # Eliminar tabla si existe para asegurar recreación limpia
    client.delete_table(table_id, not_found_ok=True)
    print(f"   🗑️  Tabla eliminada (si existía): {table_id}")

    try:
        # Crear tabla
        table = client.create_table(table)
        print(f"   ✅ Tabla creada: {table_id}")
    except Exception as e:
        print(f"   ❌ Error creando tabla: {e}")
        continue
    
    print(f"   📍 URI: {config['uri']}")
    print(f"   📋 Columnas: {len(config['schema'])}")


# ============================================
# VERIFICAR TABLAS CREADAS
# ============================================

print(f"\n{'='*70}")
print("📋 VERIFICACIÓN - Tablas en dataset 'raw'")
print(f"{'='*70}")

tables = list(client.list_tables(f"{PROJECT_ID}.{DATASET_ID}"))

if tables:
    for table in tables:
        full_table = client.get_table(table)
        is_external = full_table.external_data_configuration is not None
        table_type = "External" if is_external else "Native"
        print(f"   - {table.table_id} ({table_type})")
else:
    print("   ⚠️  No hay tablas en el dataset")

print(f"\n{'='*70}")
print("✅ EXTERNAL TABLES CREADAS")
print(f"{'='*70}")

print(f"\n💡 Próximo paso - Probar queries:")
print(f"   SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.raw_donaciones` LIMIT 10")
print(f"   SELECT anio, mes, COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.raw_donaciones` GROUP BY anio, mes")
