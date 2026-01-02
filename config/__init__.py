"""
Configuración base del proyecto.
Define constantes y configuraciones compartidas entre entornos.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Detectar entorno y base path
if os.path.exists("/opt/airflow"): # Si existe esta ruta, estamos en Docker
    BASE_DIR = Path("/opt/airflow")
else:
    BASE_DIR = Path(__file__).resolve().parent.parent

# Entorno de Ejecución (default: local)
ENV = os.getenv("ENV", "local") # local | prod

# ============================================
# PATHS - Rutas del Proyecto
# ============================================
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_RAW_DIR = DATA_DIR / "raw"
DATA_PROCESSED_DIR = DATA_DIR / "processed"
DATA_OUTPUT_DIR = DATA_DIR / "output"
LOGS_DIR = PROJECT_ROOT / "logs"

# Crear directorios si no existen
for directory in [DATA_RAW_DIR, DATA_PROCESSED_DIR, DATA_OUTPUT_DIR, LOGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# ============================================
# STORAGE CONFIGURATION (Cloud Agnostic)
# ============================================
PROJECT_ID = "salvando-patitas-de-spark"

if ENV == "local":
    # --- CONFIGURACIÓN LOCAL ---
    print("🌍 MODO: LOCAL (Usando sistema de archivos local)")
    STORAGE_PROTOCOL = "file://"
    # Usar ruta absoluta para evitar problemas con file:// en Spark
    LOCAL_LAKE_PATH = str(DATA_DIR / "lake")
    RAW_PATH =    str(Path(LOCAL_LAKE_PATH) / "raw")
    SILVER_PATH = str(Path(LOCAL_LAKE_PATH) / "silver")
    GOLD_PATH =   str(Path(LOCAL_LAKE_PATH) / "gold")
    QUARANTINE_PATH = str(Path(LOCAL_LAKE_PATH) / "quarantine")
    
    # Asegurar que existan carpetas locales
    for p in [RAW_PATH, SILVER_PATH, GOLD_PATH, QUARANTINE_PATH]:
        Path(p).mkdir(parents=True, exist_ok=True)

else:
    # --- CONFIGURACIÓN PROD / GCS ---
    print("☁️ MODO: CLOUD (Usando Google Cloud Storage)")
    STORAGE_PROTOCOL = "gs://"
    BUCKET_NAME = "salvando-patitas-spark"
    
    RAW_PATH = f"{STORAGE_PROTOCOL}{BUCKET_NAME}/lake/raw"
    SILVER_PATH = f"{STORAGE_PROTOCOL}{BUCKET_NAME}/lake/silver"
    GOLD_PATH = f"{STORAGE_PROTOCOL}{BUCKET_NAME}/lake/gold"
    QUARANTINE_PATH = f"{STORAGE_PROTOCOL}{BUCKET_NAME}/lake/quarantine"

# ============================================
# SUPABASE - Credenciales
# ============================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# ============================================
# SPARK - Configuración
# ============================================
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[2]")
SPARK_APP_NAME = "CRM-Data-Platform"
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "3g")
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "3g")

# ============================================
# TABLAS - Configuración de Fuentes de Datos
# ============================================

# Tablas incrementales (con columna de fecha para watermark)
# Se usa 'last_modified_at' para particionar y detectar cambios (CDC Pattern)
INCREMENTAL_TABLES = {
    "donaciones": "last_modified_at",
    "gastos": "last_modified_at",
    "donantes": "created_at",
}

# Tablas snapshot (Full overwrite - sin last_modified_at o muy pocos registros)
FULL_LOAD_TABLES = [
    "proveedores",  # ~60 registros, snapshot puro
    "hogar_de_paso",  # ~8 registros, snapshot puro (sin last_modified_at)
    "casos",    # Dimensión maestra (~200 registros), mejor full load para evitar desfaces
]

# ============================================
# SCHEMAS - Definición de Columnas por Tabla
# ============================================

TABLE_SCHEMAS = {
    "donaciones": {
        "id": "id_donacion",
        "fecha": "fecha_donacion",
        "monto": "monto",
        "fk_donante": "id_donante",
        "fk_caso": "id_caso",
        "columns": [
            "id_donacion", "id_donante", "id_caso", "fecha_donacion",
            "monto", "medio_pago", "estado", "comprobante",
            "created_at", "last_modified_at"
        ]
    },
    "gastos": {
        "id": "id_gasto",
        "fecha": "fecha_pago",
        "monto": "monto",
        "fk_proveedor": "id_proveedor",
        "fk_caso": "id_caso",
        "columns": [
            "id_gasto", "nombre_gasto", "fecha_pago", "id_caso",
            "medio_pago", "monto", "estado", "comprobante",
            "id_proveedor", "created_at", "last_modified_at"
        ]
    },
    "donantes": {
        "id": "id_donante",
        "columns": [
            "id_donante", "donante", "tipo_id", "identificacion",
            "correo", "telefono", "ciudad", "tipo_donante", "pais",
            "canal_origen", "consentimiento", "notas", "archivos",
            "created_at", "last_modified_at"
        ]
    },
    "casos": {
        "id": "id_caso",
        "fecha": "fecha_ingreso",
        "columns": [
            "id_caso", "nombre_caso", "estado", "fecha_ingreso",
            "fecha_salida", "veterinaria", "diagnostico", "archivo",
            "id_hogar_de_paso", "presupuesto_estimado", "ciudad",
            "created_at", "last_modified_at"
        ]
    },
    "proveedores": {
        "id": "id_proveedor",
        "columns": [
            "id_proveedor", "nombre_proveedor", "tipo_proveedor",
            "nit", "nombre_contacto", "correo", "telefono", "ciudad",
            "created_at", "last_modified_at"
        ]
    },
    "hogar_de_paso": {
        "id": "id_hogar_de_paso",
        "columns": [
            "id_hogar_de_paso", "nombre_hogar", "tipo_hogar",
            "nombre_contacto", "correo", "telefono", "ciudad", "pais",
            "cupo_maximo", "tarifa_diaria", "desempeno",
            "ultimo_contacto", "notas"
        ]
    }
}

# ============================================
# PARTICIONADO - Configuración por Capa
# ============================================

# Bronze: Sin particionado (datos raw)
BRONZE_PARTITION = {}

# Silver: Particionado por año/mes
SILVER_PARTITION = {
    "donaciones": ["anio", "mes"],  # Derivados de fecha_donacion
    "gastos": ["anio", "mes"],      # Derivados de fecha_pago
    "casos": ["anio", "mes"],       # Derivados de fecha_ingreso
}

# Gold: Particionado por año (agregaciones)
GOLD_PARTITION = {
    "donaciones_monthly": ["anio"],
    "gastos_monthly": ["anio"],
}

# ============================================
# DATA QUALITY - Reglas de Validación
# ============================================

REQUIRED_COLUMNS = {
    "donaciones": ["id_donacion", "fecha_donacion", "monto", "id_donante"],
    "gastos": ["id_gasto", "fecha_pago", "monto", "id_proveedor"],
    "donantes": ["id_donante", "donante", "correo"],
    "casos": ["id_caso", "nombre_caso", "fecha_ingreso"],
    "proveedores": ["id_proveedor", "nombre_proveedor"],
}

# Validaciones de rango de fechas (últimos N años)
DATE_RANGE_YEARS = 5

# Validaciones de montos
MIN_AMOUNT = 0
MAX_AMOUNT = 100_000_000  # 100 millones

# ============================================
# LOGGING
# ============================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# ============================================
# TEST MODE
# ============================================
# Set to "YYYY-MM" to process only that month (e.g., "2024-01").
# Set to None to process all data.
TEST_MONTH = os.getenv("TEST_MONTH")  # None = procesar toda la data de Supabase

