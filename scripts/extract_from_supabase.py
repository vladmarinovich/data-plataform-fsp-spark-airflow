"""
Script de extracción incremental desde Supabase.
Usa last_modified_at como watermark para todas las tablas.

Estrategia:
- Primera ejecución: Full load (watermark = NULL)
- Siguientes: Incremental (WHERE last_modified_at > watermark)
- Watermark se guarda por tabla en watermarks.json
"""
import sys
from pathlib import Path
from datetime import datetime
import json

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from supabase import create_client, Client
import config
from config import FULL_LOAD_TABLES
# Importar utilidades de GCS
from jobs.utils.watermark import _read_from_gcs, PIPELINE_NAME as GLOBAL_PIPELINE_NAME


# ============================================
# CONFIGURACIÓN
# ============================================

WATERMARKS_FILE = config.PROJECT_ROOT / "watermarks.json"

# TODAS las tablas usan last_modified_at como watermark
ALL_TABLES = list(config.INCREMENTAL_TABLES.keys()) + config.FULL_LOAD_TABLES


# ============================================
# FUNCIONES DE WATERMARKS
# ============================================

def load_watermarks() -> dict:
    """
    Carga el Watermark GLOBAL desde GCS.
    
    Returns:
        Diccionario simple con la fecha global.
        Ejemplo: {"global_watermark": "2023-04-01T00:00:00"}
    """
    print(f"☁️  Leyendo estado global desde GCS...")
    try:
        data = _read_from_gcs()
        
        # Extraer watermark global
        # Estructura esperada en GCS: { "spdp_data_platform_main": { "watermark": "..." } }
        global_wm = data.get(GLOBAL_PIPELINE_NAME, {}).get("watermark")
        
        if global_wm:
            print(f"✅ Globar Watermark encontrado: {global_wm}")
            return {"global_watermark": global_wm}
        else:
            print("⚠️  No hay watermark global registrado. Se asumirá FULL LOAD (o Clean Slate).")
            return {}
            
    except Exception as e:
        print(f"⚠️  Error leyendo GCS: {e}")
        return {}


def save_watermarks(watermarks: dict) -> None:
    """
    Guarda watermarks en archivo JSON.
    
    Args:
        watermarks: Diccionario con fechas por tabla
    """
    try:
        with open(WATERMARKS_FILE, 'w') as f:
            json.dump(watermarks, f, indent=2)
        print(f"\n✅ Watermarks actualizados:")
        for table, wm in watermarks.items():
            print(f"   - {table}: {wm}")
    except Exception as e:
        print(f"❌ Error guardando watermarks: {e}")
        raise


# ============================================
# CONEXIÓN A SUPABASE
# ============================================

def get_supabase_client() -> Client:
    """
    Crea cliente de Supabase usando credenciales del .env
    
    Returns:
        Cliente de Supabase autenticado
        
    Raises:
        ValueError: Si faltan credenciales
    """
    url = config.SUPABASE_URL
    key = config.SUPABASE_KEY
    
    if not url or not key:
        raise ValueError(
            "❌ Faltan credenciales de Supabase.\n"
            "   Configura SUPABASE_URL y SUPABASE_KEY en el archivo .env"
        )
    
    client = create_client(url, key)
    print(f"✅ Conectado a Supabase: {url}")
    
    return client


# ============================================
# EXTRACCIÓN DE DATOS
# ============================================

def extract_table(
    client: Client, 
    table_name: str, 
    watermark: str = None
) -> pd.DataFrame:
    """
    Extrae datos de una tabla (full o incremental según watermark).
    
    Args:
        client: Cliente de Supabase
        table_name: Nombre de la tabla
        watermark: Última fecha procesada (None = full load)
        
    Returns:
        DataFrame de pandas con datos nuevos
    """
    print(f"\n{'='*70}")
    print(f"📥 Extrayendo tabla: {table_name}")
    print(f"{'='*70}")
    
    # Determinar si es Full Load permanente (tablas snapshot)
    is_snapshot = table_name in config.FULL_LOAD_TABLES
    
    if is_snapshot:
        print("   ℹ️  Tabla tipo SNAPSHOT (Siempre Full Load)")
        watermark = None # Ignorar watermark previo

    if watermark:
        print(f"   Modo: INCREMENTAL")
        print(f"   Watermark actual: {watermark}")
    else:
        print(f"   Modo: FULL LOAD")
    
    all_data = []
    offset = 0
    batch_size = 200 # Reducido para evitar OOM (SIGTERM)
    max_date_str = None # Inicializar variable para scope

    # LÍMITE TRIMESTRAL: Procesar máximo 3 meses por ejecución
    from datetime import datetime, timedelta
    
    # Calcular fecha límite superior (watermark + 3 meses)
    if watermark and not is_snapshot:
        watermark_date = datetime.fromisoformat(watermark.replace('Z', '+00:00'))
        max_date = watermark_date + timedelta(days=90)  # ~3 meses
        max_date_str = max_date.isoformat()
        print(f"   📅 Límite trimestral (Incremental): {watermark} → {max_date_str} (Q{(watermark_date.month-1)//3 + 1} -> Q{(max_date.month-1)//3 + 1})")
    elif not is_snapshot:
        # Si NO hay watermark (Carga Inicial), forzar carga solo del primer trimestre (Q1 2023)
        # para evitar sobrecarga de memoria y particiones.
        print(f"   🚀 Detectada Carga Inicial (Clean Slate). Limitando a Q1 2023.")
        
        # Definir inicio ficticio para Q1 2023
        start_date_q1 = datetime(2023, 1, 1)
        watermark = start_date_q1.isoformat() # Usamos esto para el filtro gte
        
        max_date = start_date_q1 + timedelta(days=90) # Hasta ~abril 2023
        max_date_str = max_date.isoformat()
        print(f"   📅 Límite Carga Inicial: {watermark} → {max_date_str} (Q1 2023)")
    else:
        max_date_str = None
    
    try:
        while True:
            # Query base
            query = client.table(table_name).select("*")
            
            # Filtro de watermark (desde última ejecución)
            if watermark and not is_snapshot:
                query = query.gte("last_modified_at", watermark)
            
            # LÍMITE SUPERIOR: Máximo 3 meses por ejecución
            if max_date_str and not is_snapshot:
                query = query.lte("last_modified_at", max_date_str)
            
            # Ordenar
            if not is_snapshot:
                query = query.order("last_modified_at")
            
            # Paginación
            query = query.range(offset, offset + batch_size - 1)
            
            # Ejecutar query
            response = query.execute()
            
            if not response.data:
                break
                
            all_data.extend(response.data)
            
            # Si recibimos menos del batch, es la última página
            if len(response.data) < batch_size:
                break
                
            offset += batch_size
            print(f"   ... extraídos {len(all_data)} registros", end='\r')

        # Guardar estado del pipeline (Next Watermark)
        if not is_snapshot:
            import json
            state_file = "/tmp/pipeline_state.json"
            now_iso = datetime.utcnow().isoformat()
            
            if max_date_str:
                next_watermark = min(max_date_str, now_iso)
            else:
                next_watermark = now_iso
            
            with open(state_file, 'w') as f:
                json.dump({"next_watermark": next_watermark}, f)
            # print(f"\n   💾 Estado guardado: next_watermark = {next_watermark}")

        if not all_data:
            print(f"   ⚠️ No hay datos nuevos.")
            return pd.DataFrame() # Retornar vacío
            
        print(f"   ✅ Total registros extraídos: {len(all_data)}")
        
        # Mostrar rango de fechas
        df = pd.DataFrame(all_data)
        if 'last_modified_at' in df.columns and not df.empty:
            try:
                min_date = df['last_modified_at'].min()
                max_date = df['last_modified_at'].max()
                print(f"   Rango last_modified_at: {min_date} → {max_date}")
            except:
                pass
                
        return df
            
    except Exception as e:
        print(f"   ❌ Error extrayendo '{table_name}': {e}")
        raise


# ============================================
# ESCRITURA A BRONZE LAYER
# ============================================

def write_to_bronze(df: pd.DataFrame, table_name: str, mode: str = "append") -> None:
    """
    Escribe DataFrame a Bronze layer particionado por fecha de ingesta (Hive-style).
    Path: data/lake/raw/{table_name}/ingest_date=YYYY-MM-DD/file.parquet
    """
    if df.empty:
        print(f"   ⚠️  No hay datos para escribir")
        return
    
    # Calcular particiones basadas en last_modified_at (Historico) o Ingesta (Snapshot)
    if "last_modified_at" in df.columns:
        print("   📅 Particionando por 'last_modified_at' (Negocio)...")
        # Asegurar tipo datetime
        dates = pd.to_datetime(df["last_modified_at"], errors="coerce", utc=True)
        # Rellenar NaT con ahora (fallback)
        dates = dates.fillna(pd.Timestamp.now(tz="UTC"))
        
        df["y"] = dates.dt.strftime("%Y")
        df["m"] = dates.dt.strftime("%m")
        df["d"] = dates.dt.strftime("%d")
    else:
        print("   📅 Particionando por fecha de ingesta (System)...")
        now = datetime.now()
        df["y"] = now.strftime("%Y")
        df["m"] = now.strftime("%m")
        df["d"] = now.strftime("%d")
    
    # Determinar si estamos en cloud o local
    is_cloud = config.ENV != "local"
    
    if is_cloud:
        # En cloud: escribir localmente primero, luego subir a GCS
        local_temp_path = Path(f"/tmp/bronze_{table_name}")
        table_path = local_temp_path
    else:
        # En local: escribir directamente
        table_path = Path(config.RAW_PATH) / table_name
    
    print(f"\n💾 Escribiendo a Bronze layer (Hive Style):")
    print(f"   Path: {table_path}")
    
    try:
        # Usar pyarrow para escribir particionado
        import pyarrow as pa
        import pyarrow.parquet as pq
        
        table = pa.Table.from_pandas(df)
        
        pq.write_to_dataset(
            table,
            root_path=str(table_path),
            partition_cols=["y", "m"],
            existing_data_behavior="overwrite_or_ignore", 
            compression="snappy"
        )
        
        print(f"   ✅ Escritura local completada: {len(df)} registros distribuidos en particiones.")
        
        # Si estamos en cloud, subir a GCS
        if is_cloud:
            import subprocess
            import os
            
            gcs_path = f"{config.RAW_PATH}/{table_name}"
            print(f"   ☁️  Subiendo a GCS: {gcs_path}")
            
            # Usar módulo de credenciales (detecta automáticamente dev/prod)
            import sys
            # Path ya está importado al inicio del archivo
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from config.credentials import get_gcs_client
            
            print(f"   🔑 Detectando credenciales automáticamente...")

            
            # Crear cliente de GCS (usa ADC en dev, service account en prod)
            storage_client = get_gcs_client()
            bucket_name = "salvando-patitas-spark"
            bucket = storage_client.bucket(bucket_name)
            
            # Subir todos los archivos del directorio local
            uploaded_count = 0
            for root, dirs, files in os.walk(table_path):
                for file in files:
                    local_file = os.path.join(root, file)
                    # Calcular path relativo en GCS
                    rel_path = os.path.relpath(local_file, table_path)
                    blob_path = f"lake/raw/{table_name}/{rel_path}"
                    
                    blob = bucket.blob(blob_path)
                    blob.upload_from_filename(local_file)
                    uploaded_count += 1
            
            print(f"   ✅ Subida a GCS completada: {uploaded_count} archivos")
            
            # Limpiar temp
            import shutil
            shutil.rmtree(table_path)
            
    except Exception as e:
        print(f"   ❌ Error escribiendo a Bronze: {e}")
        raise


# ============================================
# PIPELINE PRINCIPAL
# ============================================

def extract_all_tables(client: Client, watermarks: dict) -> dict:
    """
    Extrae todas las tablas usando el Watermark Global.
    """
    print("\n" + "="*70)
    print("🚀 EXTRACCIÓN DE TODAS LAS TABLAS")
    print("="*70)
    
    global_watermark = watermarks.get("global_watermark")
    
    # Si hay watermark global, todas las tablas arrancan desde ahí
    
    for table_name in ALL_TABLES:
        
        # Lógica Híbrida:
        # 1. Si es tabla FULL LOAD (Dimensiones), ignorar watermark -> Overwrite Raw
        # 2. Si es tabla INCREMENTAL (Hechos), usar Global Watermark -> Append Raw
        
        if table_name in FULL_LOAD_TABLES:
            print(f"🔄 Tabla {table_name} es FULL LOAD. Ignorando watermark.")
            current_watermark = None
        else:
            current_watermark = global_watermark
        
        # Extraer datos
        df = extract_table(client, table_name, current_watermark)
        
        if not df.empty:
            # Determinar modo de escritura
            # Si current_watermark es None (Full Load) -> Overwrite
            # Si hay watermark (Incremental) -> Append
            mode = "append" if current_watermark else "overwrite"
            
            # Escribir a Bronze
            write_to_bronze(df, table_name, mode=mode)
            
            # (Nota: El cálculo del nuevo watermark se delega a Spark)
    
    return {} # No necesitamos devolver nada complejo ya


def main():
    """Función principal del script de extracción."""
    print("="*70)
    print("🏁 INICIANDO EXTRACCIÓN INCREMENTAL DESDE SUPABASE")
    print(f"   Timestamp: {datetime.now()}")
    print("="*70)
    
    try:
        # 1. Conectar a Supabase
        client = get_supabase_client()
        
        # 2. Cargar watermarks
        watermarks = load_watermarks()
        
        # 3. Extraer todas las tablas
        new_watermarks = extract_all_tables(client, watermarks)
        
        # 4. Guardar watermarks actualizados (DESACTIVADO - Delegado al Orquestador al final del DAG)
        # save_watermarks(new_watermarks)
        print("\nℹ️  Watermark update SKIPPED (State update delegated to final DAG step)")
        
        print("\n" + "="*70)
        print("✅ EXTRACCIÓN COMPLETADA EXITOSAMENTE")
        print("="*70)
        
        # Mostrar resumen
        print("\n📊 Resumen:")
        print(f"   - Tablas procesadas: {len(ALL_TABLES)}")
        print(f"   - Archivos en Bronze: {len(list(config.DATA_RAW_DIR.glob('*.parquet')))}")
        
        print("\n💡 Próximo paso:")
        print("   spark-submit --master local[*] jobs/transform_all.py")

        print("\n💡 Próximo paso:")
        print("   spark-submit --master local[*] jobs/transform_all.py")
        
    except Exception as e:
        print(f"\n❌ Error en la extracción: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
