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
    Carga watermarks desde archivo JSON.
    
    Returns:
        Diccionario con última fecha procesada por tabla.
        Ejemplo: {"donaciones": "2024-12-20T10:30:00", "gastos": "2024-12-15T08:00:00"}
    """
    if not WATERMARKS_FILE.exists():
        print("⚠️  No existe archivo de watermarks, creando nuevo...")
        print("   → Primera ejecución: Se hará FULL LOAD de todas las tablas")
        return {}
    
    try:
        with open(WATERMARKS_FILE, 'r') as f:
            watermarks = json.load(f)
        print(f"✅ Watermarks cargados:")
        for table, wm in watermarks.items():
            print(f"   - {table}: {wm}")
        return watermarks
    except Exception as e:
        print(f"⚠️  Error leyendo watermarks: {e}")
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
    
    try:
        # Query base
        query = client.table(table_name).select("*")
        
        # Si hay watermark (y no es snapshot), filtrar
        if watermark and not is_snapshot:
            query = query.gt("last_modified_at", watermark)
        
        # Ordenar (solo si es incremental/tiene la columna)
        if not is_snapshot:
            query = query.order("last_modified_at")
        
        # Ejecutar query
        response = query.execute()
        
        if response.data:
            df = pd.DataFrame(response.data)
            
            print(f"   ✅ Registros extraídos: {len(df)}")
            
            # Mostrar rango de fechas
            if 'last_modified_at' in df.columns:
                min_date = df['last_modified_at'].min()
                max_date = df['last_modified_at'].max()
                print(f"   Rango last_modified_at: {min_date} → {max_date}")
            
            return df
        else:
            print(f"   ⚠️  No hay datos nuevos")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"   ❌ Error extrayendo '{table_name}': {e}")
        raise


# ============================================
# ESCRITURA A BRONZE LAYER
# ============================================

def write_to_bronze(df: pd.DataFrame, table_name: str, mode: str = "append") -> None:
    """
    Escribe DataFrame a Bronze layer en formato Parquet.
    
    Args:
        df: DataFrame a escribir
        table_name: Nombre de la tabla
        mode: "append" para incremental, "overwrite" para full
    """
    if df.empty:
        print(f"   ⚠️  No hay datos para escribir")
        return
    
    output_path = config.DATA_RAW_DIR / f"{table_name}.parquet"
    
    print(f"\n💾 Escribiendo a Bronze layer:")
    print(f"   Path: {output_path}")
    print(f"   Modo: {mode}")
    
    try:
        if mode == "overwrite" or not output_path.exists():
            # Overwrite o primera vez
            df.to_parquet(
                output_path,
                engine="pyarrow",
                compression="snappy",
                index=False
            )
            print(f"   ✅ Escritura completa: {len(df)} registros")
            
        elif mode == "append":
            # Append: leer existente, concatenar, deduplicar, escribir
            df_existing = pd.read_parquet(output_path)
            df_combined = pd.concat([df_existing, df], ignore_index=True)
            
            # Deduplicar por ID (mantener el más reciente según last_modified_at)
            id_col = config.TABLE_SCHEMAS[table_name]["id"]
            if id_col in df_combined.columns:
                df_combined = df_combined.sort_values('last_modified_at', ascending=False)
                df_combined = df_combined.drop_duplicates(subset=[id_col], keep='first')
                print(f"   🔄 Deduplicación por '{id_col}'")
            
            df_combined.to_parquet(
                output_path,
                engine="pyarrow",
                compression="snappy",
                index=False
            )
            print(f"   ✅ Append exitoso:")
            print(f"      - Registros nuevos: {len(df)}")
            print(f"      - Total en Bronze: {len(df_combined)}")
            
    except Exception as e:
        print(f"   ❌ Error escribiendo a Bronze: {e}")
        raise


# ============================================
# PIPELINE PRINCIPAL
# ============================================

def extract_all_tables(client: Client, watermarks: dict) -> dict:
    """
    Extrae todas las tablas (incremental o full según watermark).
    
    Args:
        client: Cliente de Supabase
        watermarks: Diccionario con watermarks actuales
        
    Returns:
        Diccionario con watermarks actualizados
    """
    print("\n" + "="*70)
    print("🚀 EXTRACCIÓN DE TODAS LAS TABLAS")
    print("="*70)
    
    new_watermarks = watermarks.copy()
    
    for table_name in ALL_TABLES:
        # Obtener watermark actual (None si no existe)
        current_watermark = watermarks.get(table_name)
        
        # Extraer datos
        df = extract_table(client, table_name, current_watermark)
        
        if not df.empty:
            # Determinar modo de escritura
            mode = "append" if current_watermark else "overwrite"
            
            # Escribir a Bronze
            write_to_bronze(df, table_name, mode=mode)
            
            # Calcular nuevo watermark (máximo last_modified_at)
            if 'last_modified_at' in df.columns:
                max_date = pd.to_datetime(df['last_modified_at'], format='ISO8601').max()
                new_watermark = max_date.isoformat()
                
                # Actualizar solo si es mayor (o si no existía)
                if not current_watermark or new_watermark > current_watermark:
                    new_watermarks[table_name] = new_watermark
    
    return new_watermarks


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
        
        # 4. Guardar watermarks actualizados
        save_watermarks(new_watermarks)
        
        print("\n" + "="*70)
        print("✅ EXTRACCIÓN COMPLETADA EXITOSAMENTE")
        print("="*70)
        
        # Mostrar resumen
        print("\n📊 Resumen:")
        print(f"   - Tablas procesadas: {len(ALL_TABLES)}")
        print(f"   - Archivos en Bronze: {len(list(config.DATA_RAW_DIR.glob('*.parquet')))}")
        
        print("\n💡 Próximo paso:")
        print("   spark-submit --master local[*] jobs/transform_all.py")
        
    except Exception as e:
        print(f"\n❌ Error en la extracción: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
