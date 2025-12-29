
import sys
import os
from pathlib import Path
import pandas as pd
import shutil
import pyarrow as pa
import pyarrow.parquet as pq

# Agregar raíz del proyecto al path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))
import config

print("♻️ RE-PARTICIONANDO RAW POR FECHA HISTÓRICA")
print("="*80)

# Configuración de columnas de fecha por tabla
# PRIORIDAD: last_modified_at (CDC Real) > created_at > Ingest Date
DATE_COLS = {
    "donaciones": "last_modified_at",
    "gastos": "last_modified_at",
    "casos": "last_modified_at",
    "donantes": "last_modified_at",
    "proveedores": "last_modified_at",
    "hogar_de_paso": "ingest_date"
}

# Columnas para el filtro de negocio (Q1 2023)
BUSINESS_DATE_COLS = {
    "donaciones": "fecha_donacion",
    "gastos": "fecha_pago",
    "donantes": "last_modified_at",
    "casos": "last_modified_at"
}

# Filtro Q1 2023 Hardcodeado para mantener regla de negocio
Q1_LIMIT = pd.Timestamp("2023-03-31 23:59:59").tz_localize(None)
TABLES_Q1_ONLY = ["donaciones", "gastos"]  # donantes y casos son maestros, se traen completos

base_path = Path(config.RAW_PATH)
temp_path = Path(config.DATA_DIR) / "raw_temp"

if temp_path.exists(): shutil.rmtree(temp_path)
temp_path.mkdir()

for table in ["donaciones", "gastos", "casos", "donantes", "proveedores", "hogar_de_paso"]:
    print(f"📦 Procesando tabla: {table}")
    source_dir = base_path / table
    
    if not source_dir.exists():
        print(f"   ⚠️ No existe directorio {source_dir}")
        continue
        
    files = list(source_dir.glob("**/*.parquet"))
    if not files:
        print(f"   ⚠️ No hay archivos en {table}")
        continue
        
    # Cargar DF completo
    dfs = [pd.read_parquet(f) for f in files]
    if not dfs: continue
    df = pd.concat(dfs, ignore_index=True)
    
    # Limpiar particiones previas
    for col in ['y', 'm', 'd', 'ingest_date']:
        if col in df.columns:
            df = df.drop(columns=[col])
            
    # Determinar columna fecha
    date_col = DATE_COLS.get(table)
    
    # Estrategia de Fechado
    if date_col and date_col in df.columns:
        print(f"   📅 Usando columna técnica (Partition): {date_col}")
        try:
            # Parsear ISO string a DT
            series = pd.to_datetime(df[date_col], errors='coerce', utc=True).dt.tz_localize(None)
            
            # FILTRO Q1 (Basado en BUSINESS_DATE_COLS)
            if table in TABLES_Q1_ONLY:
                biz_col = BUSINESS_DATE_COLS.get(table)
                if biz_col in df.columns:
                    initial_len = len(df)
                    biz_series = pd.to_datetime(df[biz_col], errors='coerce', utc=True).dt.tz_localize(None)
                    df = df[biz_series <= Q1_LIMIT]
                    series = series[biz_series <= Q1_LIMIT] # Alinear series de partición
                    print(f"   📉 Filtro Q1 aplicado ({table}) via {biz_col}: {initial_len} -> {len(df)}")
            
        except Exception as e:
            print(f"   ❌ Error parseando {date_col}: {e}")
            continue
    else:
        print(f"   ⚠️ No tiene {date_col}. Usando fecha actual (2025).")
        series = pd.Series([pd.Timestamp.now()] * len(df))

    # Crear columnas de partición
    df['y'] = series.dt.strftime('%Y')
    df['m'] = series.dt.strftime('%m')
    
    # Determinar esquema de partición (maestros usan y/m, transaccionales y/m/d)
    partition_cols = ['y', 'm']
    if table in ['donaciones', 'gastos']:
        df['d'] = series.dt.strftime('%d')
        partition_cols = ['y', 'm', 'd']
    
    # Filtrar nulos y escribir
    df = df.dropna(subset=partition_cols)
    
    # Escribir nueva estructura
    out_table_path = temp_path / table
    
    table_pa = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table_pa,
        root_path=str(out_table_path),
        partition_cols=partition_cols,
        compression='snappy',
        existing_data_behavior="overwrite_or_ignore"
    )
    
    # --- PASO EXTRA: RENOMBRAMIENTO CANÓNICO ---
    # Formato: raw_{nombre_tabla}_{00#}.parquet
    print(f"   🏷️ Renombrando archivos en {out_table_path}...")
    all_files = list(out_table_path.glob("**/*.parquet"))
    # Ordenar para determinismo
    all_files.sort()
    
    new_name = ""
    for i, file_path in enumerate(all_files, 1):
        new_name = f"raw_{table}_{i:04d}.parquet"
        new_path = file_path.parent / new_name
        file_path.rename(new_path)
        
    print(f"   ✅ Re-escrito y renombrado: {len(all_files)} archivos")
    if len(df) > 0 and new_name:
        print(f"      - Ejemplo: {new_name}")


print("\n🔄 Sustituyendo data/raw con nueva estructura...")
if base_path.exists():
    shutil.rmtree(base_path)
shutil.move(str(temp_path), str(base_path))
print("✅ LISTO.")
