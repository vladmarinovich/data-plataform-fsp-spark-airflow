"""
Re-extrae TODOS los donantes desde Supabase (sin filtro Q1).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from supabase import create_client
import config

# Conectar a Supabase
supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

print("🚀 Extrayendo TODOS los donantes desde Supabase...")

# Extraer con paginación
all_data = []
offset = 0
batch_size = 1000

while True:
    response = supabase.table("donantes").select("*").range(offset, offset + batch_size - 1).execute()
    data = response.data
    
    if not data:
        break
        
    all_data.extend(data)
    offset += batch_size
    print(f"   Extraídos {len(all_data)} registros...")
    
    if len(data) < batch_size:
        break

print(f"✅ Total donantes extraídos: {len(all_data)}")

# Convertir a DataFrame
df = pd.DataFrame(all_data)

# Escribir a Bronze
output_path = Path(config.RAW_PATH) / "donantes"
output_path.mkdir(parents=True, exist_ok=True)

# Particionar por last_modified_at
df['last_modified_at_dt'] = pd.to_datetime(df['last_modified_at'])
df['y'] = df['last_modified_at_dt'].dt.strftime('%Y')
df['m'] = df['last_modified_at_dt'].dt.strftime('%m')
df = df.drop(columns=['last_modified_at_dt'])

# Escribir particionado
table = pa.Table.from_pandas(df)
pq.write_to_dataset(
    table,
    root_path=str(output_path),
    partition_cols=['y', 'm']
)

print(f"✅ Donantes escritos en {output_path}")
