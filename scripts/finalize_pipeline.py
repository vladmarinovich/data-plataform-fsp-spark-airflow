"""
Script de finalización del Pipeline.
Actualiza TODOS los watermarks a la fecha/hora actual (NOW).
Debe ejecutarse SOLO si todo el DAG (Raw -> Silver -> Gold) fue exitoso.
"""
import sys
import json
from pathlib import Path
from datetime import datetime, timezone

# Agregar raíz al path
sys.path.insert(0, str(Path(__file__).parent.parent))

import config

# Tablas que gestionamos
MANAGED_TABLES = ["donaciones", "gastos", "donantes", "casos", "proveedores", "hogar_de_paso"]
PIPELINE_MARKERS = ["spdp_data_platform_main"] # Marcador global si se usa

def finalize_watermarks():
    now_ts = datetime.now(timezone.utc).isoformat()
    print(f"🏁 FINALIZANDO PIPELINE")
    print(f"⏰ Timestamp de cierre: {now_ts}")

    if config.ENV == "local":
        watermark_file = config.PROJECT_ROOT / "watermarks.json"
        data = {}
        
        # Cargar existente para no borrar otras cosas si las hay
        if watermark_file.exists():
            try:
                with open(watermark_file, 'r') as f:
                    data = json.load(f)
            except:
                data = {}
        
        # Actualizar TODAS las tablas al Timestamp actual
        for table in MANAGED_TABLES:
            data[table] = {
                "watermark": now_ts,
                "updated_at": now_ts
            }
            # También actualizar keys 'silver_' por si acaso en el futuro las separamos
            # data[f"silver_{table}"] = ... (Opcional, por ahora usamos mismo key)
            
        # Actualizar global
        for markers in PIPELINE_MARKERS:
             data[markers] = {
                "watermark": now_ts,
                "updated_at": now_ts
            }

        with open(watermark_file, 'w') as f:
            json.dump(data, f, indent=2)
            
        print(f"✅ Watermarks locales actualizados a {now_ts}")
        
    else:
        # Prod (BigQuery)
        # Aquí iría lógica BQ si tuvieramos la librería, pero en este mockup imprimimos
        print("⚠️  Modo PROD: Ejecutar MERGE en BigQuery para actualizar spdp_control.pipeline_watermark")
        # TODO: Implementar BQ Update real

if __name__ == "__main__":
    finalize_watermarks()
