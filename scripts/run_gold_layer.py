"""
Orquestador de Capa Gold.
Ejecuta los jobs Gold en orden, forzando la lectura/escritura en GCS incluso si corre localmente.
"""
import sys
import os
from pathlib import Path

# Agregar raíz
sys.path.insert(0, str(Path(__file__).parent.parent))

import config

# SOBRESCRIBIR PATHS PARA USAR GCS
# BUCKET = "gs://salvando-patitas-spark"
# print(f"🌍 FORZANDO MODO CLOUD STORAGE: {BUCKET}")

# config.SILVER_PATH = f"{BUCKET}/silver"
# config.GOLD_PATH = f"{BUCKET}/gold"
# config.QUARANTINE_PATH = f"{BUCKET}/quarantine"
# config.RAW_PATH = f"{BUCKET}/raw"

# Importar Jobs (ahora usarán los paths modificados)
from jobs.gold.dim_donantes import run_gold_dim_donantes
from jobs.gold.dim_proveedores import run_gold_dim_proveedores
from jobs.gold.dim_hogar_de_paso import run_gold_dim_hogar
from jobs.gold.dim_casos import run_gold_dim_casos
from jobs.gold.dim_calendario import run_dim_calendario
from jobs.gold.fact_donaciones import run_gold_fact_donaciones
from jobs.gold.fact_gastos import run_gold_fact_gastos
from jobs.gold.dashboard_donaciones import run_gold_dashboard_donaciones
from jobs.gold.dashboard_gastos import run_gold_dashboard_gastos
from jobs.gold.dashboard_financiero import run_gold_dashboard_financiero

def main():
    print("\n🏆 INICIANDO EJECUCIÓN CAPA GOLD (GCS MODE)\n")
    
    # 1. Dimensiones (Maestros)
    run_dim_calendario()
    run_gold_dim_donantes()
    run_gold_dim_proveedores()
    run_gold_dim_hogar()
    run_gold_dim_casos()
    
    # 2. Hechos (Transaccionales)
    run_gold_fact_donaciones()
    run_gold_fact_gastos()
    
    # 3. Agregados (Dashboards)
    run_gold_dashboard_donaciones()
    run_gold_dashboard_gastos()
    
    # 4. Final Boss (Dashboard Financiero)
    run_gold_dashboard_financiero()
    
    print("\n✨ CAPA GOLD FINALIZADA EXITOSAMENTE ✨")

if __name__ == "__main__":
    # Asegurar credenciales para Spark
    adc_path = os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = adc_path
    
    main()
