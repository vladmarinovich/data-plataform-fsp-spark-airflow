#!/bin/bash
# Script para ejecutar el pipeline manualmente (sin Airflow)
# Útil para debugging rápido

set -e  # Exit on error

# Colores
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Variables
export ENV=local
FAILED_JOBS=()

echo -e "${YELLOW}🚀 PIPELINE MANUAL EXECUTION${NC}"
echo "======================================"

# Función para ejecutar un job
run_job() {
    local job_path=$1
    local job_name=$(basename "$job_path" .py)
    
    echo -e "\n${YELLOW}► Ejecutando: $job_name${NC}"
    
    if timeout 60 python3 "$job_path" 2>&1 | tee /tmp/pipeline_${job_name}.log | tail -5; then
        echo -e "${GREEN}✅ $job_name - SUCCESS${NC}"
        return 0
    else
        echo -e "${RED}❌ $job_name - FAILED${NC}"
        FAILED_JOBS+=("$job_name")
        echo "Log guardado en: /tmp/pipeline_${job_name}.log"
        return 1
    fi
}

# ==============================================================================
# CAPA SILVER
# ==============================================================================
echo -e "\n${YELLOW}═══════════════════════════════════════${NC}"
echo -e "${YELLOW}  CAPA SILVER (Limpieza de datos)${NC}"
echo -e "${YELLOW}═══════════════════════════════════════${NC}"

SILVER_JOBS=(
    "jobs/silver/donantes.py"
    "jobs/silver/casos.py"
    "jobs/silver/donaciones.py"
    "jobs/silver/gastos.py"
    "jobs/silver/proveedores.py"
    "jobs/silver/hogar_de_paso.py"
)

for job in "${SILVER_JOBS[@]}"; do
    run_job "$job" || true  # Continue even if fails
done

# ==============================================================================
# CAPA GOLD - DIMENSIONS
# ==============================================================================
echo -e "\n${YELLOW}═══════════════════════════════════════${NC}"
echo -e "${YELLOW}  CAPA GOLD - DIMENSIONS${NC}"
echo -e "${YELLOW}═══════════════════════════════════════${NC}"

GOLD_DIM_JOBS=(
    "jobs/gold/dim_calendario.py"
    "jobs/gold/dim_donantes.py"
    "jobs/gold/dim_casos.py"
    "jobs/gold/dim_proveedores.py"
    "jobs/gold/dim_hogar_de_paso.py"
)

for job in "${GOLD_DIM_JOBS[@]}"; do
    run_job "$job" || true
done

# ==============================================================================
# CAPA GOLD - FACTS
# ==============================================================================
echo -e "\n${YELLOW}═══════════════════════════════════════${NC}"
echo -e "${YELLOW}  CAPA GOLD - FACTS${NC}"
echo -e "${YELLOW}═══════════════════════════════════════${NC}"

GOLD_FACT_JOBS=(
    "jobs/gold/fact_donaciones.py"
    "jobs/gold/fact_gastos.py"
)

for job in "${GOLD_FACT_JOBS[@]}"; do
    run_job "$job" || true
done

# ==============================================================================
# CAPA GOLD - FEATURES
# ==============================================================================
echo -e "\n${YELLOW}═══════════════════════════════════════${NC}"
echo -e "${YELLOW}  CAPA GOLD - FEATURES${NC}"
echo -e "${YELLOW}═══════════════════════════════════════${NC}"

GOLD_FEAT_JOBS=(
    "jobs/gold/feat_donantes.py"
    "jobs/gold/feat_casos.py"
    "jobs/gold/feat_proveedores.py"
)

for job in "${GOLD_FEAT_JOBS[@]}"; do
    run_job "$job" || true
done

# ==============================================================================
# CAPA GOLD - DASHBOARDS
# ==============================================================================
echo -e "\n${YELLOW}═══════════════════════════════════════${NC}"
echo -e "${YELLOW}  CAPA GOLD - DASHBOARDS${NC}"
echo -e "${YELLOW}═══════════════════════════════════════${NC}"

GOLD_DASH_JOBS=(
    "jobs/gold/dashboard_donaciones.py"
    "jobs/gold/dashboard_gastos.py"
    "jobs/gold/dashboard_financiero.py"
)

for job in "${GOLD_DASH_JOBS[@]}"; do
    run_job "$job" || true
done

# ==============================================================================
# RESUMEN FINAL
# ==============================================================================
echo -e "\n${YELLOW}═══════════════════════════════════════${NC}"
echo -e "${YELLOW}  RESUMEN DE EJECUCIÓN${NC}"
echo -e "${YELLOW}═══════════════════════════════════════${NC}"

if [ ${#FAILED_JOBS[@]} -eq 0 ]; then
    echo -e "${GREEN}🎉 TODOS LOS JOBS EJECUTADOS EXITOSAMENTE${NC}"
    exit 0
else
    echo -e "${RED}❌ Jobs fallidos (${#FAILED_JOBS[@]}):${NC}"
    for job in "${FAILED_JOBS[@]}"; do
        echo -e "  - ${RED}$job${NC}"
        echo "    Log: /tmp/pipeline_${job}.log"
    done
    exit 1
fi
