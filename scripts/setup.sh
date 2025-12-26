#!/bin/bash
# Script de setup para el proyecto PySpark + Airflow
# Versión con extracción REAL desde Supabase

set -e  # Exit on error

echo "🚀 Iniciando setup del proyecto..."

# Colores para output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# 1. Verificar Python
echo -e "\n${YELLOW}1. Verificando Python...${NC}"
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Python 3 no encontrado. Instala Python 3.9+ primero.${NC}"
    exit 1
fi
python3 --version

# 2. Verificar Spark
echo -e "\n${YELLOW}2. Verificando Apache Spark...${NC}"
if ! command -v spark-submit &> /dev/null; then
    echo -e "${RED}❌ Spark no encontrado. Instala con: brew install apache-spark${NC}"
    exit 1
fi
spark-submit --version 2>&1 | head -n 1

# 3. Verificar Java
echo -e "\n${YELLOW}3. Verificando Java...${NC}"
if ! command -v java &> /dev/null; then
    echo -e "${RED}❌ Java no encontrado. Instala con: brew install openjdk@11${NC}"
    exit 1
fi
java -version 2>&1 | head -n 1

# 4. Crear entorno virtual
echo -e "\n${YELLOW}4. Creando entorno virtual...${NC}"
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "✅ Entorno virtual creado"
else
    echo "⚠️  Entorno virtual ya existe"
fi

# 5. Activar y actualizar pip
echo -e "\n${YELLOW}5. Actualizando pip...${NC}"
source venv/bin/activate
pip install --upgrade pip

# 6. Instalar dependencias
echo -e "\n${YELLOW}6. Instalando dependencias...${NC}"
pip install -r requirements.txt

# 7. Crear archivo .env
echo -e "\n${YELLOW}7. Configurando variables de entorno...${NC}"
if [ ! -f ".env" ]; then
    cp .env.example .env
    echo -e "${GREEN}✅ Archivo .env creado.${NC}"
    echo -e "${RED}⚠️  IMPORTANTE: Edita .env con tus credenciales de Supabase${NC}"
else
    echo "⚠️  Archivo .env ya existe"
fi

# 8. Crear directorios necesarios
echo -e "\n${YELLOW}8. Creando estructura de directorios...${NC}"
mkdir -p data/raw data/processed data/output logs
touch data/raw/.gitkeep data/processed/.gitkeep data/output/.gitkeep

# 9. Verificar credenciales de Supabase
echo -e "\n${YELLOW}9. Verificando configuración de Supabase...${NC}"
if grep -q "tu-proyecto.supabase.co" .env; then
    echo -e "${RED}⚠️  ADVERTENCIA: Debes configurar tus credenciales de Supabase en .env${NC}"
    echo -e "${YELLOW}   Edita el archivo .env y configura:${NC}"
    echo -e "   - SUPABASE_URL=https://tu-proyecto.supabase.co"
    echo -e "   - SUPABASE_KEY=tu-api-key"
    echo ""
    echo -e "${YELLOW}¿Quieres usar datos MOCK para testing? (s/n)${NC}"
    read -r use_mock
    
    if [[ "$use_mock" =~ ^[Ss]$ ]]; then
        echo -e "\n${YELLOW}Generando datos mock para testing...${NC}"
        python scripts/generate_mock_data.py
    else
        echo -e "${RED}Setup pausado. Configura .env y ejecuta de nuevo.${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}✅ Credenciales de Supabase configuradas${NC}"
    echo -e "\n${YELLOW}¿Quieres extraer datos REALES de Supabase ahora? (s/n)${NC}"
    read -r extract_now
    
    if [[ "$extract_now" =~ ^[Ss]$ ]]; then
        echo -e "\n${YELLOW}Extrayendo datos desde Supabase...${NC}"
        python scripts/extract_from_supabase.py
    else
        echo -e "${YELLOW}Puedes extraer datos más tarde con:${NC}"
        echo -e "   python scripts/extract_from_supabase.py"
    fi
fi

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}✅ Setup completado exitosamente${NC}"
echo -e "${GREEN}========================================${NC}"

echo -e "\n📝 Próximos pasos:"
echo "1. Verifica que .env tenga tus credenciales de Supabase"
echo "2. Activa el entorno virtual: source venv/bin/activate"
echo "3. Extrae datos (si no lo hiciste): python scripts/extract_from_supabase.py"
echo "4. Ejecuta el job PySpark:"
echo "   spark-submit --master local[*] jobs/transform_donations.py"
