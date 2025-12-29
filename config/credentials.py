"""
Gestión de credenciales para diferentes entornos.
Detecta automáticamente si estamos en desarrollo local o producción.
"""
import os
from pathlib import Path
from google.cloud import storage
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError


def get_gcs_client():
    """
    Crea cliente de GCS con credenciales apropiadas según el entorno.
    
    Estrategia de autenticación (en orden):
    1. GOOGLE_APPLICATION_CREDENTIALS (service account JSON)
    2. GCE Metadata Server (si estamos en VM de GCP)
    3. ADC local (~/.config/gcloud/application_default_credentials.json)
    
    Returns:
        storage.Client: Cliente autenticado de GCS
        
    Raises:
        DefaultCredentialsError: Si no encuentra credenciales válidas
    """
    try:
        # Intentar obtener credenciales (ADC automático)
        credentials, project = default()
        
        # Detectar tipo de credencial
        cred_type = type(credentials).__name__
        
        if 'ServiceAccount' in cred_type:
            print("🔐 Autenticación: Service Account (Producción)")
        elif 'ComputeEngine' in cred_type:
            print("🔐 Autenticación: GCE Metadata Server (VM)")
        else:
            print("🔐 Autenticación: ADC User Credentials (Desarrollo)")
        
        # Crear cliente
        client = storage.Client(credentials=credentials, project=project)
        
        print(f"✅ Proyecto GCP: {project}")
        
        return client
        
    except DefaultCredentialsError as e:
        print("❌ Error: No se encontraron credenciales válidas")
        print("\nOpciones:")
        print("1. Desarrollo local: gcloud auth application-default login")
        print("2. Producción: Configurar GOOGLE_APPLICATION_CREDENTIALS")
        print("3. VM de GCP: Asegurar que la VM tenga service account attached")
        raise


def get_bigquery_client():
    """
    Crea cliente de BigQuery con credenciales apropiadas.
    Usa la misma estrategia que get_gcs_client().
    
    Returns:
        bigquery.Client: Cliente autenticado de BigQuery
    """
    from google.cloud import bigquery
    
    try:
        credentials, project = default()
        client = bigquery.Client(credentials=credentials, project=project)
        return client
    except DefaultCredentialsError as e:
        print("❌ Error obteniendo credenciales para BigQuery")
        raise


def verify_credentials():
    """
    Verifica que las credenciales funcionen correctamente.
    Útil para debugging.
    
    Returns:
        dict: Información sobre las credenciales actuales
    """
    try:
        credentials, project = default()
        
        info = {
            "project_id": project,
            "credential_type": type(credentials).__name__,
            "has_token": hasattr(credentials, 'token'),
            "environment": os.getenv("ENV", "local")
        }
        
        # Intentar listar buckets (test de permisos)
        try:
            client = storage.Client(credentials=credentials, project=project)
            buckets = list(client.list_buckets(max_results=1))
            info["gcs_access"] = True
        except Exception as e:
            info["gcs_access"] = False
            info["gcs_error"] = str(e)
        
        return info
        
    except DefaultCredentialsError as e:
        return {
            "error": "No credentials found",
            "message": str(e)
        }


if __name__ == "__main__":
    """Test de credenciales"""
    print("\n" + "="*60)
    print("🔍 VERIFICACIÓN DE CREDENCIALES")
    print("="*60 + "\n")
    
    info = verify_credentials()
    
    for key, value in info.items():
        print(f"{key}: {value}")
    
    print("\n" + "="*60)
    
    if info.get("gcs_access"):
        print("✅ Credenciales válidas y funcionando")
    else:
        print("❌ Problema con las credenciales")
