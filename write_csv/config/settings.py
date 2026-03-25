import os
from pathlib import Path

# Rutas del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_OUTPUT_DIR = BASE_DIR / "data_output"
LOGS_DIR = BASE_DIR / "logs"
SQLITE_DB_PATH = BASE_DIR / "extraccion" / "control_extraccion.db"

# Crear directorios si no existen
for folder in [DATA_OUTPUT_DIR, LOGS_DIR]:
    folder.mkdir(exist_ok=True)

# Configuración Oracle (Asegúrate de que coincidan con tu Docker)
ORACLE_USER = "hospital_admin"
ORACLE_PASS = "Hospital123"
ORACLE_HOST = "localhost"
ORACLE_PORT = "1522"
ORACLE_SERVICE = "XEPDB1"  # Cambiar a XEPDB1 si es necesario según tu prueba de DBeaver

# Cadena de conexión simplificada para oracledb (Modo Thin)
DSN = f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"