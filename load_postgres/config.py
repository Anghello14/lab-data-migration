import logging
from sqlalchemy import create_engine

# --- CONFIGURACIÓN DE LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- PARÁMETROS DE CONEXIÓN (POSTGRESQL) ---
PG_USER = "postgres"
PG_PASS = "Jajaja123"
PG_HOST = "localhost"
PG_PORT = "5433"
PG_DB   = "postgres"

# --- CONFIGURACIÓN DE TABLA ÚNICA ---
# Ahora todo va a una sola tabla como solicitaste
TABLE_DESTINO = "lab_entrenamiento_maestra"

# --- MENSAJES ESTÁNDAR PARA DATOS ERRÓNEOS ---
# Estos son los valores que insertaremos cuando la validación falle
MSG_ERR_TEXTO  = "DATO_INVALIDO"
MSG_ERR_NUMERO = -1
MSG_ERR_FECHA  = "1930-01-01"

# --- MOTOR DE BASE DE DATOS ---
PG_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_ENGINE = create_engine(PG_URL, pool_pre_ping=True)

# --- CONFIGURACIÓN DEL ARCHIVO ORIGEN ---
FILE_ORIGEN = "data/registros.txt"