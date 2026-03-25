import oracledb
from config.settings import ORACLE_USER, ORACLE_PASS, DSN

try:
    conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASS, dsn=DSN)
    print(f"Conexion exitosa. Version de Oracle: {conn.version}")
    conn.close()
except Exception as e:
    print(f"Error de conexion: {e}")