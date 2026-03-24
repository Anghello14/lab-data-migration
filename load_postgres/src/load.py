import io
import psycopg2
import logging
import time
import psutil
import sys
import re
from psycopg2.extras import execute_values
from config import PG_USER, PG_PASS, PG_HOST, PG_PORT, PG_DB

# Expresión regular para validar identificadores SQL (tablas/columnas).
# Solo permite minúsculas, números y guiones bajos para evitar Inyección SQL.
_IDENT_RE = re.compile(r'^[a-z_][a-z0-9_]*$')

def _validate_identifier(name: str, context: str):
    """
    Capa de seguridad: Valida que los nombres de tablas y columnas sean seguros.
    Lanza ValueError si detecta caracteres sospechosos.
    """
    if not _IDENT_RE.match(name):
        raise ValueError(f"Identificador SQL invalido en {context}: '{name}'")

def _connect():
    """
    Establece la conexion con PostgreSQL configurando el DateStyle en ISO.
    Esto garantiza que las fechas YYYY-MM-DD se inserten correctamente 
    independientemente de la configuracion regional del servidor de base de datos.
    """
    conn = psycopg2.connect(
        dbname=PG_DB, user=PG_USER, password=PG_PASS,
        host=PG_HOST, port=PG_PORT,
        options="-c datestyle=ISO"
    )
    return conn

def check_ram_limit():
    """
    Mecanismo de proteccion de hardware (Panic Button).
    Si el consumo de RAM del sistema supera el 85%, detiene el script 
    inmediatamente para evitar un bloqueo (crash) del servidor.
    """
    mem = psutil.virtual_memory()
    if mem.percent > 85:
        logging.error(f"----- PANIC BUTTON: RAM al {mem.percent}%. Deteniendo para proteger el sistema.")
        sys.exit(1) # Cierre de emergencia preventivo.

def is_batch_processed(batch_id):
    """
    Consulta la tabla 'control_lotes' para verificar si un lote especifico 
    ya fue cargado con exito. Pilar fundamental de la IDEMPOTENCIA.
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT estado FROM control_lotes WHERE batch_id = %s AND estado = 'FINALIZADO'", (batch_id,))
    result = cur.fetchone()
    cur.close()
    conn.close()
    return result is not None

def load_data(df, table_name, batch_id, strategy="execute_values"):
    """
    Orquestador de carga que soporta dos estrategias de insercion masiva:
    1. execute_values: Insercion optimizada estandar.
    2. COPY: Insercion binaria/archivo ultra rapida (ideal para lotes de >250k).
    """
    # Proteccion previa al procesamiento de cada lote.
    check_ram_limit()
    
    start_time = time.time()
    rows_to_process = len(df)
    conn = _connect()
    cur = conn.cursor()
    
    # Refuerzo de formato de fecha para la sesion actual.
    cur.execute("SET datestyle = 'ISO, YMD'")

    try:
        # Registro inicial del lote en la tabla de auditoria con estado 'PROCESANDO'.
        cur.execute("INSERT INTO control_lotes (batch_id, estado) VALUES (%s, 'PROCESANDO') ON CONFLICT (batch_id) DO NOTHING", (batch_id,))
        
        # Validacion de seguridad para tabla y nombres de columnas.
        _validate_identifier(table_name, 'table_name')
        for col in df.columns:
            _validate_identifier(col, f'columna {col}')

        # ESTRATEGIA A: Insercion por valores agrupados (execute_values).
        if strategy == "execute_values":
            query = f"INSERT INTO {table_name} ({','.join(df.columns)}) VALUES %s"
            execute_values(cur, query, [tuple(x) for x in df.values])
        
        # ESTRATEGIA B: Metodo de copia masiva (COPY FROM).
        # Convierte el DataFrame en un buffer de memoria (StringIO) y lo 'copia' directamente a la tabla.
        else: 
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False, sep='|')
            buffer.seek(0)
            cur.copy_from(buffer, table_name, sep='|', columns=df.columns.tolist())
        
        # Monitoreo de salud del sistema post-insercion.
        ram_porcentaje = psutil.virtual_memory().percent
        consumoram_str = f"{ram_porcentaje:.1f}%"

        # Finalizacion de transaccion: Actualiza el estado del lote y estadisticas de consumo.
        cur.execute(
            "UPDATE control_lotes SET estado = 'FINALIZADO', filas_cargadas = %s, consumoram = %s WHERE batch_id = %s",
            (rows_to_process, consumoram_str, batch_id)
        )
        conn.commit()
        
        duration = time.time() - start_time
        
        # Logs detallados para el reporte de benchmark solicitado.
        logging.info(f"----- [LOTE #{batch_id}] FINALIZADO EXITOSAMENTE")
        logging.info(f"----- Resumen Lote: Procesados: {rows_to_process} | Fallidos: 0")
        logging.info(f"----- Duracion: {duration:.2f} seg | Rendimiento: {int(rows_to_process/duration)} filas/seg")
        logging.info("-" * 60)
        
        # Retorna metricas al main.py para la tabla comparativa.
        return rows_to_process, 0, duration, psutil.Process().memory_info().rss / (1024 * 1024)
    
    except Exception as e:
        # En caso de cualquier error (red, esquema, etc.), se deshacen los cambios del lote actual.
        conn.rollback()
        logging.error(f"----- Error en lote #{batch_id}: {e}")
        return 0, rows_to_process, 0, 0
    finally:
        cur.close()
        conn.close()


def print_final_report(total_success, total_fail, total_time, lotes_cargados=None):
    """
    Genera el cierre visual del proceso con metricas acumuladas de rendimiento global.
    Utilizado para el entregable del Laboratorio de Datos.
    """
    logging.info("="*60)
    logging.info("---------------------- REPORTE FINAL DE MIGRACION ----------------------")
    logging.info("="*60)
    logging.info(f"----- TOTAL CARGADOS: {total_success:,} registros")
    logging.info(f"----- TOTAL RECHAZADOS: {total_fail:,} registros")
    logging.info(f"----- TIEMPO TOTAL: {total_time/60:.2f} minutos")
    logging.info(f"----- VELOCIDAD GLOBAL: {int(total_success/total_time) if total_time > 0 else 0} filas/seg")

    # Si se proporciona el desglose de lotes, se imprime la tabla comparativa detallada.
    if lotes_cargados:
        logging.info("-" * 60)
        logging.info(f"----- LOTES PROCESADOS: {len(lotes_cargados)} lote(s)")
        for i, lote in enumerate(lotes_cargados, start=1):
            logging.info(
                f"Lote {i}: {lote['Lote']} registros | "
                f"Estrategia: {lote['Estrategia']} | "
                f"Tiempo: {lote['Tiempo']} | "
                f"RAM consumida: {lote['RAM_pct']}"
            )

    if total_fail > 0:
        logging.warning("----- Motivo de rechazo: Ver logs de error superiores (Violacion de esquema o red)")
    
    logging.info("="*60)