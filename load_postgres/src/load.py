import pandas as pd
import io
import psycopg2
import logging
import time
import psutil
import sys
from psycopg2.extras import execute_values
from config import PG_USER, PG_PASS, PG_HOST, PG_PORT, PG_DB

def check_ram_limit():
    """Verifica si la RAM supera el 85% y detiene el proceso si es necesario."""
    mem = psutil.virtual_memory()
    if mem.percent > 85:
        logging.error(f"----- PANIC BUTTON: RAM al {mem.percent}%. Deteniendo para proteger el sistema.")
        sys.exit(1) # Cierre de emergencia

def is_batch_processed(batch_id):
    """Revisa en Postgres si el lote ya fue cargado exitosamente."""
    conn = psycopg2.connect(dbname=PG_DB, user=PG_USER, password=PG_PASS, host=PG_HOST, port=PG_PORT)
    cur = conn.cursor()
    cur.execute("SELECT estado FROM control_lotes WHERE batch_id = %s AND estado = 'FINALIZADO'", (batch_id,))
    result = cur.fetchone()
    cur.close()
    conn.close()
    return result is not None

def load_data(df, table_name, batch_id, strategy="execute_values"):
    # 1. Verificación de Idempotencia
    if is_batch_processed(batch_id):
        logging.info(f"----- LOTE #{batch_id} ya fue cargado anteriormente. Saltando...")
        return len(df), 0, 0, 0

    # 2. Protección de Hardware
    check_ram_limit()
    
    start_time = time.time()
    rows_to_process = len(df)
    conn = psycopg2.connect(dbname=PG_DB, user=PG_USER, password=PG_PASS, host=PG_HOST, port=PG_PORT)
    cur = conn.cursor()
    
    try:
        # Registrar inicio en tabla de control
        cur.execute("INSERT INTO control_lotes (batch_id, estado) VALUES (%s, 'PROCESANDO') ON CONFLICT (batch_id) DO NOTHING", (batch_id,))
        
        if strategy == "execute_values":
            query = f"INSERT INTO {table_name} ({','.join(df.columns)}) VALUES %s"
            execute_values(cur, query, [tuple(x) for x in df.values])
        else: # Estrategia COPY
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False, sep='|')
            buffer.seek(0)
            cur.copy_from(buffer, table_name, sep='|', columns=df.columns.tolist())
        
        # Capturar RAM justo después de la carga, antes del commit
        ram_porcentaje = psutil.virtual_memory().percent
        consumoram_str = f"{ram_porcentaje:.1f}%"

        # Marcar como finalizado incluyendo el consumo de RAM del lote
        cur.execute(
            "UPDATE control_lotes SET estado = 'FINALIZADO', filas_cargadas = %s, consumoram = %s WHERE batch_id = %s",
            (rows_to_process, consumoram_str, batch_id)
        )
        conn.commit()
        
        duration = time.time() - start_time
        
        # LOGS ESTRUCTURADOS 
        logging.info(f"----- [LOTE #{batch_id}] FINALIZADO EXITOSAMENTE")
        logging.info(f"----- Resumen Lote: Procesados: {rows_to_process} | Fallidos: 0")
        logging.info(f"----- Duracion: {duration:.2f} seg | Rendimiento: {int(rows_to_process/duration)} filas/seg")
        logging.info("-" * 60)
        
        return rows_to_process, 0, duration, psutil.Process().memory_info().rss / (1024 * 1024)
    
    except Exception as e:
        conn.rollback()
        logging.error(f"----- Error en lote #{batch_id}: {e}")
        return 0, rows_to_process, 0, 0
    finally:
        cur.close()
        conn.close()


def print_final_report(total_success, total_fail, total_time, lotes_cargados=None):
    logging.info("="*60)
    logging.info("---------------------- REPORTE FINAL DE MIGRACION ----------------------")
    logging.info("="*60)
    logging.info(f"----- TOTAL CARGADOS: {total_success:,} registros")
    logging.info(f"----- TOTAL RECHAZADOS: {total_fail:,} registros")
    logging.info(f"----- TIEMPO TOTAL: {total_time/60:.2f} minutos")
    logging.info(f"----- VELOCIDAD GLOBAL: {int(total_success/total_time) if total_time > 0 else 0} filas/seg")

    if lotes_cargados:
        logging.info("-" * 60)
        logging.info(f"----- LOTES PROCESADOS: {len(lotes_cargados)} lote(s)")
        for i, lote in enumerate(lotes_cargados, start=1):
            logging.info(
                f"  Lote {i}: {lote['Lote']} registros | "
                f"Estrategia: {lote['Estrategia']} | "
                f"Tiempo: {lote['Tiempo']} | "
                f"RAM consumida: {lote['RAM_pct']}"
            )

    if total_fail > 0:
        logging.warning("----- Motivo de rechazo: Ver logs de error superiores (Violacion de esquema o red)")
    
    logging.info("="*60)