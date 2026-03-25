import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

import gc
import os
import time
import logging
import psutil
from datetime import datetime
from src.extract import inspect_file, stream_batches
from src.transform import clean_data
from src.load import load_data, print_final_report, check_ram_limit, is_batch_processed

# Formato de logs profesional: Incluye marca de tiempo, nivel de severidad y el mensaje.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BATCH_SIZE = 250_000


def build_plan(total_filas):
    """
    Genera el plan de carga uniforme: lotes de 250,000 filas con metodo COPY.
    El ultimo lote puede ser menor si el total de filas no es multiplo exacto.
    """
    plan = []
    batch_id = 1
    remaining = total_filas
    while remaining > 0:
        size = min(BATCH_SIZE, remaining)
        plan.append((batch_id, size))
        batch_id += 1
        remaining -= size
    return plan


def main():
    """
    Funcion Principal: Orquestador del Pipeline ETL.
    Coordina la Inspeccion, Extraccion, Transformacion y Carga (E-T-L).
    """
    # 0. CONFIGURACION DE LOGGING A ARCHIVO
    # Cada ejecucion genera un archivo de log independiente con sello de fecha/hora.
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_path = os.path.join(log_dir, f"pipeline_{timestamp}.log")
    fh = logging.FileHandler(log_path, encoding='utf-8')
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logging.getLogger().addHandler(fh)
    logging.info(f"----- Log de sesion iniciado: {log_path}")

    # 1. INSPECCIÓN Y CONTEO INICIAL
    try:
        total_filas = inspect_file()
    except Exception as e:
        logging.error(f"Fallo en la lectura inicial: {e}")
        return

    # 2. GENERACION DEL PLAN DE EJECUCION
    plan = build_plan(total_filas)
    total_lotes = len(plan)
    lotes_completos = sum(1 for _, s in plan if s == BATCH_SIZE)
    lotes_parciales = total_lotes - lotes_completos

    logging.info("\n" + "="*60)
    logging.info(f"----- PLAN DE CARGA: {total_lotes} lotes para {total_filas:,} registros (Estrategia: COPY)")
    logging.info(f"  - {lotes_completos} lotes de {BATCH_SIZE:,} = {lotes_completos * BATCH_SIZE:>12,} registros")
    if lotes_parciales:
        ultimo_size = plan[-1][1]
        logging.info(f"  - 1 lote  parcial de {ultimo_size:>9,} = {ultimo_size:>12,} registros (ultimo lote)")
    logging.info("="*60)

    resultados_finales = []
    inicio_total = time.time()
    total_cargados_exito = 0

    # 3. PIPELINE RECURSIVO: Se procesa lote por lote para optimizar memoria RAM.
    for batch_id, df_raw in stream_batches(plan):
        size     = len(df_raw)
        strategy = "copy"  # Unica estrategia: COPY para todos los lotes.

        logging.info(f"\n----- LOTE #{batch_id}/{total_lotes} | {size:,} registros | {strategy.upper()}")

        # A. IDEMPOTENCIA: Verifica si el lote ya existe en 'control_lotes' con estado 'FINALIZADO'.
        # Si es asi, se libera la memoria del lote crudo y se salta al siguiente.
        if is_batch_processed(batch_id):
            logging.info(f"----- LOTE #{batch_id} ya procesado anteriormente. Saltando...")
            del df_raw
            gc.collect() # Forzamos la limpieza de RAM.
            continue

        # B. PROTECCION DE HARDWARE: Valida que el sistema tenga recursos antes de transformar.
        check_ram_limit()

        # C. TRANSFORMACION: Limpieza y normalizacion de los datos del lote.
        df_clean = clean_data(df_raw)
        del df_raw       # Eliminamos el DataFrame crudo inmediatamente para ahorrar espacio.
        gc.collect()

        # D. CARGA: Insercion masiva en PostgreSQL.
        exitos, fallidos, segs, ram = load_data(
            df_clean,
            "lab_entrenamiento_maestra",
            batch_id=batch_id,
            strategy=strategy
        )
        del df_clean    # Liberamos el DataFrame limpio tras la carga exitosa.
        gc.collect()

        # E. REGISTRO DE METRICAS: Acumula datos para el reporte comparativo final.
        if exitos > 0:
            total_cargados_exito += exitos
            rendimiento = int(exitos / segs) if segs > 0 else 0
            resultados_finales.append({
                "Lote":       f"{size:,}",
                "Estrategia": strategy,
                "Tiempo":     f"{segs:.2f}s",
                "Rendimiento":f"{rendimiento:,} f/s",
                "RAM":        f"{ram:.2f} MB",
                "RAM_pct":    f"{psutil.virtual_memory().percent:.1f}%"
            })

    # 4. CIERRE Y REPORTE FINAL DE BENCHMARK
    print_final_report(total_cargados_exito, 0, time.time() - inicio_total, lotes_cargados=resultados_finales)


if __name__ == "__main__":
    main()