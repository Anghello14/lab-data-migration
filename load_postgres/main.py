# PROYECTO_ETL_LAB / main.py

# Forzar UTF-8 en consola Windows ANTES de cualquier import que use logging
import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

import gc
import time
import logging
import psutil
from src.extract import inspect_file, stream_batches
from src.transform import clean_data
from src.load import load_data, print_final_report, check_ram_limit, is_batch_processed

# Formato de logs profesional basado en tus requerimientos
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def build_plan(total_filas):
    """
    Construye la lista ordenada de lotes según la estrategia de carga:
      · 20 lotes de  25,000  →    500,000 registros
      · 15 lotes de 100,000  →  1,500,000 registros
      · 16 lotes de 250,000  →  4,000,000 registros
      · Lotes de   500,000   → resto del archivo (último puede ser menor)
    Retorna: list of (batch_id: int, size: int)
    """
    plan = []
    batch_id = 1

    for _ in range(20):
        plan.append((batch_id, 25_000));   batch_id += 1
    for _ in range(15):
        plan.append((batch_id, 100_000));  batch_id += 1
    for _ in range(16):
        plan.append((batch_id, 250_000));  batch_id += 1

    remaining = total_filas - (20 * 25_000 + 15 * 100_000 + 16 * 250_000)
    while remaining > 0:
        size = min(500_000, remaining)
        plan.append((batch_id, size))
        batch_id += 1
        remaining -= size

    return plan


def main():
    # 1. INSPECCIÓN Y CONTEO
    try:
        total_filas = inspect_file()
    except Exception as e:
        logging.error(f"Fallo en la lectura inicial: {e}")
        return

    # 2. PLAN DE LOTES
    plan = build_plan(total_filas)
    total_lotes = len(plan)
    lotes_500k  = total_lotes - 51  # 20 + 15 + 16 = 51 lotes fijos

    logging.info("\n" + "="*60)
    logging.info(f"----- PLAN DE CARGA: {total_lotes} lotes para {total_filas:,} registros")
    logging.info(f"  · 20 lotes de  25,000  → {20 * 25_000:>12,} registros")
    logging.info(f"  · 15 lotes de 100,000  → {15 * 100_000:>12,} registros")
    logging.info(f"  · 16 lotes de 250,000  → {16 * 250_000:>12,} registros")
    logging.info(f"  · {lotes_500k} lotes de 500,000  → resto del archivo")
    logging.info("="*60)

    resultados_finales = []
    inicio_total = time.time()
    total_cargados_exito = 0

    # 3. PIPELINE: stream → idempotencia → transform → load
    for batch_id, df_raw in stream_batches(plan):
        size     = len(df_raw)
        strategy = "execute_values" if size <= 100_000 else "copy"

        logging.info(f"\n----- LOTE #{batch_id}/{total_lotes} | {size:,} registros | {strategy.upper()}")

        # A. Idempotencia: el stream ya avanzó el puntero del archivo;
        #    solo saltamos la transformación y la carga.
        if is_batch_processed(batch_id):
            logging.info(f"----- LOTE #{batch_id} ya procesado anteriormente. Saltando...")
            del df_raw
            gc.collect()
            continue

        # B. Protección de Hardware
        check_ram_limit()

        # C. Transformación
        df_clean = clean_data(df_raw)
        del df_raw
        gc.collect()

        # D. Carga
        exitos, fallidos, segs, ram = load_data(
            df_clean,
            "lab_entrenamiento_maestra",
            batch_id=batch_id,
            strategy=strategy
        )
        del df_clean
        gc.collect()

        # E. Registro de resultados
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

    # 4. REPORTE FINAL
    print_final_report(total_cargados_exito, 0, time.time() - inicio_total, lotes_cargados=resultados_finales)


if __name__ == "__main__":
    main()