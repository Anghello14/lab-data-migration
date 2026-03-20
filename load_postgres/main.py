# Configuracion critica para terminales Windows: fuerza la salida en UTF-8 para soportar caracteres especiales y acentos en los logs sin errores de codificacion.
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

# Formato de logs profesional: Incluye marca de tiempo, nivel de severidad y el mensaje.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def build_plan(total_filas):
    """
    ESTRATEGIA DE BENCHMARK:
    Construye una hoja de ruta detallada para procesar el archivo en bloques variables.
    Permite comparar el rendimiento de diferentes tamaños de lote y metodos de carga.
    """
    plan = []
    batch_id = 1

    # Fase 1: Pruebas de carga ligera (Lotes de 25k)
    for _ in range(20):
        plan.append((batch_id, 25_000));   batch_id += 1
    # Fase 2: Pruebas de carga media (Lotes de 100k)
    for _ in range(15):
        plan.append((batch_id, 100_000));  batch_id += 1
    # Fase 3: Pruebas de carga pesada (Lotes de 250k - Cambio a estrategia COPY sugerido)
    for _ in range(16):
        plan.append((batch_id, 250_000));  batch_id += 1

    # Fase 4: Procesamiento del resto del archivo en bloques maximos de 500k.
    remaining = total_filas - (20 * 25_000 + 15 * 100_000 + 16 * 250_000)
    while remaining > 0:
        size = min(500_000, remaining)
        plan.append((batch_id, size))
        batch_id += 1
        remaining -= size

    return plan


def main():
    """
    Funcion Principal: Orquestador del Pipeline ETL.
    Coordina la Inspeccion, Extraccion, Transformacion y Carga (E-T-L).
    """
    # 1. INSPECCIÓN Y CONTEO INICIAL
    try:
        total_filas = inspect_file()
    except Exception as e:
        logging.error(f"Fallo en la lectura inicial: {e}")
        return

    # 2. GENERACION DEL PLAN DE EJECUCION
    plan = build_plan(total_filas)
    total_lotes = len(plan)
    lotes_500k  = total_lotes - 51  # Calculo para el desglose visual del reporte.

    logging.info("\n" + "="*60)
    logging.info(f"----- PLAN DE CARGA: {total_lotes} lotes para {total_filas:,} registros")
    logging.info(f"  - 20 lotes de  25,000  = {20 * 25_000:>12,} registros")
    logging.info(f"  - 15 lotes de 100,000  = {15 * 100_000:>12,} registros")
    logging.info(f"  - 16 lotes de 250,000  = {16 * 250_000:>12,} registros")
    logging.info(f"  - {lotes_500k} lotes de 500,000  =  resto del archivo")
    logging.info("="*60)

    resultados_finales = []
    inicio_total = time.time()
    total_cargados_exito = 0

    # 3. PIPELINE RECURSIVO: Se procesa lote por lote para optimizar memoria RAM.
    for batch_id, df_raw in stream_batches(plan):
        size     = len(df_raw)
        # Seleccion inteligente de estrategia: 'execute_values' para volumenes pequeños,
        # 'copy' para volumenes grandes donde el rendimiento es critico.
        strategy = "execute_values" if size <= 100_000 else "copy"

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