import pandas as pd
import logging
import os
from config import FILE_ORIGEN

ENCODING = 'latin-1'
SEP = '»'
BASE_CHUNK = 25000  # Unidad mínima de lectura (todas las estrategias son múltiplos de esta)


def inspect_file():
    """
    Inspección rápida: muestra cabecera, columnas y cuenta el total de registros.
    Retorna: total_filas (int)
    """
    if not os.path.exists(FILE_ORIGEN):
        logging.error(f"Error: No se encontró {FILE_ORIGEN}")
        raise FileNotFoundError(FILE_ORIGEN)

    logging.info("="*60)
    logging.info("----- INSPECCIÓN RÁPIDA (3.1)")

    df_sample = pd.read_csv(FILE_ORIGEN, sep=SEP, engine='python', nrows=5, encoding=ENCODING)

    logging.info("--- Contando registros totales (espere un momento)...")
    count = 0
    with open(FILE_ORIGEN, 'rb') as f:
        for line in f:
            count += 1
    total_filas = count - 1

    logging.info(f"* Total filas: {total_filas:,} | Columnas: {len(df_sample.columns)}")
    logging.info(f"* Primeras 5 filas:\n{df_sample.to_string(index=False)}")

    return total_filas


def stream_batches(plan):
    """
    Generador que lee el archivo UNA sola vez y produce DataFrames de
    tamaño variable según el plan de lotes.

    El archivo se lee en unidades de BASE_CHUNK filas y se acumula en un
    buffer hasta completar cada lote. El puntero del archivo siempre
    avanza en orden, garantizando que batch_id N siempre corresponda a
    las mismas filas del archivo (condición necesaria para idempotencia).

    plan  : list of (batch_id: int, size: int)
    Yields: (batch_id: int, df: pd.DataFrame)
    """
    reader = pd.read_csv(
        FILE_ORIGEN, sep=SEP, engine='python',
        chunksize=BASE_CHUNK, encoding=ENCODING
    )

    buffer_df = pd.DataFrame()
    chunk_iter = iter(reader)
    eof = False

    for batch_id, size in plan:
        if eof and buffer_df.empty:
            break

        # Acumular chunks hasta tener al menos 'size' filas disponibles
        while len(buffer_df) < size and not eof:
            try:
                new_chunk = next(chunk_iter)
                buffer_df = pd.concat([buffer_df, new_chunk], ignore_index=True)
            except StopIteration:
                eof = True
                break

        if buffer_df.empty:
            break

        # Entregar exactamente 'size' filas (o las que queden en el último lote)
        lote_df = buffer_df.iloc[:size].copy()
        buffer_df = buffer_df.iloc[size:].reset_index(drop=True)

        yield batch_id, lote_df