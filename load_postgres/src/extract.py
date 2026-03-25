import pandas as pd
import logging
import os
from config import FILE_ORIGEN

ENCODING = 'latin-1'

SEP = '»'

# Constante de granularidad: define el bloque mínimo de lectura para optimizar el uso de buffer.
BASE_CHUNK = 25000  # Unidad mínima de lectura (todas las estrategias son múltiplos de esta)


def inspect_file():
    """
    Realiza un análisis preliminar del archivo de origen para validar su existencia,
    estructura de columnas y volumen total de datos antes de iniciar el proceso ETL.
    """
    
    # Validación de seguridad: verifica que el archivo de 4GB esté presente en la ruta configurada.
    if not os.path.exists(FILE_ORIGEN):
        logging.error(f"Error: No se encontró {FILE_ORIGEN}")
        raise FileNotFoundError(FILE_ORIGEN)

    logging.info("="*60)
    logging.info("----- INSPECCION RAPIDA (3.1)")

    # Lectura parcial (nrows=5) para obtener metadatos y nombres de columnas sin cargar el archivo en RAM.
    df_sample = pd.read_csv(FILE_ORIGEN, sep=SEP, engine='python', nrows=5, encoding=ENCODING)

    logging.info("--- Contando registros totales (espere un momento)...")
    
    # Conteo eficiente de filas mediante lectura de flujo binario ('rb').
    # Este método es mucho más rápido y ligero en memoria que cargar el archivo con Pandas.
    count = 0
    with open(FILE_ORIGEN, 'rb') as f:
        for line in f:
            count += 1
    
    # Se resta 1 para excluir la fila de encabezados del total de registros.
    total_filas = count - 1

    # Análisis de calidad: lectura con dtype=str para distinguir vacíos de NaN textuales.
    df_analysis = pd.read_csv(
        FILE_ORIGEN, sep=SEP, engine='python', nrows=5,
        encoding=ENCODING, dtype=str, keep_default_na=False
    )
    total_cells = df_analysis.size
    # Campos nulos: celdas vacías o representaciones explícitas de ausencia de valor.
    null_values = {'', 'null', 'none', 'sin dato'}
    null_count = df_analysis.apply(lambda col: col.str.lower().str.strip().isin(null_values)).sum().sum()
    # Campos NaN: celdas cuyo valor textual es alguna variante de 'nan' / 'na'.
    nan_values  = {'NaN', 'nan', 'n/a', 'nan%'}
    nan_count   = df_analysis.apply(lambda col: col.str.lower().str.strip().isin(nan_values)).sum().sum()
    null_pct = (null_count / total_cells) * 100
    nan_pct  = (nan_count  / total_cells) * 100

    # Reporte de diagnóstico inicial detallando dimensiones del dataset.
    logging.info(f"* Total filas: {total_filas:,} | Columnas: {len(df_sample.columns)}")
    logging.info(f"* Calidad (primeras 5 filas): Nulos={null_pct:.1f}% ({null_count}/{total_cells} celdas) | NaN={nan_pct:.1f}% ({nan_count}/{total_cells} celdas)")
    logging.info(f"* Primeras 5 filas:\n{df_sample.to_string(index=False)}")

    return total_filas


def stream_batches(plan):
    """
    Generador de alto rendimiento que implementa la estrategia de 'Streaming'.
    Lee el archivo una sola vez y distribuye los datos en lotes (batches) de tamaño dinámico
    según el plan de pruebas definido para el benchmark de rendimiento.
    """
    
    # Configuración del lector por trozos (chunks). 
    # dtype=str asegura que no haya inferencia de tipos incorrecta durante la extracción inicial.
    reader = pd.read_csv(
        FILE_ORIGEN, sep=SEP, engine='python',
        chunksize=BASE_CHUNK, encoding=ENCODING,
        dtype=str        # garantiza que TODOS los valores lleguen como str exacto
    )

    # Inicialización del buffer de memoria para acumular datos y el iterador del archivo.
    buffer_df = pd.DataFrame()
    chunk_iter = iter(reader)
    eof = False # Bandera de fin de archivo (End Of File)

    # Itera sobre el plan de ejecución (ID del lote y tamaño solicitado).
    for batch_id, size in plan:
        # Si el archivo terminó y el buffer está vacío, se cierra el generador.
        if eof and buffer_df.empty:
            break

        # Lógica de acumulación en buffer: Mientras el buffer tenga menos filas de las solicitadas por el lote actual, se siguen extrayendo 'BASE_CHUNK' filas del archivo original.
        while len(buffer_df) < size and not eof:
            try:
                new_chunk = next(chunk_iter)
                # Concatenación eficiente de nuevos datos al buffer existente.
                buffer_df = pd.concat([buffer_df, new_chunk], ignore_index=True)
            except StopIteration:
                # Se activa al llegar al final del archivo físico.
                eof = True
                break

        # Validación final del buffer antes de la entrega.
        if buffer_df.empty:
            break

        # Segmentación exacta: Se extrae del buffer únicamente la cantidad de filas requerida por el test actual.
        lote_df = buffer_df.iloc[:size].copy()
        
        # Limpieza del buffer: se eliminan las filas entregadas para liberar memoria RAM.
        buffer_df = buffer_df.iloc[size:].reset_index(drop=True)

        # Entrega el lote procesado al orquestador (main.py) manteniendo el batch_id para control.
        yield batch_id, lote_df