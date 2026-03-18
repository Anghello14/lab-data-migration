import pandas as pd
import logging
import os
from typing import Generator, Tuple
from config import FILE_ORIGEN # Ruta definida en config.py

def get_text_data(chunk_size: int) -> Tuple[Generator[pd.DataFrame, None, None], int]:
    # Realiza una inspección profunda y profesional del archivo fuente y retorna un generador para el procesamiento por lotes.
    
    # --- 1. VALIDACIÓN DE EXISTENCIA ---
    if not os.path.exists(FILE_ORIGEN):
        logging.error(f"----- ERROR CRÍTICO: No existe el archivo en {FILE_ORIGEN}")
        raise FileNotFoundError(f"Archivo faltante: {FILE_ORIGEN}")

    logging.info("="*50)
    logging.info(f"----- INICIANDO INSPECCIÓN DE FUENTE: {FILE_ORIGEN}")
    logging.info("="*50)

    try:
        # --- 2. PERFILAMIENTO INICIAL (Muestra representativa) ---
        # Leemos solo 500 filas para inferir tipos y nulos sin agotar la RAM
        df_sample = pd.read_csv(
            FILE_ORIGEN, 
            sep='»', 
            engine='python', 
            nrows=500, 
            encoding='ISO-8859-1'
        )
        
        # A. Conteo de Filas (Eficiente) y Columnas
        with open(FILE_ORIGEN, 'r', encoding='ISO-8859-1') as f:
            total_filas = sum(1 for line in f) - 1
        total_columnas = len(df_sample.columns)
        
        logging.info(f"----- DIMENSIONES: {total_filas} filas | {total_columnas} columnas")

        # B. Tipos de Datos Inferidos
        logging.info("----- TIPOS DE DATOS DETECTADOS (Muestra):")
        for col, dtype in df_sample.dtypes.items():
            logging.info(f"   - {col}: {dtype}")

        # C. Análisis de Nulidad (Porcentaje)
        logging.info("----- ANÁLISIS DE HUECOS (NULOS %):")
        null_pct = (df_sample.isnull().sum() / len(df_sample)) * 100
        for col, pct in null_pct.items():
            if pct > 0:
                logging.warning(f"   - {col}: {pct:.2f}% de nulos detectados")

        # D. Inspección de Anomalías (Top & Bottom) 
        logging.info("----- PRIMEROS REGISTROS (Head):")
        logging.info(f"\n{df_sample.head(2).to_string(index=False)}")
        
        # Para las últimas filas, leemos solo el final del archivo
        df_tail = pd.read_csv(
            FILE_ORIGEN, sep='»', engine='python', 
            skiprows=range(1, total_filas - 1), encoding='ISO-8859-1'
        )
        logging.info("----- ÚLTIMOS REGISTROS (Tail):")
        logging.info(f"\n{df_tail.to_string(index=False)}")

    except Exception as e:
        logging.error(f"----- ERROR DURANTE LA INSPECCIÓN: {e}")
        raise

    logging.info("="*50)
    logging.info(f"----- INSPECCIÓN FINALIZADA. INICIANDO EXTRACCIÓN POR LOTES ({chunk_size})")
    logging.info("="*50)

    # --- 3. GENERADOR DE DATOS (Estrategia Batch Loading) ---
    def data_generator():
        try:
            reader = pd.read_csv(
                FILE_ORIGEN,
                sep='»',
                engine='python',
                chunksize=chunk_size, 
                encoding='ISO-8859-1',
                on_bad_lines='warn'
            )
            
            for i, chunk in enumerate(reader):
                logging.info(f"----- LOTE #{i+1}: Procesando {len(chunk)} registros...")
                yield chunk
                
        except Exception as e:
            logging.error(f"----- FALLO CRÍTICO EN EXTRACCIÓN: {e}")
            raise

    return data_generator(), total_filas