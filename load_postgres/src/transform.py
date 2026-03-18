import pandas as pd
import logging
import re
from config import (
    MSG_ERR_TEXTO, MSG_ERR_NUMERO, MSG_ERR_FECHA, 
    MSG_NULL, MSG_ZERO
)

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Módulo de TRANSFORMACIÓN con Normalización de caracteres y espacios.
    """
    
    logging.info("----- Iniciando normalización y limpieza profunda...")

    # 1. COPIA DINÁMICA DE TODAS LAS COLUMNAS RECUPERADAS
    df_clean = df.copy()

    # 2. NORMALIZACIÓN DE CARACTERES Y ESPACIOS (Regla: Caracteres reales y sin espacios extremos)
    # Definimos una función para limpiar cada celda
    def normalize_string(val):
        if pd.isna(val): return val
        # Convertir a string y a minúsculas
        val = str(val).lower()
        # Eliminar caracteres de control y basura (no imprimibles) usando Regex
        val = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', val)
        # Eliminar espacios al principio y al final
        return val.strip()

    # Aplicamos la normalización a todo el DataFrame
    df_clean = df_clean.applymap(normalize_string)

    # 3. MANEJO DE NULL / VACÍOS -> "sin dato"
    df_clean = df_clean.replace(['nan', 'none', ''], MSG_NULL.lower())

    # 4. MANEJO DE VALORES CERO -> "campo con valor cero"
    df_clean = df_clean.replace(['0', '0.0'], MSG_ZERO.lower())

    # 5. VALIDACIÓN DE CAMPOS NUMÉRICOS (id, cod, num)
    cols_para_int = [c for c in df_clean.columns if any(x in c for x in ['id', 'cod', 'num'])]
    
    for col in cols_para_int:
        # Intentamos convertir a número para validar integridad
        temp_series = pd.to_numeric(df_clean[col], errors='coerce')
        
        # Si es negativo o basura (NaN) y no era un 'campo con valor cero'
        mask_error = (temp_series < 0) | (temp_series.isna() & (df_clean[col] != MSG_ZERO.lower()))
        df_clean.loc[mask_error, col] = str(MSG_ERR_NUMERO)

    # 6. REGLA ESPECÍFICA: GÉNERO (Solo hombre/mujer)
    if 'genero' in df_clean.columns:
        permitidos = ['hombre', 'mujer']
        mask_genero = ~df_clean['genero'].isin(permitidos)
        df_clean.loc[mask_genero, 'genero'] = MSG_ERR_TEXTO.lower()

    # 7. FORMATEO DE FECHAS (Usando 1930-01-01 como centinela)
    cols_fechas = [c for c in df_clean.columns if 'fecha' in c]
    for col in cols_fechas:
        #
        df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce').fillna(pd.Timestamp(MSG_ERR_FECHA))
        df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%d')

    logging.info(f"----- Normalización completada para {len(df_clean)} registros.")
    
    return df_clean