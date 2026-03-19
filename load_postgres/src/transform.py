import pandas as pd
import logging
import re
from config import (
    MSG_ERR_TEXTO, MSG_ERR_NUMERO, MSG_ERR_FECHA, 
    MSG_NULL, MSG_ZERO
)

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Módulo de TRANSFORMACIÓN profesional con normalización vectorizada.
    """
    logging.info("----- Iniciando normalizacion y limpieza profunda...")

    # 1. NORMALIZAR NOMBRES DE COLUMNAS A MINÚSCULAS PRIMERO
    df_clean = df.copy()
    df_clean.columns = [c.lower() for c in df_clean.columns]

    # 2. NORMALIZACIÓN DE STRINGS (Versión optimizada para Pandas 2.x)
    # En lugar de una función por celda, limpiamos la serie completa
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            # Convertir a minúsculas y quitar espacios extremos
            df_clean[col] = df_clean[col].astype(str).str.lower().str.strip()
            # Eliminar caracteres de control (Regex no imprimibles)
            df_clean[col] = df_clean[col].str.replace(r'[\x00-\x1f\x7f-\x9f]', '', regex=True)

    # 3. MANEJO DE NULL / VACÍOS / CEROS (Reemplazos masivos)
    # Usamos los mensajes estándar de tu config
    reemplazos = {
        'NaN': MSG_NULL.lower(),
        'none': MSG_NULL.lower(),
        '': MSG_NULL.lower(),
        '0': MSG_ZERO.lower(),
        0: MSG_ZERO.lower(),
        '0.0': MSG_ZERO.lower()
    }
    df_clean = df_clean.replace(reemplazos)

    # 4. VALIDACIÓN DINÁMICA DE CAMPOS NUMÉRICOS
    # Buscamos columnas que contengan 'id', 'cod' o 'num' en su nombre ya en minúsculas
    cols_para_int = [c for c in df_clean.columns if any(x in c for x in ['id', 'cod', 'num'])]
    
    for col in cols_para_int:
        # Convertimos a numérico el contenido de la columna limpia
        # Esto gestiona el error de tipos que vimos antes
        series_numerica = pd.to_numeric(df_clean[col], errors='coerce')
        
        # Máscara de error usando la serie numérica y el valor original en df_clean
        mask_error = (series_numerica.isna() & (df_clean[col].astype(str).str.strip() != '0')) | (series_numerica < 0)
        
        # Aplicamos el centinela directamente sobre la serie numérica (evita TypeError en columnas StringDtype)
        series_numerica = series_numerica.where(~mask_error, other=float(MSG_ERR_NUMERO))
        
        # Aseguramos el tipo de dato final para la carga en Postgres
        df_clean[col] = series_numerica.fillna(float(MSG_ERR_NUMERO)).astype('int64')

    # 5. REGLA ESPECÍFICA: GÉNERO (Consistencia de datos)
    if 'genero' in df_clean.columns:
        permitidos = ['hombre', 'mujer']
        # Si no es hombre ni mujer, es un DATO_INVALIDO
        mask_genero = ~df_clean['genero'].isin(permitidos)
        df_clean.loc[mask_genero, 'genero'] = MSG_ERR_TEXTO.lower()

    # 6. FORMATEO DE FECHAS (Centinela 1930-01-01)
    cols_fechas = [c for c in df_clean.columns if 'fecha' in c]
    for col in cols_fechas:
        # Convertimos a fecha, lo que no sirve se vuelve NaT y luego se llena con el centinela
        # dayfirst=True porque el formato del archivo es DD/MM/YYYY
        df_clean[col] = pd.to_datetime(df_clean[col], dayfirst=True, errors='coerce').fillna(pd.Timestamp(MSG_ERR_FECHA))
        df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%d')

    logging.info(f"----- Normalizacion completada para {len(df_clean):,} registros.")


    return df_clean