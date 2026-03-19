import pandas as pd
import logging
import re
from config import (
    MSG_ERR_TEXTO, MSG_ERR_NUMERO, MSG_ERR_FECHA, 
    MSG_NULL, MSG_ZERO
)

# Compilados a nivel módulo para no recrearlos en cada lote
_CTRL_RE = re.compile(r'[\x00-\x1f\x7f-\x9f]')
_YEAR_RE = re.compile(r'^(\d{2})([-/]\d{2}[-/]\d{2})$')

def _expand_year(v: str) -> str:
    """
    Expande año de 2 dígitos a 4 en cualquier columna.
    Solo actúa sobre cadenas que coincidan EXACTAMENTE con YY-MM-DD o YY/MM/DD.
    Texto normal nunca hace match con este patrón.
    Regla: YY > 26 → '19YY...', YY <= 26 → '20YY...'
    """
    m = _YEAR_RE.match(v)
    if m:
        yy = int(m.group(1))
        return ('19' if yy > 26 else '20') + v
    return v


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Módulo de TRANSFORMACIÓN profesional con normalización vectorizada.
    """
    logging.info("----- Iniciando normalizacion y limpieza profunda...")

    # 1. COPIA Y NORMALIZACIÓN DE NOMBRES DE COLUMNAS
    df_clean = df.copy()
    df_clean.columns = [c.lower() for c in df_clean.columns]

    # 2. NORMALIZACIÓN DE STRINGS — list comprehensions de Python puro.
    # _expand_year se aplica a TODAS las columnas aquí: si una columna de fecha
    # no tiene 'fecha' en el nombre, el año corto queda expandido igualmente.
    for col in df_clean.columns:
        df_clean[col] = [
            _expand_year(_CTRL_RE.sub('', str(v).lower().strip()))
            for v in df_clean[col]
        ]

    # 3. MANEJO DE NULL / VACÍOS / CEROS
    # Después del paso 2 todo es str: NaN → 'nan', None → 'none', 0 → '0', 0.0 → '0.0'
    reemplazos = {
        'nan':  MSG_NULL.lower(),
        'none': MSG_NULL.lower(),
        '':     MSG_NULL.lower(),
        '0':    MSG_ZERO.lower(),
        '0.0':  MSG_ZERO.lower()
    }
    df_clean = df_clean.replace(reemplazos)

    # 4. VALIDACIÓN DINÁMICA DE CAMPOS NUMÉRICOS
    # \bid\b  → word boundary: evita capturar 'apellido', 'valido', etc.
    cols_para_int = [
        c for c in df_clean.columns
        if re.search(r'\bid\b|\bcod|\bnum', c)
    ]
    for col in cols_para_int:
        series_numerica = pd.to_numeric(df_clean[col], errors='coerce')
        mask_error = series_numerica.isna() | (series_numerica < 0)
        series_numerica = series_numerica.where(~mask_error, other=float(MSG_ERR_NUMERO))
        df_clean[col] = series_numerica.fillna(float(MSG_ERR_NUMERO)).astype('int64')

    # 5. REGLA ESPECÍFICA: GÉNERO
    if 'genero' in df_clean.columns:
        permitidos = ['hombre', 'mujer']
        mask_genero = ~df_clean['genero'].isin(permitidos)
        df_clean.loc[mask_genero, 'genero'] = MSG_ERR_TEXTO.lower()

    # 6. FORMATEO DE FECHAS (Centinela 1930-01-01)
    # Los años cortos ya fueron expandidos en el paso 2 para TODAS las columnas.
    # Aquí solo parseamos y formateamos las columnas cuyo nombre contiene 'fecha'.
    SENTINEL = pd.Timestamp(MSG_ERR_FECHA)
    cols_fechas = [c for c in df_clean.columns if 'fecha' in c]
    for col in cols_fechas:
        df_clean[col] = (
            pd.to_datetime(df_clean[col], dayfirst=True, errors='coerce')
            .fillna(SENTINEL)
            .dt.strftime('%Y-%m-%d')
        )

    logging.info(f"----- Normalizacion completada para {len(df_clean):,} registros.")
    return df_clean