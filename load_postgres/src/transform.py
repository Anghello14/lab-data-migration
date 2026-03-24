import pandas as pd
import logging
import re
from config import (
    MSG_ERR_TEXTO, MSG_ERR_NUMERO, MSG_ERR_FECHA, 
    MSG_NULL, MSG_ZERO
)

# Optimizacion: Compilacion de expresiones regulares a nivel de modulo.
# _CTRL_RE: Elimina caracteres de control no imprimibles que rompen la carga en Postgres.
_CTRL_RE = re.compile(r'[\x00-\x1f\x7f-\x9f]')
# _YEAR_RE: Identifica formatos de fecha con año de 2 digitos (YY-MM-DD o YY/MM/DD).
_YEAR_RE = re.compile(r'^(\d{2})([-/]\d{2}[-/]\d{2})$')

def _expand_year(v: str) -> str:
    """
    Funcion de soporte para normalizar la cronologia de los datos.
    Expande años de 2 digitos a 4 siguiendo la regla de negocio:
    YY > 26 (referencia al año actual 2026)
    YY <= 26 
    """
    m = _YEAR_RE.match(v)
    if m:
        yy = int(m.group(1))
        # Ajuste dinamico basado en el año actual del proyecto (2026).
        return ('19' if yy > 26 else '20') + v
    return v


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Modulo de TRANSFORMACION: Implementa limpieza profunda, normalizacion de tipos 
    y estandarizacion de valores nulos antes de la carga a base de datos.
    """
    logging.info("----- Iniciando normalizacion y limpieza profunda...")

    # 1. COPIA Y NORMALIZACIÓN DE NOMBRES DE COLUMNAS
    # Se fuerza todo a minusculas para garantizar compatibilidad con PostgreSQL.
    df_clean = df.copy()
    df_clean.columns = [c.lower() for c in df_clean.columns]

    # 2. NORMALIZACIÓN DE STRINGS — List comprehensions de alto rendimiento.
    # Se realiza: Eliminacion de basura ASCII, conversion a minusculas, recorte de espacios 
    # y expansion de años cortos en un solo paso por cada columna.
    for col in df_clean.columns:
        df_clean[col] = [
            _expand_year(_CTRL_RE.sub('', str(v).lower().strip()))
            for v in df_clean[col]
        ]

    # 3. MANEJO DE NULL / VACÍOS / CEROS
    # Estandariza diversas representaciones de 'ausencia de dato' al valor definido en config.py.
    # Esto asegura que la base de datos no reciba strings inconsistentes como 'nan' o 'none'.
    reemplazos = {
        'nan':   MSG_NULL.lower(),
        'none':  MSG_NULL.lower(),
        '':      MSG_NULL.lower(),
        '0':     MSG_ZERO.lower(),
        '0.0':   MSG_ZERO.lower()
    }
    df_clean = df_clean.replace(reemplazos)

    # 4. VALIDACIÓN DINÁMICA DE CAMPOS NUMÉRICOS
    # Evita el error 'Invalid value for dtype int64' detectado en pruebas previas.
    # Filtra columnas que semanticamente representen IDs o codigos usando RegEx.
    cols_para_int = [
        c for c in df_clean.columns 
        if re.search(r'\bid\b|\bcod|\bnum', c)
    ]
    for col in cols_para_int:
        # Intento de conversion a numerico; los fallos se marcan como NaN temporalmente.
        series_numerica = pd.to_numeric(df_clean[col], errors='coerce')
        # Se consideran errores: valores no numericos (NaN) o numeros negativos.
        mask_error = series_numerica.isna() | (series_numerica < 0)
        # Se aplica el valor centinela definido (ej. -1) donde existan errores.
        series_numerica = series_numerica.where(~mask_error, other=float(MSG_ERR_NUMERO))
        # Casteo final a int64 para coincidir con la arquitectura de la DB.
        df_clean[col] = series_numerica.fillna(float(MSG_ERR_NUMERO)).astype('int64')

    # 5. REGLA ESPECÍFICA: GÉNERO
    # Estandarizacion de una columna critica para reportes de victimas.
    if 'genero' in df_clean.columns:
        permitidos = ['hombre', 'mujer']
        mask_genero = ~df_clean['genero'].isin(permitidos)
        # Cualquier valor fuera de los permitidos se marca como error de texto estandarizado.
        df_clean.loc[mask_genero, 'genero'] = MSG_ERR_TEXTO.lower()

    # 6. FORMATEO DE FECHAS (Centinela 1930-01-01)
    # Procesa todas las columnas que contengan la palabra 'fecha'.
    SENTINEL = pd.Timestamp(MSG_ERR_FECHA)
    cols_fechas = [c for c in df_clean.columns if 'fecha' in c]
    for col in cols_fechas:
        # Conversion a objeto DateTime con dia primero (formato LATAM).
        # Los errores de formato se reemplazan con la fecha centinela 1930-01-01.
        df_clean[col] = (
            pd.to_datetime(df_clean[col], dayfirst=True, errors='coerce')
            .fillna(SENTINEL)
            .dt.strftime('%Y-%m-%d') # Formato ISO estandar para carga en SQL.
        )

    logging.info(f"----- Normalizacion completada para {len(df_clean):,} registros.")
    return df_clean