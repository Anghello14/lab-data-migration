"""
Módulo de extracción de datos desde Oracle Database.

Utiliza la librería oracledb en modo Thin (sin instalación de Oracle Client).
Implementa extracción paginada para manejar tablas de gran volumen sin saturar RAM.
"""
import oracledb
import pandas as pd
import logging
# Credenciales y DSN de conexión definidos en config/settings.py
from config.settings import ORACLE_USER, ORACLE_PASS, DSN


class OracleReader:
    """
    Clase responsable de la conexión y extracción de datos desde Oracle.

    Establece una conexión persistente en modo Thin al instanciarse.
    Debe cerrarse explícitamente con el método close() al finalizar.
    """

    def __init__(self):
        """
        Inicializa la conexión a Oracle en modo Thin.
        Modo Thin: no requiere Oracle Instant Client instalado en el sistema.
        """
        self.conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASS, dsn=DSN)

    def get_count(self, table, condition="1=1"):
        """
        Obtiene el conteo exacto de filas en Oracle para reconciliación.

        Se usa antes de la extracción para verificar que se descargaron
        exactamente las mismas filas que existen en la base de datos.

        Parámetros:
          table     -- Nombre de la tabla a contar.
          condition -- Cláusula WHERE opcional (por defecto selecciona todas).

        Retorna:
          Entero con el número total de filas en Oracle.
        """
        # Consulta de conteo con condición dinámica para posibles filtros futuros
        query = f"SELECT COUNT(*) FROM {table} WHERE {condition}"
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchone()[0]  # Retorna el primer (y único) valor de la fila

    def extract_table_paginated(self, table, condition="1=1", batch_size=50000):
        """
        Extrae todos los datos de una tabla usando paginación OFFSET/FETCH.

        La paginación evita cargar millones de filas en memoria de una sola vez.
        El ORDER BY es obligatorio en Oracle para que OFFSET/FETCH funcione
        de forma determinista.

        Parámetros:
          table      -- Nombre de la tabla a extraer.
          condition  -- Cláusula WHERE opcional.
          batch_size -- Número de filas por página (por defecto 50.000).

        Retorna:
          DataFrame de pandas con la extracción completa concatenada.
        """
        # Obtener total de filas para saber cuántas páginas iterar
        total_rows = self.get_count(table, condition)
        offset = 0       # Posición inicial de la ventana de paginación
        chunks = []      # Lista para acumular cada bloque extraído

        while offset < total_rows:
            # ORDER BY 1 es obligatorio en Oracle para usar OFFSET/FETCH
            query = f"""
                SELECT * FROM {table}
                WHERE {condition}
                ORDER BY 1
                OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY
            """
            # pd.read_sql devuelve un DataFrame directamente desde la conexión
            chunk = pd.read_sql(query, self.conn)
            chunks.append(chunk)
            offset += batch_size  # Avanzar la ventana al siguiente bloque
            logging.info(
                f"[{table}] Extraidos {min(offset, total_rows)} "
                f"de {total_rows} registros..."
            )

        # Si la tabla está vacía, retornar DataFrame vacío
        if not chunks:
            return pd.DataFrame()

        # Concatenar todos los bloques en un único DataFrame final
        return pd.concat(chunks, ignore_index=True)

    def close(self):
        """
        Cierra la conexión a Oracle de forma segura.
        Debe llamarse siempre al finalizar, incluso si hubo errores.
        """
        if self.conn:
            self.conn.close()