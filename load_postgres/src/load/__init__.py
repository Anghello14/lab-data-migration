"""
Load module for load_postgres ETL.
Handles data loading into a PostgreSQL database via SQLAlchemy.
"""
import logging
import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def load_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    connection_string: str,
    if_exists: str = "append",
) -> None:
    """
    Load a DataFrame into a PostgreSQL table.

    Args:
        df: DataFrame to load.
        table_name: Target table name in PostgreSQL.
        connection_string: SQLAlchemy connection string for PostgreSQL.
                           Format: postgresql+psycopg2://user:password@host:port/dbname
        if_exists: Action if the table already exists ('fail', 'replace', 'append').
    """
    logger.info(f"Loading {len(df)} rows into table '{table_name}'...")
    engine = create_engine(connection_string)
    with engine.begin() as conn:
        df.to_sql(table_name, con=conn, if_exists=if_exists, index=False)
    logger.info("Load complete.")
