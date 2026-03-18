"""
Extract module for load_postgres ETL.
Handles data extraction from Oracle database.
"""
import logging
import oracledb
import pandas as pd

logger = logging.getLogger(__name__)


def extract_from_oracle(connection_params: dict, query: str) -> pd.DataFrame:
    """
    Extract data from an Oracle database.

    Args:
        connection_params: Dictionary with Oracle connection parameters
                           (user, password, dsn).
        query: SQL query to execute.

    Returns:
        DataFrame with the extracted data.
    """
    logger.info("Connecting to Oracle database...")
    with oracledb.connect(**connection_params) as conn:
        logger.info("Executing extraction query...")
        df = pd.read_sql(query, con=conn)
    logger.info(f"Extracted {len(df)} rows.")
    return df
