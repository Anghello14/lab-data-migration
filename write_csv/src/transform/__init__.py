"""
Transform module for write_csv ETL.
Handles data transformation and hashing logic.
"""
import hashlib
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def hash_column(value: str) -> str:
    """Return the SHA-256 hash of a string value."""
    return hashlib.sha256(str(value).encode()).hexdigest()


def apply_transformations(
    df: pd.DataFrame, hash_columns: list | None = None
) -> pd.DataFrame:
    """
    Apply transformations to the extracted DataFrame.

    Args:
        df: Raw DataFrame from the extraction step.
        hash_columns: Optional list of column names whose values should be
                      replaced with their SHA-256 hash.

    Returns:
        Transformed DataFrame ready for writing to CSV.
    """
    logger.info("Applying transformations...")
    df = df.copy()
    df.columns = [col.lower() for col in df.columns]
    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)
    for col in hash_columns or []:
        if col in df.columns:
            logger.info(f"Hashing column '{col}'...")
            df[col] = df[col].apply(hash_column)
    logger.info(f"Transformation complete. {len(df)} rows remaining.")
    return df
