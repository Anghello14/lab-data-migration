"""
Load module for write_csv ETL.
Handles writing transformed data to CSV files.
"""
import logging
import os
import pandas as pd

logger = logging.getLogger(__name__)


def write_to_csv(
    df: pd.DataFrame,
    output_path: str,
    sep: str = ",",
    encoding: str = "utf-8",
    index: bool = False,
) -> None:
    """
    Write a DataFrame to a CSV file.

    Args:
        df: DataFrame to write.
        output_path: Full path of the output CSV file.
        sep: Field delimiter for the CSV file. Defaults to ','.
        encoding: File encoding. Defaults to 'utf-8'.
        index: Whether to write row indices. Defaults to False.
    """
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    logger.info(f"Writing {len(df)} rows to '{output_path}'...")
    df.to_csv(output_path, sep=sep, encoding=encoding, index=index)
    logger.info("CSV write complete.")
