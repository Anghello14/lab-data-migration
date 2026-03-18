"""
load_postgres ETL - Main entry point.

Pipeline: Oracle (Extract) → Transform → PostgreSQL (Load)
"""
import configparser
import logging
import os
import sys

from src.extract import extract_from_oracle
from src.transform import apply_transformations
from src.load import load_to_postgres


def setup_logging(log_file: str, level: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )


def main() -> None:
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), "config", "config.ini"))

    log_file = config.get("logging", "log_file", fallback="logs/load_postgres.log")
    log_level = config.get("logging", "level", fallback="INFO")
    setup_logging(log_file, log_level)

    logger = logging.getLogger(__name__)
    logger.info("Starting load_postgres ETL pipeline...")

    oracle_params = {
        "user": config.get("oracle", "user"),
        "password": config.get("oracle", "password"),
        "dsn": config.get("oracle", "dsn"),
    }

    query = "SELECT * FROM your_table"

    pg_conn_string = (
        "postgresql+psycopg2://"
        f"{config.get('postgres', 'user')}:{config.get('postgres', 'password')}"
        f"@{config.get('postgres', 'host')}:{config.get('postgres', 'port')}"
        f"/{config.get('postgres', 'dbname')}"
    )

    df = extract_from_oracle(oracle_params, query)
    df = apply_transformations(df, hash_columns=[])
    load_to_postgres(df, table_name="target_table", connection_string=pg_conn_string)

    logger.info("ETL pipeline finished successfully.")


if __name__ == "__main__":
    main()
