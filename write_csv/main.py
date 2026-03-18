"""
write_csv ETL - Main entry point.

Pipeline: Oracle (Extract) → Transform → CSV (Load)
"""
import configparser
import logging
import os
import sys

from src.extract import extract_from_oracle
from src.transform import apply_transformations
from src.load import write_to_csv


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

    log_file = config.get("logging", "log_file", fallback="logs/write_csv.log")
    log_level = config.get("logging", "level", fallback="INFO")
    setup_logging(log_file, log_level)

    logger = logging.getLogger(__name__)
    logger.info("Starting write_csv ETL pipeline...")

    oracle_params = {
        "user": config.get("oracle", "user"),
        "password": config.get("oracle", "password"),
        "dsn": config.get("oracle", "dsn"),
    }

    query = "SELECT * FROM your_table"

    output_dir = config.get("csv", "output_dir", fallback="output")
    filename = config.get("csv", "filename", fallback="result.csv")
    sep = config.get("csv", "separator", fallback=",")
    encoding = config.get("csv", "encoding", fallback="utf-8")
    output_path = os.path.join(os.path.dirname(__file__), output_dir, filename)

    df = extract_from_oracle(oracle_params, query)
    df = apply_transformations(df, hash_columns=[])
    write_to_csv(df, output_path=output_path, sep=sep, encoding=encoding)

    logger.info("ETL pipeline finished successfully.")


if __name__ == "__main__":
    main()
