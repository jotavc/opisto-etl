import logging
import pandas as pd
import os

from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s - %(message)s"
)

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

QUERY = text("SELECT * FROM export.opisto_export_view;")


def fetch_data(demo: bool = False) -> pd.DataFrame:
    """Fetch from Postgres (default) or from a local sample CSV when demo=True."""
    if demo:
        df = pd.read_csv(Path("data") / "sample_source.csv",
                         na_values=["NULL", "null", "NaN", "nan", ""])
        logging.info("DEMO mode → loaded sample CSV: rows=%s cols=%s", len(df), len(df.columns))
        return df
    
    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    df = pd.read_sql_query(QUERY, engine)
    logging.info("Loaded from Postgres: rows=%s cols=%s", len(df), len(df.columns))
    return df


# Smoke test
if __name__ == "__main__":
    df = fetch_data(demo=True)
    logging.info(f"Rows: {len(df):,} | Cols: {len(df.columns)}")
    logging.info("Columns: %s", list(df.columns))