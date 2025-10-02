
from datetime import datetime
import logging

from opisto_etl.extract.postgres_extractor import fetch_data
from opisto_etl.transform.transform import transform
from opisto_etl.load.ftp_loader import save_csv

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

def main():
    # 1) Extract from local sample CSV (no DB)
    df = fetch_data(demo=True)
    logging.info("Extracted rows=%s, cols=%s", len(df), len(df.columns))

    # 2) Transform
    df_t = transform(df)
    logging.info("Transformed rows=%s, cols=%s", len(df_t), len(df_t.columns))

    # 3) Save CSV only (no FTP)
    out_name = f"Opisto_demo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    save_csv(df_t, out_name)
    logging.info("CSV saved to %s", out_name)


# Smoke test
if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("Demo failed")
        raise
