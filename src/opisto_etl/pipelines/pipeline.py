from prefect import flow, task, get_run_logger
from opisto_etl.extract.postgres_extractor import fetch_data
from opisto_etl.transform.transform import transform
from opisto_etl.load.ftp_loader import upload_to_ftp, save_csv


@task
def t_extract(demo: bool):
    logger = get_run_logger()
    df = fetch_data(demo=demo)
    logger.info("Extracted rows=%s, cols=%s", len(df), len(df.columns))
    return df

@task
def t_transform(df):
    logger = get_run_logger()
    df_t = transform(df)
    logger.info("Transformed rows=%s, cols=%s", len(df_t), len(df_t.columns))
    return df_t

@task
def t_load(df, upload: bool):
    logger = get_run_logger()
    out_name = "Opisto.csv"
    path = save_csv(df, out_name)
    logger.info("CSV saved to %s", path)

    if upload:
        ok = upload_to_ftp(path)
        logger.info("FTP upload %s", "succeeded" if ok else "failed or skipped by loader")
    else:
        logger.info("Upload disabled (demo)")
    return True

@flow(name="opisto-etl")
def run_pipeline(demo: bool = False, upload: bool = True):
    df = t_extract(demo)
    df_t = t_transform(df)
    t_load(df_t, upload)



# Smoke test
if __name__ == "__main__":
    run_pipeline(demo=False, upload=False)
