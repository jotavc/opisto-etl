
from opisto_etl.pipelines.pipeline import run_pipeline

if __name__ == "__main__":
    # Production: read from Postgres and upload to FTP
    run_pipeline(demo=False, upload=True)
