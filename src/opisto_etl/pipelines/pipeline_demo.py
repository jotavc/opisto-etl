
from opisto_etl.pipelines.pipeline import run_pipeline

if __name__ == "__main__":
    # Demo: read from data/sample_source.csv, do NOT upload to FTP, using Prefect for orchestration in local mode
    run_pipeline(demo=True, upload=False)
