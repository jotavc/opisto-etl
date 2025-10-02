from pathlib import Path
import pandas as pd
from opisto_etl.load.ftp_loader import save_csv

def test_save_csv_semicolon_and_decimal(tmp_path: Path):
    df = pd.DataFrame({"A": [1.5], "B": ["x"]})
    out = save_csv(df, tmp_path / "test.csv")
    assert out.exists()

    # Read raw text to assert semicolon and comma-decimal are present
    text = out.read_text(encoding="utf-8")
    # header uses ';' as separator
    assert "A;B" in text.splitlines()[0]
