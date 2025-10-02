import pandas as pd
from opisto_etl.transform.transform import transform, EXPORT_COLUMNS

def test_transform_schema_and_price(df_raw):
    out = transform(df_raw)

    # 1) Columns: exact set & order
    assert list(out.columns) == EXPORT_COLUMNS

    # 2) Row count unchanged
    assert len(out) == len(df_raw)

    # 3) Price conversion (12345 cents -> 123.45)
    assert float(out.loc[0, "Price"]) == 123.45

def test_transform_specific_fields(df_raw):
    out = transform(df_raw)

    # Manufacturer_Reference: 'SLV' prefix removed and trimmed
    assert out.loc[0, "Manufacturer_Reference"] == "123"

    # Part_Pictures: list joined with comma and truncated policy applied
    assert out.loc[0, "Part_Pictures"] == "http://p1,http://p2"

    # Basic string truncation behavior (length caps shouldn't chop this sample)
    assert len(out.loc[0, "Part_Name"]) <= 70
    assert len(out.loc[0, "Identifier"]) <= 50
