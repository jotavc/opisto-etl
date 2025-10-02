import pandas as pd
import pytest

@pytest.fixture
def df_raw():
    """Minimal raw dataframe that mimics the extractor output schema."""
    return pd.DataFrame({
        "descripcion_articulo": ["Filtro aceite"],
        "ref_local": ["X1"],
        "nombre_marca": ["VW"],
        "nombre_version": ["Golf VI"],
        "nombre_modelo": ["Golf"],
        "bastidor": ["WVWZZZ..."],
        "codigo": [123], 
        "combustible": ["Gasolina"],
        "precio": [12345],
        "observaciones": ["ok"],
        "codigo_motor": ["BKC"],
        "codigo_cambio": ["5MT"],
        "color": ["Rojo"],
        "ref_principal": ["SLV123"],
        "puertas": [5],
        "p_urls_imgs": [["http://p1", "http://p2"]],
        "v_urls_imgs": ["http://v1"],
        "potencia_hp": [110],
        "cilindrada": [1600],
    })
