import pandas as pd

EXPORT_COLUMNS = [
        'Part_Name', 'Identifier', 'Brand', 'Gam', 'Model', 'VIN',
        'Vehicle_Identifier', 'Energy', 'Price', 'Quantity', 'Guarantee',
        'Description', 'Part_Condition', 'Licence_Plate', 'Version',
        'Finish', 'GearBox_Type', 'GearBox_Code', 'Engine_Code', 'Color',
        'Color_Code', 'Manufacturer_Reference', 'Shipping_Fees', 'Circulation_Date',
        'Mileage', 'CNIT', 'Doors_Number', 'Part_Pictures', 'Vehicle_Pictures',
        'KType', 'KBA_Nummer', 'Power_HP', 'Displacement'
    ]
    

def clean_urls(value):
    if isinstance(value, list):        
        return ",".join(url.strip() for url in value)
    #case to manage sample data
    elif isinstance(value, str):
        urls = [url.strip() for url in value.split(",") if url.strip()]
        return ",".join(urls)
    else:
        return ""

def transform(df: pd.DataFrame) -> pd.DataFrame:

    if df is not None:
        # Apply formatting and type conversion
        df['Part_Name'] = (df['descripcion_articulo'].astype("string").str.strip().fillna("").str[:70])        
        df['Identifier'] = df['ref_local'].astype("string").str.strip().fillna("").str[:50]
        df['Brand'] = df['nombre_marca'].astype("string").str.strip().fillna("").str[:50]
        df['Gam'] = df['nombre_version'].astype("string").str.strip().fillna("").str[:50]
        df['Model'] = df['nombre_modelo'].astype("string").str.strip().fillna("").str[:50]
        df['VIN'] = df['bastidor'].astype("string").str.strip().fillna("").str[:50]
        df['Vehicle_Identifier'] = pd.to_numeric(df['codigo'], errors='coerce').astype('Int64')
        df['Energy'] = df['combustible'].astype("string").str.strip().fillna("").str[:50]
        df['Price'] = pd.to_numeric(df['precio'], errors='coerce').div(100).round(4)
        df['Quantity'] = 1
        df['Guarantee'] = 12
        df['Description'] = df['observaciones'].astype("string").str.strip().fillna("").str.replace("\r\n", " ").str[:2000]

        # columns with empty values
        df['Part_Condition'] = ""
        df['Licence_Plate'] = ""
        df['Version'] = ""
        df['Finish'] = ""
        df['GearBox_Type'] = ""
        df['Color_Code'] = ""
        df['Shipping_Fees'] = ""
        df['Circulation_Date'] = ""
        df['Mileage'] = ""
        df['CNIT'] = ""
        df['KType'] = ""
        df['KBA_Nummer'] = ""

        df['Engine_Code'] = df['codigo_motor'].astype("string").str.strip().fillna("").str[:50]
        df['GearBox_Code'] = df['codigo_cambio'].astype("string").str.strip().fillna("").str[:50]
        df['Color'] = df['color'].astype("string").str.strip().fillna("").str[:50]
        df['Manufacturer_Reference'] = df['ref_principal'].astype("string").str.strip().fillna("").str.removeprefix("SLV").str.strip().str[:50]
        df['Doors_Number'] = pd.to_numeric(df['puertas'], errors='coerce').astype('Int64')
        df['Part_Pictures'] = df['p_urls_imgs'].apply(clean_urls).str[:600]
        df['Vehicle_Pictures'] = df['v_urls_imgs'].apply(clean_urls).str[:600]
        df['Power_HP'] = pd.to_numeric(df['potencia_hp'], errors='coerce').astype('Int64')
        df['Displacement'] = pd.to_numeric(df['cilindrada'], errors='coerce').astype('Int64')

    export_df = df[EXPORT_COLUMNS]
    return export_df