import os
import pandas as pd
from ftplib import FTP_TLS

def save_csv(df: pd.DataFrame, file_path: str):
    df.to_csv(file_path, index=False, sep=';', encoding='utf-8')
    return file_path


def save_csv(df: pd.DataFrame, file_path: str):

    # CSV settings required by Opisto
    sep = ";"
    decimal = ","
    encoding = "utf-8"

    df.to_csv(
        file_path,
        index=False,
        sep=sep,
        decimal=decimal,
        na_rep="",
        encoding=encoding,
    )

    return file_path

def upload_to_ftp(local_path: str):
    host = os.getenv('FTP_HOST')
    user = os.getenv('FTP_USER')
    password = os.getenv('FTP_PASSWORD')

    with FTP_TLS(host) as ftp:
        ftp.login(user=user, passwd=password)
        ftp.prot_p() # Switch to secure data connection
        with open(local_path, 'rb') as file:
            ftp.storbinary(f'STOR {os.path.basename(local_path)}', file)
        ftp.quit()
    return True