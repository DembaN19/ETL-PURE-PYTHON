from prefect import task, get_run_logger
import pandas as pd
import uuid
from pathlib import Path
from config.settings import SQLSERVER_CONN_ODS

# Chemin absolu vers le dossier data_lake dans prefect_dw
DATA_LAKE_PATH = Path(__file__).parent.parent.parent / 'data_lake'

@task(name="Extract G_LEntry", retries=3, retry_delay_seconds=10)
def extract_g_lentry():
    logger = get_run_logger()

    query = """
    select *
    from [ODS_SERVER].[dbo].[G_LEntry]
    """
    df = pd.read_sql(query, con=SQLSERVER_CONN_ODS)
    
    # Convertir les colonnes UUID en string
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
            df[col] = df[col].astype(str)
    
    # Utilisation du chemin absolu pour sauvegarder le fichier parquet
    output_path = DATA_LAKE_PATH / 'raw' / 'g_lentry.parquet'
    df.to_parquet(str(output_path))
    logger.info(f"{len(df)} lignes extraites pour G_LEntry.")
    return df
