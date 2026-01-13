from prefect import task, get_run_logger
import pandas as pd
import uuid
from pathlib import Path
from config.settings import SQLSERVER_CONN_BC

# Chemin absolu vers le dossier data_lake dans prefect_ods
DATA_LAKE_PATH = Path(__file__).parent.parent.parent / 'data_lake'

@task(name="Extract Postcode", retries=3, retry_delay_seconds=10)
def extract_postcode():
    logger = get_run_logger()

    query = """
    SELECT 
	coalesce(Code, '') Code, 
	City, 
	[Search City] [SearchCity],
	[Country_Region Code] [CountryRegionCode],
	County,
	[$systemId] [systemId],
	[$systemCreatedAt] [systemCreatedAt],
	[$systemCreatedBy] [systemCreatedBy],
	[$systemModifiedAt] [systemModifiedAt],
	[$systemModifiedBy] [systemModifiedBy],
	[Time Zone] [TimeZone]
    FROM
    [company_name].[dbo].[table_business_central_prefix$Post Code$id_business_central]
    WHERE
    Code IS NOT NULL AND LTRIM(RTRIM(Code)) <> ''
    """
    df = pd.read_sql(query, con=SQLSERVER_CONN_BC)
    
    # Convertir les colonnes UUID en string
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
            df[col] = df[col].astype(str)
    
    # Utilisation du chemin absolu pour sauvegarder le fichier parquet
    output_path = DATA_LAKE_PATH / 'raw' / 'postcode.parquet'
    df.to_parquet(str(output_path))
    logger.info(f"{len(df)} lignes extraites pour Postcode.")
    return df
