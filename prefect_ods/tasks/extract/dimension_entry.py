from prefect import task, get_run_logger
import pandas as pd
import uuid
from pathlib import Path
from config.settings import SQLSERVER_CONN_BC

# Chemin absolu vers le dossier data_lake dans prefect_ods
DATA_LAKE_PATH = Path(__file__).parent.parent.parent / 'data_lake'

@task(name="Extract Dimension_Entry", retries=3, retry_delay_seconds=10)
def extract_dimension_entry():
    logger = get_run_logger()
    
    query = """
    SELECT 
	[Dimension Set ID] [DimensionSetID], 
	[Dimension Code] [DimensionCode], 
	[Dimension Value Code] [DimensionValueCode], 
	[Dimension Value ID] [DimensionValueID], 
	[$systemId] [systemId] 
    FROM [company_name].[dbo].[table_business_central_prefix$Dimension Set Entry$id_business_central]
    """
    
    df = pd.read_sql(query, con=SQLSERVER_CONN_BC)
    
    # Convertir les colonnes UUID en string
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
            df[col] = df[col].astype(str)
    
    # Utilisation du chemin absolu pour sauvegarder le fichier parquet
    output_path = DATA_LAKE_PATH / 'raw' / 'dimension_entry.parquet'
    df.to_parquet(str(output_path))
    logger.info(f"{len(df)} lignes extraites pour Dimension_Entry.")
    return df
