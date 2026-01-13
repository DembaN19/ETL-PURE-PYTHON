from prefect import task, get_run_logger
import pandas as pd
import uuid
from pathlib import Path
from config.settings import SQLSERVER_CONN_BC

# Chemin absolu vers le dossier data_lake dans prefect_ods
DATA_LAKE_PATH = Path(__file__).parent.parent.parent / 'data_lake'



@task(name="Extract Salesperson", retries=3, retry_delay_seconds=10)
def extract_salesperson():
    logger = get_run_logger()
    query = """
        SELECT
        T1.Code,
        T1.Name SalesPersonName,
        T1.[Commission _] Commission_,
        T2.[TM Commission2$067a6169-f228-4252-9802-cc5452726985] [TMCommission2],
        T2.[TM Is actif$067a6169-f228-4252-9802-cc5452726985] [TMIsactif],
        T2.[TM VA$067a6169-f228-4252-9802-cc5452726985] [TMVA],
        T2.[TM Region Code$067a6169-f228-4252-9802-cc5452726985] [TMRegionCode],
        T2.[TM Region Name$067a6169-f228-4252-9802-cc5452726985] [TMRegionName],
        T2.[TM Registration Code$067a6169-f228-4252-9802-cc5452726985] [TMRegistrationCode],
        T2.[TM SalesPerson Status$067a6169-f228-4252-9802-cc5452726985] [TMSalesPersonStatus],
        T3.[Team Code] TeamCode,
        T4.Name TeamName,
        T1.[E-Mail] EMail,
        T1.[Phone No_] PhoneNo_,
        T1.[Job Title] JobTitle,
        T2.[TM Is Actif$067a6169-f228-4252-9802-cc5452726985] TMIsActif
        FROM
            [company_name].[dbo].[table_business_central_prefix$Salesperson_Purchaser$id_business_central] T1
        LEFT JOIN
            [company_name].[dbo].[table_business_central_prefix$Salesperson_Purchaser$id_business_central$ext] T2
            ON T1.Code = T2.Code
        LEFT JOIN
            [company_name].[dbo].[table_business_central_prefix$Team Salesperson$id_business_central] T3
            ON T1.Code = T3.[Salesperson Code]
        LEFT JOIN
            [company_name].[dbo].[table_business_central_prefix$Team$id_business_central] T4
            ON T3.[Team Code] = T4.Code
        ORDER BY
            TeamCode
    """
    df = pd.read_sql(query, con=SQLSERVER_CONN_BC)
    
    # Convertir les colonnes UUID en string
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
            df[col] = df[col].astype(str)
    
    # Utilisation du chemin absolu pour sauvegarder le fichier parquet
    output_path = DATA_LAKE_PATH / 'raw' / 'salesperson.parquet'
    df.to_parquet(str(output_path))
    logger.info(f"{len(df)} lignes extraites pour Salesperson_Purchaser.")
    return df
