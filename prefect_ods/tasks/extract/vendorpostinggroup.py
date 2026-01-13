from prefect import task, get_run_logger
import pandas as pd
import uuid
from pathlib import Path
from config.settings import SQLSERVER_CONN_BC

# Chemin absolu vers le dossier data_lake dans prefect_ods
DATA_LAKE_PATH = Path(__file__).parent.parent.parent / 'data_lake'

@task(name="Extract Vendor Posting Group", retries=3, retry_delay_seconds=10)
def extract_vendorpostinggroup():
    logger = get_run_logger()

    query = """
    SELECT 
    A.[Code] AS Code,
    A.[Payables Account] AS PayablesAccount,
    A.[Service Charge Acc_] AS ServiceChargeAcc,
    A.[Payment Disc_ Debit Acc_] AS PaymentDiscDebitAcc,
    A.[Invoice Rounding Account] AS InvoiceRoundingAccount,
    A.[Debit Curr_ Appln_ Rndg_ Acc_] AS DebitCurrApplnRndgAcc,
    A.[Credit Curr_ Appln_ Rndg_ Acc_] AS CreditCurrApplnRndgAcc,
    A.[Debit Rounding Account] AS DebitRoundingAccount,
    A.[Credit Rounding Account] AS CreditRoundingAccount,
    A.[Payment Disc_ Credit Acc_] AS PaymentDiscCreditAcc,
    A.[Payment Tolerance Debit Acc_] AS PaymentToleranceDebitAcc,
    A.[Payment Tolerance Credit Acc_] AS PaymentToleranceCreditAcc,
    A.[Description] AS Description,
    A.[$systemId] AS systemId,
    A.[View All Accounts on Lookup] AS ViewAllAccountsonLookup,
    B.[ACY Description Multico$df74057f-5b58-4d6f-8a52-989845f6320c] AS ACYDescriptionMultico
    FROM 
        [company_name].[dbo].[table_business_central_prefix$Vendor Posting Group$id_business_central] A
    INNER JOIN 
        [company_name].[dbo].[table_business_central_prefix$Vendor Posting Group$id_business_central$ext] B
        ON A.Code = B.Code
    """
    df = pd.read_sql(query, con=SQLSERVER_CONN_BC)
    
    # Convertir les colonnes UUID en string
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
            df[col] = df[col].astype(str)
    
    # Utilisation du chemin absolu pour sauvegarder le fichier parquet
    output_path = DATA_LAKE_PATH / 'raw' / 'vendorpostinggroup.parquet'
    df.to_parquet(str(output_path))
    logger.info(f"{len(df)} lignes extraites pour Vendor Posting Group.")
    return df
