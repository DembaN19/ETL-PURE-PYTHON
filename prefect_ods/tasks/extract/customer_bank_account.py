from prefect import task, get_run_logger
import pandas as pd
import uuid
from pathlib import Path
from config.settings import SQLSERVER_CONN_BC

# Chemin absolu vers le dossier data_lake dans prefect_ods
DATA_LAKE_PATH = Path(__file__).parent.parent.parent / 'data_lake'

@task(name="Extract Customer_Bank_Account", retries=3, retry_delay_seconds=10)
def extract_customer_bank_account():
    logger = get_run_logger()
    
    query = """
    SELECT 
	[Customer No_] [CustomerNo],
	[Code] [Code],
	[Name] [Name],
	[Name 2] [Name2],
	[Address] [Address],
	[Address 2] [Address2],
	[City] [City],
	[Post Code] [PostCode],
	[Contact] [Contact],
	[Phone No_] [PhoneNo],
	[Telex No_] [TelexNo],
	[Bank Branch No_] [BankBranchNo],
	[Bank Account No_] [BankAccountNo],
	[Transit No_] [TransitNo],
	[Currency Code] [CurrencyCode],
	[Country_Region Code] [CountryRegionCode],
	[County] [County],
	[Fax No_] [FaxNo],
	[Telex Answer Back] [TelexAnswerBack],
	[Language Code] [LanguageCode],
	[E-Mail] [Email],
	[Home Page] [HomePage],
	[IBAN] [IBAN],
	[SWIFT Code] [SWIFTCode],
	[Bank Clearing Code] [BankClearingCode],
	[Bank Clearing Standard] [BankClearingStandard],
	[Agency Code] [AgencyCode],
	[RIB Key] [RIBKey],
	[RIB Checked] [RIBChecked]
    FROM [company_name].[dbo].[table_business_central_prefix$Customer Bank Account$id_business_central]
    """
    
    df = pd.read_sql(query, con=SQLSERVER_CONN_BC)
    
    # Convertir les colonnes UUID en string
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
            df[col] = df[col].astype(str)
    
    # Utilisation du chemin absolu pour sauvegarder le fichier parquet
    output_path = DATA_LAKE_PATH / 'raw' / 'customer_bank_account.parquet'
    df.to_parquet(str(output_path))
    logger.info(f"{len(df)} lignes extraites pour Customer_Bank_Account.")
    return df
