from prefect import task, get_run_logger
import pandas as pd
import uuid
from pathlib import Path
from config.settings import SQLSERVER_CONN_BC

# Chemin absolu vers le dossier data_lake dans prefect_ods
DATA_LAKE_PATH = Path(__file__).parent.parent.parent / 'data_lake'

@task(name="Extract Vat_Entry", retries=3, retry_delay_seconds=10)
def extract_vat_entry():
    logger = get_run_logger()

    query = """
    SELECT 
	   [Entry No_] [EntryNo] 
      ,[Gen_ Bus_ Posting Group] [GenBusPostingGroup]
      ,[Gen_ Prod_ Posting Group] [GenProdPostingGroup]
      ,[Posting Date] [PostingDate]
      ,[Document No_] [DocumentNo]
      ,[Document Type] [DocumentType]
      ,[Type] [Type]
      ,[Base] [Base]
      ,[Amount] [Amount]
      ,[VAT Calculation Type] [VATCalculationType]
      ,[Bill-to_Pay-to No_] [BilltoPaytoNo]
      ,[EU 3-Party Trade] [EU3PartyTrade]
      ,[User ID] [UserID]
      ,[Source Code] [SourceCode]
      ,[Reason Code] [ReasonCode]
      ,[Closed by Entry No_] [ClosedbyEntryNo]
      ,[Closed] [Closed]
      ,[Country_Region Code] [CountryRegionCode]
      ,[Internal Ref_ No_] [InternalRefNo]
      ,[Transaction No_] [TransactionNo]
      ,[Unrealized Amount] [UnrealizedAmount]
      ,[Unrealized Base] [UnrealizedBase]
      ,[Remaining Unrealized Amount] [RemainingUnrealizedAmount]
      ,[Remaining Unrealized Base] [RemainingUnrealizedBase]
      ,[External Document No_] [ExternalDocumentNo]
      ,[No_ Series] [NoSeries]
      ,[Tax Area Code] [TaxAreaCode]
      ,[Tax Liable] [TaxLiable]
      ,[Tax Group Code] [TaxGroupCode]
      ,[Use Tax] [UseTax]
      ,[Tax Jurisdiction Code] [TaxJurisdictionCode]
      ,[Tax Group Used] [TaxGroupUsed]
      ,[Tax Type] [TaxType]
      ,[Tax on Tax] [TaxonTax]
      ,[Sales Tax Connection No_] [SalesTaxConnectionNo]
      ,[Unrealized VAT Entry No_] [UnrealizedVATEntryNo]
      ,[VAT Bus_ Posting Group] [VATBusPostingGroup]
      ,[VAT Prod_ Posting Group] [VATProdPostingGroup]
      ,[Additional-Currency Amount] [AdditionalCurrencyAmount]
      ,[Additional-Currency Base] [AdditionalCurrencyBase]
      ,[Add_-Currency Unrealized Amt_] [AddCurrencyUnrealizedAmt]
      ,[Add_-Currency Unrealized Base] [AddCurrencyUnrealizedBase]
      ,[VAT Base Discount _] [VATBaseDiscount]
      ,[Add_-Curr_ Rem_ Unreal_ Amount] [AddCurrencyRemUnrealAmount]
      ,[Add_-Curr_ Rem_ Unreal_ Base] [AddCurrencyRemUnrealBase]
      ,[VAT Difference] [VATDifference]
      ,[Add_-Curr_ VAT Difference] [AddCurrencyVATDifference]
      ,[Ship-to_Order Address Code] [ShipToOrderAddressCode]
      ,[Document Date] [DocumentDate]
      ,[VAT Registration No_] [VATRegistrationNo]
      ,[Reversed] [Reversed]
      ,[Reversed by Entry No_] [ReversedByEntryNo]
      ,[Reversed Entry No_] [ReversedEntryNo]
      ,[EU Service] [EUService]
      ,[Base Before Pmt_ Disc_] [BaseBeforePmtDisc] 
      ,[Realized Amount] [RealizedAmount]
      ,[Realized Base] [RealizedBase]
      ,[Add_-Curr_ Realized Amount] [AddCurrencyRealizedAmount]
      ,[Add_-Curr_ Realized Base] [AddCurrencyRealizedBase]
	  ,[G_L Entry No_] [GLEntryNo]
    FROM [company_name].[dbo].[table_business_central_prefix$VAT Entry$id_business_central] t1
    LEFT JOIN    [company_name].[dbo].[table_business_central_prefix$G_L Entry - VAT Entry Link$id_business_central] t2
    ON t2.[VAT Entry No_] = t1.[Entry No_]
    """
    df = pd.read_sql(query, con=SQLSERVER_CONN_BC)
    
    # Convertir les colonnes UUID en string
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
            df[col] = df[col].astype(str)
    
    # Utilisation du chemin absolu pour sauvegarder le fichier parquet
    output_path = DATA_LAKE_PATH / 'raw' / 'vat_entry.parquet'
    df.to_parquet(str(output_path))
    logger.info(f"{len(df)} lignes extraites pour Vat_Entry.")
    return df
