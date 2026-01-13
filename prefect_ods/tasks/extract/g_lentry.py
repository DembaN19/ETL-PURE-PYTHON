from prefect import task, get_run_logger
import pandas as pd
import uuid
from pathlib import Path
from config.settings import SQLSERVER_CONN_BC

# Chemin absolu vers le dossier data_lake dans prefect_ods
DATA_LAKE_PATH = Path(__file__).parent.parent.parent / 'data_lake'

@task(name="Extract G_LEntry", retries=3, retry_delay_seconds=10)
def extract_g_lentry():
    logger = get_run_logger()

    query = """
    SELECT A.[Entry No_] [EntryNo]
	,[G_L Account No_] [G_LAccountNo]
	,[Posting Date] [PostingDate]
	,[Document Type] [DocumentType]
	,[Document No_] [DocumentNo]
	,[Description] [Description]
	,[Bal_ Account No_] [BalAccountNo]
	,[Amount]
	,[Global Dimension 1 Code] [GlobalDimension1Code]
	,[Global Dimension 2 Code] [GlobalDimension2Code]
	,D.[Dimension Value Code] [GlobalDimension3Code]
	,E.[Dimension Value Code] [GlobalDimension4Code]
	,[User ID] [UserID]
	,[Source Code] [SourceCode]
	,[System-Created Entry] [SystemCreatedEntry]
	,[Prior-Year Entry] [PriorYearEntry]
	,[Job No_] [JobNo]
	,[Quantity] [Quantity]
	,[VAT Amount] [VATAmount]
	,[Business Unit Code] [BusinessUnitCode]
	,[Journal Batch Name] [JournalBatchName]
	,[Reason Code] [ReasonCode]
	,[Gen_ Posting Type] [GenPostingType]
	,[Gen_ Bus_ Posting Group] [GenBusPostingGroup]
	,[Gen_ Prod_ Posting Group] [GenProdPostingGroup]
	,[Bal_ Account Type] [BalAccountType]
	,[Transaction No_] [TransactionNo]
	,[Debit Amount] [DebitAmount]
	,[Credit Amount] [CreditAmount]
	,[Document Date] [DocumentDate]
	,[External Document No_] [ExternalDocumentNo]
	,[Source Type] [SourceType]
	,[Source No_] [SourceNo]
	,[No_ Series] [NoSeries]
	,[Tax Area Code] [TaxAreaCode]
	,[Tax Liable] [TaxLiable]
	,[Tax Group Code] [TaxGroupCode]
	,[Use Tax] [UseTax]
	,[VAT Bus_ Posting Group] [VATBusPostingGroup]
	,[VAT Prod_ Posting Group] [VATProdPostingGroup]
	,[Additional-Currency Amount] [AdditionalCurrencyAmount]
	,[Add_-Currency Debit Amount] [AddCurrencyDebitAmount]
	,[Add_-Currency Credit Amount] [AddCurrencyCreditAmount]
	,[Close Income Statement Dim_ ID] [CloseIncomeStatementDimID]
	,[IC Partner Code] [ICPartnerCode]
	,[Reversed] [Reversed]
	,[Reversed by Entry No_] [ReversedByEntryNo]
	,[Reversed Entry No_] [ReversedEntryNo]
	,A.[Dimension Set ID] [DimensionSetID]
	,[Prod_ Order No_] [ProdOrderNo]
	,[FA Entry Type] [FAEntryType]
	,[FA Entry No_] [FAEntryNo]
	,[Last Modified DateTime] [LastModifiedDate]
	,[Entry Type] [EntryType]
	,[Applies-to ID] [AppliestoID]
	,[Letter] [Letter]
	,[Letter Date] [LetterDate]
	,[ACYEntryType$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYEntryType]
	,[ACY Order No_$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYOrderNo]
	,[ACY Batch No_$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYBatchNo]
	,[ACY Reversal Date$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYReversalDate]
	,[ACY Historic Letter$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYHistoricLetter]
    FROM [company_name].[dbo].[table_business_central_prefix$G_L Entry$id_business_central] A
    LEFT JOIN [company_name].[dbo].[table_business_central_prefix$G_L Entry$id_business_central$ext] B ON A.[Entry No_] = B.[Entry No_]
    LEFT JOIN [company_name].[dbo].[table_business_central_prefix$Dimension Set Entry$id_business_central] D ON A.[Dimension Set ID] = D.[Dimension Set ID]
        AND D.[Dimension Code] = 'FAMILLE PRODUIT'
    LEFT JOIN [company_name].[dbo].[table_business_central_prefix$Dimension Set Entry$id_business_central] E ON A.[Dimension Set ID] = E.[Dimension Set ID]
        AND E.[Dimension Code] = 'OI'
    """
    df = pd.read_sql(query, con=SQLSERVER_CONN_BC)
    
    # Convertir les colonnes UUID en string
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
            df[col] = df[col].astype(str)
    
    # Utilisation du chemin absolu pour sauvegarder le fichier parquet
    output_path = DATA_LAKE_PATH / 'raw' / 'g_lentry.parquet'
    df.to_parquet(str(output_path))
    logger.info(f"{len(df)} lignes extraites pour G_LEntry.")
    return df
