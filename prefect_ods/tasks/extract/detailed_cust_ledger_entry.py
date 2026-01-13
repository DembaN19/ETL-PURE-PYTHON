from prefect import task, get_run_logger
import pandas as pd
import uuid
from pathlib import Path
from config.settings import SQLSERVER_CONN_BC

# Chemin absolu vers le dossier data_lake dans prefect_ods
DATA_LAKE_PATH = Path(__file__).parent.parent.parent / 'data_lake'

@task(name="Extract Detailed_Cust_Ledg_Entry", retries=3, retry_delay_seconds=10)
def extract_detailed_cust_ledger_entry():
    logger = get_run_logger()

    query = """
    SELECT 
       A.[Entry No_] [EntryNo]
      ,[Cust_ Ledger Entry No_] [CustLedgerEntryNo]
      ,[Entry Type] [EntryType]
      ,[Posting Date] [PostingDate]
      ,[Document Type] [DocumentType]
      ,[Document No_] [DocumentNo]
      ,[Amount] [Amount]
      ,[Amount (LCY)] [AmountLCY]
      ,[Customer No_] [CustomerNo]
      ,[Currency Code] [CurrencyCode]
      ,[User ID] [UserID]
      ,[Source Code] [SourceCode]
      ,[Transaction No_] [TransactionNo]
      ,[Journal Batch Name] [JournalBatchName]
      ,[Reason Code] [ReasonCode]
      ,[Debit Amount] [DebitAmount]
      ,[Credit Amount] [CreditAmount]
      ,[Debit Amount (LCY)] [DebitAmountLCY]
      ,[Credit Amount (LCY)] [CreditAmountLCY]
      ,[Initial Entry Due Date] [InitialEntryDueDate]
      ,[Initial Entry Global Dim_ 1] [InitialEntryGlobalDim1]
      ,[Initial Entry Global Dim_ 2] [InitialEntryGlobalDim2]
      ,[Gen_ Bus_ Posting Group] [GenBusPostingGroup]
      ,[Gen_ Prod_ Posting Group] [GenProdPostingGroup]
      ,[Use Tax] [UseTax]
      ,[VAT Bus_ Posting Group] [VATBusPostingGroup]
      ,[VAT Prod_ Posting Group] [VATProdPostingGroup]
      ,[Initial Document Type] [InitialDocumentType]
      ,[Applied Cust_ Ledger Entry No_] [AppliedCustLedgerEntryNo]
      ,[Unapplied] [Unapplied]
      ,[Unapplied by Entry No_] [UnappliedByEntryNo]
      ,[Remaining Pmt_ Disc_ Possible] [RemainingPmtDiscPossible]
      ,[Max_ Payment Tolerance] [MaxPaymentTolerance]
      ,[Tax Jurisdiction Code] [TaxJurisdictionCode]
      ,[Application No_] [ApplicationNo]
      ,[Ledger Entry Amount] [LedgerEntryAmount]
      ,[Curr_ Adjmt_ G_L Account No_] [CurrAdjmtGLAccountNo]
      ,[ACY Customer Posting Group$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYCustomerPostingGroup]
    FROM [company_name].[dbo].[table_business_central_prefix$Detailed Cust_ Ledg_ Entry$id_business_central] A
    JOIN
    [company_name].[dbo].[table_business_central_prefix$Detailed Cust_ Ledg_ Entry$id_business_central$ext] B
    ON 
    A.[Entry No_]	= B.[Entry No_]
    """
    df = pd.read_sql(query, con=SQLSERVER_CONN_BC)
    
    # Convertir les colonnes UUID en string
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
            df[col] = df[col].astype(str)
    
    # Utilisation du chemin absolu pour sauvegarder le fichier parquet
    output_path = DATA_LAKE_PATH / 'raw' / 'detailed_cust_ledger_entry.parquet'
    df.to_parquet(str(output_path))
    logger.info(f"{len(df)} lignes extraites pour Detailed_Cust_Ledg_Entry.")
    return df
