from prefect import task, get_run_logger
import pandas as pd
import uuid
from pathlib import Path
from config.settings import SQLSERVER_CONN_BC

# Chemin absolu vers le dossier data_lake dans prefect_ods
DATA_LAKE_PATH = Path(__file__).parent.parent.parent / 'data_lake'

@task(name="Extract Bank_Account_Ledger_Entry", retries=3, retry_delay_seconds=10)
def extract_bank_account_ledger_entry():
    logger = get_run_logger()
    
    query = """
    SELECT 
    [Entry No_] [EntryNo],
    [Bank Account No_] [BankAccountNo],
    [Posting Date] [PostingDate],
    [Document Type] [DocumentType],
    [Document No_] [DocumentNo],
    [Description] [Description],
    [Currency Code] [CurrencyCode],
    [Amount] [Amount],
    [Remaining Amount] [RemainingAmount],
    [Amount (LCY)] [AmountLCY],
    [Bank Acc_ Posting Group] [BankAccPostingGroup],
    [Global Dimension 1 Code] [GlobalDimension1Code],
    [Global Dimension 2 Code] [GlobalDimension2Code],
    [Our Contact Code] [OurContactCode],
    [User ID] [UserID],
    [Source Code] [SourceCode],
    [Open] [Open],
    [Positive] [Positive],
    [Closed by Entry No_] [ClosedByEntryNo],
    [Closed at Date] [ClosedAtDate],
    [Journal Batch Name] [JournalBatchName],
    [Reason Code] [ReasonCode],
    [Bal_ Account Type] [BalAccountType],
    [Bal_ Account No_] [BalAccountNo],
    [Transaction No_] [TransactionNo],
    [Statement Status] [StatementStatus],
    [Statement No_] [StatementNo],
    [Statement Line No_] [StatementLineNo],
    [Debit Amount] [DebitAmount],
    [Credit Amount] [CreditAmount],
    [Debit Amount (LCY)] [DebitAmountLCY],
    [Credit Amount (LCY)] [CreditAmountLCY],
    [Document Date] [DocumentDate],
    [External Document No_] [ExternalDocumentNo],
    [Reversed] [Reversed],
    [Reversed by Entry No_] [ReversedByEntryNo],
    [Reversed Entry No_] [ReversedEntryNo],
    [Dimension Set ID] [DimensionSetID],
    [$systemId] [systemId]
    FROM [company_name].[dbo].[table_business_central_prefix$Bank Account Ledger Entry$id_business_central]
    """
    
    df = pd.read_sql(query, con=SQLSERVER_CONN_BC)
    
    # Convertir les colonnes UUID en string
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
            df[col] = df[col].astype(str)
    
    # Utilisation du chemin absolu pour sauvegarder le fichier parquet
    output_path = DATA_LAKE_PATH / 'raw' / 'bank_account_ledger_entry.parquet'
    df.to_parquet(str(output_path))
    logger.info(f"{len(df)} lignes extraites pour Bank_Account_Ledger_Entry.")
    return df

