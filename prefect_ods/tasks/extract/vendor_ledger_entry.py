from prefect import task, get_run_logger
import pandas as pd
import uuid
from pathlib import Path
from config.settings import SQLSERVER_CONN_BC

# Chemin absolu vers le dossier data_lake dans prefect_ods
DATA_LAKE_PATH = Path(__file__).parent.parent.parent / 'data_lake'

@task(name="Extract Vendor_Ledger_Entry", retries=3, retry_delay_seconds=10)
def extract_vendor_ledger_entry():
    logger = get_run_logger()

    query = """
    SELECT 
      A.[Entry No_] [EntryNo]
      ,[Vendor No_] [VendorNo]
      ,[Posting Date] [PostingDate]
      ,[Document Type] [DocumentType]
      ,[Document No_] [DocumentNo]
      ,[Description] [Description]
      ,[Vendor Name] [VendorName]
      ,[Currency Code] [CurrencyCode]
      ,[Purchase (LCY)] [PurchaseLCY]
      ,[Inv_ Discount (LCY)] [InvDiscountLCY]
      ,[Buy-from Vendor No_] [BuyfromVendorNo]
      ,[Vendor Posting Group] [VendorPostingGroup]
      ,[Global Dimension 1 Code] [GlobalDimension1Code]
      ,[Global Dimension 2 Code] [GlobalDimension2Code]
      ,[Purchaser Code] [PurchaserCode]
      ,[User ID] [UserID]
      ,[Source Code] [SourceCode]
      ,[On Hold] [OnHold]
      ,[Applies-to Doc_ Type] [AppliestoDocType]
      ,[Applies-to Doc_ No_] [AppliestoDocNo]
      ,[Open] [Open]
      ,[Due Date] [DueDate]
      ,[Pmt_ Discount Date] [PmtDiscountDate]
      ,[Original Pmt_ Disc_ Possible] [OriginalPmtDiscPossible]
      ,[Pmt_ Disc_ Rcd_(LCY)] [PmtDiscRcdLCY]
      ,[Positive] [Positive]
      ,[Closed by Entry No_] [ClosedByEntryNo]
      ,[Closed at Date] [ClosedAtDate]
      ,[Closed by Amount] [ClosedByAmount]
      ,[Applies-to ID] [AppliesToID]
      ,[Journal Batch Name] [JournalBatchName]
      ,[Reason Code] [ReasonCode]
      ,[Bal_ Account Type] [BalAccountType]
      ,[Bal_ Account No_] [BalAccountNo]
      ,[Transaction No_] [TransactionNo]
      ,[Closed by Amount (LCY)] [ClosedByAmountLCY]
      ,[Document Date] [DocumentDate]
      ,[External Document No_] [ExternalDocumentNo]
      ,[No_ Series] [NoSeries]
      ,[Closed by Currency Code] [ClosedByCurrencyCode]
      ,[Closed by Currency Amount] [ClosedByCurrencyAmount]
      ,[Adjusted Currency Factor] [AdjustedCurrencyFactor]
      ,[Original Currency Factor] [OriginalCurrencyFactor]
      ,[Remaining Pmt_ Disc_ Possible] [RemainingPmtDiscPossible]
      ,[Pmt_ Disc_ Tolerance Date] [PmtDiscToleranceDate]
      ,[Max_ Payment Tolerance] [MaxPaymentTolerance]
      ,[Accepted Payment Tolerance] [AcceptedPaymentTolerance]
      ,[Accepted Pmt_ Disc_ Tolerance] [AcceptedPmtDiscTolerance]
      ,[Pmt_ Tolerance (LCY)] [PmtToleranceLCY]
      ,[Amount to Apply] [AmounttoApply]
      ,[IC Partner Code] [ICPartnerCode]
      ,[Applying Entry] [ApplyingEntry]
      ,[Reversed] [Reversed]
      ,[Reversed by Entry No_] [ReversedByEntryNo]
      ,[Reversed Entry No_] [ReversedEntryNo]
      ,[Prepayment] [Prepayment]
      ,[Creditor No_] [CreditorNo]
      ,[Payment Reference] [PaymentReference]
      ,[Payment Method Code] [PaymentMethodCode]
      ,[Applies-to Ext_ Doc_ No_] [AppliestoExtDocNo]
      ,[Recipient Bank Account] [RecipientBankAccount]
      ,[Message to Recipient] [MessageToRecipient]
      ,[Exported to Payment File] [ExportedToPaymentFile]
      ,[Dimension Set ID] [DimensionSetID]
      ,[$systemId] [systemId]
	  ,[ACY Order No_$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYOrderNo]
      ,[ACY Batch No_$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYBatchNo]
      ,[ACY Letter Code$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYLetterCode]
      ,[ACY Letter Date$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYLetterDate]
      ,[ACY Historic Letter$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYHistoricLetter]
    FROM [company_name].[dbo].[table_business_central_prefix$Vendor Ledger Entry$id_business_central] A
    INNER JOIN [company_name].[dbo].[table_business_central_prefix$Vendor Ledger Entry$id_business_central$ext] B
    ON A.[Entry No_] = B.[Entry No_]
    """
    df = pd.read_sql(query, con=SQLSERVER_CONN_BC)
    
    # Convertir les colonnes UUID en string
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
            df[col] = df[col].astype(str)
    
    # Utilisation du chemin absolu pour sauvegarder le fichier parquet
    output_path = DATA_LAKE_PATH / 'raw' / 'vendor_ledger_entry.parquet'
    df.to_parquet(str(output_path))
    logger.info(f"{len(df)} lignes extraites pour Vendor_Ledger_Entry.")
    return df
