from prefect import task, get_run_logger
import pandas as pd
import uuid
from pathlib import Path
from config.settings import SQLSERVER_CONN_BC

# Chemin absolu vers le dossier data_lake dans prefect_ods
DATA_LAKE_PATH = Path(__file__).parent.parent.parent / 'data_lake'

@task(name="Extract Cust_Ledger_Entry", retries=3, retry_delay_seconds=10)
def extract_cust_ledger_entry():
    logger = get_run_logger()

    query = """
    SELECT A.[Entry No_] [EntryNo]
	,[Customer No_] [CustomerNo]
	,[Posting Date] [PostingDate]
	,[Document Type] [DocumentType]
	,[Document No_] [DocumentNo]
	,[Description] [Description]
	,[Customer Name] [CustomerName]
	,[Currency Code] [CurrencyCode]
	,[Sales (LCY)] [SalesLCY]
	,[Profit (LCY)] [ProfitLCY]
	,[Inv_ Discount (LCY)] [InvDiscountLCY]
	,[Sell-to Customer No_] [SelltoCustomerNo]
	,[Customer Posting Group] [CustomerPostingGroup]
	,[Global Dimension 1 Code] [GlobalDimension1Code]
	,[Global Dimension 2 Code] [GlobalDimension2Code]
	,D.[Dimension Value Code] [GlobalDimension3Code]
	,E.[Dimension Value Code] [GlobalDimension4Code]
	,[Salesperson Code] [SalespersonCode]
	,[User ID] [UserID]
	,[Source Code] [SourceCode]
	,[On Hold] [OnHold]
	,[Applies-to Doc_ Type] [AppliestoDocType]
	,[Applies-to Doc_ No_] [AppliestoDocNo]
	,[Open] [Open]
	,[Due Date] [DueDate]
	,[Pmt_ Discount Date] [PmtDiscountDate]
	,[Original Pmt_ Disc_ Possible] [OriginalPmtDiscPossible]
	,[Pmt_ Disc_ Given (LCY)] [PmtDiscGivenLCY]
	,[Positive] [Positive]
	,[Closed by Entry No_] [ClosedByEntryNo]
	,[Closed at Date] [ClosedAtDate]
	,[Closed by Amount] [ClosedByAmount]
	,[Applies-to ID] [AppliestoID]
	,[Journal Batch Name] [JournalBatchName]
	,[Reason Code] [ReasonCode]
	,[Bal_ Account Type] [BalAccountType]
	,[Bal_ Account No_] [BalAccountNo]
	,[Transaction No_] [TransactionNo]
	,[Closed by Amount (LCY)] [ClosedByAmountLCY]
	,[Document Date] [DocumentDate]
	,[External Document No_] [ExternalDocumentNo]
	,[Calculate Interest] [CalculateInterest]
	,[Closing Interest Calculated] [ClosingInterestCalculated]
	,[No_ Series] [NoSeries]
	,[Closed by Currency Code] [ClosedByCurrencyCode]
	,[Closed by Currency Amount] [ClosedByCurrencyAmount]
	,[Adjusted Currency Factor] [AdjustedCurrencyFactor]
	,[Original Currency Factor] [OriginalCurrencyFactor]
	,[Remaining Pmt_ Disc_ Possible] [RemainingPmtDiscPossible]
	,[Pmt_ Disc_ Tolerance Date] [PmtDiscToleranceDate]
	,[Max_ Payment Tolerance] [MaxPaymentTolerance]
	,[Last Issued Reminder Level] [LastIssuedReminderLevel]
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
	,[Payment Method Code] [PaymentMethodCode]
	,[Applies-to Ext_ Doc_ No_] [AppliestoExtDocNo]
	,[Recipient Bank Account] [RecipientBankAccount]
	,[Message to Recipient] [MessageToRecipient]
	,[Exported to Payment File] [ExportedToPaymentFile]
	,A.[Dimension Set ID] [DimensionSetID]
	,[Direct Debit Mandate ID] [DirectDebitMandateID]
    ,A.[$systemId] [systemId]
	,[ACY Receipt No_$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYReceiptNo]
	,[ACY Order No_$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYOrderNo]
	,[ACY Batch No_$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYBatchNo]
	,[ACY Letter Code$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYLetterCode]
	,[ACY Letter Date$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYLetterDate]
	,[ACY Historic Letter$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYHistoricLetter]
    FROM [company_name].[dbo].[table_business_central_prefix$Cust_ Ledger Entry$id_business_central] A
    LEFT JOIN [company_name].[dbo].[table_business_central_prefix$Cust_ Ledger Entry$id_business_central$ext] B ON A.[Entry No_] = B.[Entry No_]
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
    output_path = DATA_LAKE_PATH / 'raw' / 'cust_ledger_entry.parquet'
    df.to_parquet(str(output_path))
    logger.info(f"{len(df)} lignes extraites pour Cust_Ledger_Entry.")
    return df
