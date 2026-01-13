from prefect import task, get_run_logger
import pandas as pd
import uuid
from pathlib import Path
from config.settings import SQLSERVER_CONN_BC

# Chemin absolu vers le dossier data_lake dans prefect_ods
DATA_LAKE_PATH = Path(__file__).parent.parent.parent / 'data_lake'

@task(name="Extract Sales_CreMemo_Line", retries=3, retry_delay_seconds=10)
def extract_sales_crememo_line():
    logger = get_run_logger()
    
    query = """
    SELECT T1.[Document No_] [DocumentNo]
	,T1.[Line No_] [LineNo]
	,[Sell-to Customer No_] [SelltoCustomerNo]
	,[Type] [Type]
	,[No_] [No]
	,[Location Code] [LocationCode]
	,[Posting Group] [PostingGroup]
	,[Shipment Date] [ShipmentDate]
	,[Description] [Description]
	,[Description 2] [Description2]
	,[Unit of Measure] [UnitofMeasure]
	,[Quantity] [Quantity]
	,[Unit Price] [UnitPrice]
	,[Unit Cost (LCY)] [UnitCostLCY]
	,[VAT _] [VAT]
	,[Line Discount _] [LineDiscountPercent]
	,[Line Discount Amount] [LineDiscountAmount]
	,[Amount] [Amount]
	,[Amount Including VAT] [AmountIncludingVAT]
	,[Allow Invoice Disc_] [AllowInvoiceDisc]
	,[Gross Weight] [GrossWeight]
	,[Net Weight] [NetWeight]
	,[Units per Parcel] [UnitsPerParcel]
	,[Unit Volume] [UnitVolume]
	,[Appl_-to Item Entry] [ApplToItemEntry]
	,[Shortcut Dimension 1 Code] [ShortcutDimension1Code]
	,[Shortcut Dimension 2 Code] [ShortcutDimension2Code]
	,[Customer Price Group] [CustomerPriceGroup]
	,[Job No_] [JobNo]
	,[Work Type Code] [WorkTypeCode]
	,[Order No_] [OrderNo]
	,[Order Line No_] [OrderLineNo]
	,[Bill-to Customer No_] [BilltoCustomerNo]
	,[Inv_ Discount Amount] [InvDiscountAmount]
	,[Gen_ Bus_ Posting Group] [GenBusPostingGroup]
	,[Gen_ Prod_ Posting Group] [GenProdPostingGroup]
	,[VAT Calculation Type] [VATCalculationType]
	,[Transaction Type] [TransactionType]
	,[Transport Method] [TransportMethod]
	,[Attached to Line No_] [AttachedtoLineNo]
	,[Exit Point] [ExitPoint]
	,[Area] [Area]
	,[Transaction Specification] [TransactionSpecification]
	,[Tax Category] [TaxCategory]
	,[Tax Area Code] [TaxAreaCode]
	,[Tax Liable] [TaxLiable]
	,[Tax Group Code] [TaxGroupCode]
	,[VAT Clause Code] [VATClauseCode]
	,[VAT Bus_ Posting Group] [VATBusPostingGroup]
	,[VAT Prod_ Posting Group] [VATProdPostingGroup]
	,[Blanket Order No_] [BlanketOrderNo]
	,[Blanket Order Line No_] [BlanketOrderLineNo]
	,[VAT Base Amount] [VATBaseAmount]
	,[Unit Cost] [UnitCost]
	,[System-Created Entry] [SystemCreatedEntry]
	,[Line Amount] [LineAmount]
	,[VAT Difference] [VATDifference]
	,[VAT Identifier] [VATIdentifier]
	,[IC Partner Ref_ Type] [ICPartnerRefType]
	,[IC Partner Reference] [ICPartnerReference]
	,[Prepayment Line] [PrepaymentLine]
	,[IC Partner Code] [ICPartnerCode]
	,[Posting Date] [PostingDate]
	,[Pmt_ Discount Amount] [PmtDiscountAmount]
	,[Line Discount Calculation] [LineDiscountCalculation]
	,[Dimension Set ID] [DimensionSetID]
	,[Job Task No_] [JobTaskNo]
	,[Job Contract Entry No_] [JobContractEntryNo]
	,[Deferral Code] [DeferralCode]
	,[Variant Code] [VariantCode]
	,[Bin Code] [BinCode]
	,[Qty_ per Unit of Measure] [QtyPerUnitofMeasure]
	,[Unit of Measure Code] [UnitofMeasureCode]
	,[Quantity (Base)] [QuantityBase]
	,[FA Posting Date] [FAPostingDate]
	,[Depreciation Book Code] [DepreciationBookCode]
	,[Depr_ until FA Posting Date] [DeprUntilFAPostingDate]
	,[Duplicate in Depreciation Book] [DuplicateinDepreciationBook]
	,[Use Duplication List] [UseDuplicationList]
	,[Responsibility Center] [ResponsibilityCenter]
	,[Cross-Reference No_] [CrossReferenceNo]
	,[Unit of Measure (Cross Ref_)] [UnitofMeasureCrossRef]
	,[Cross-Reference Type] [CrossReferenceType]
	,[Cross-Reference Type No_] [CrossReferenceTypeNo]
	,[Item Category Code] [ItemCategoryCode]
	,[Nonstock] [Nonstock]
	,[Purchasing Code] [PurchasingCode]
	,[Product Group Code] [ProductGroupCode]
	,[Appl_-from Item Entry] [ApplFromItemEntry]
	,[Return Reason Code] [ReturnReasonCode]
	,[Allow Line Disc_] [AllowLineDisc]
	,[Customer Disc_ Group] [CustomerDiscGroup]
	,[TM PGM ORG$067a6169-f228-4252-9802-cc5452726985] [TMPGMORG]
	,[TM DateTime Created$067a6169-f228-4252-9802-cc5452726985] [TMDateTimeCreated]
	,[TM Reminder 1$067a6169-f228-4252-9802-cc5452726985] [TMReminder1]
	,[TM Reminder 2$067a6169-f228-4252-9802-cc5452726985] [TMReminder2]
    FROM [company_name].[dbo].[table_business_central_prefix$Sales Cr_Memo Line$id_business_central] T1
    INNER JOIN [company_name].[dbo].[table_business_central_prefix$Sales Cr_Memo Line$id_business_central$ext] T2 
    ON T1.[Document No_] = T2.[Document No_]
        AND T1.[Line No_] = T2.[Line No_]
    """
    
    df = pd.read_sql(query, con=SQLSERVER_CONN_BC)
    
    # Convertir les colonnes UUID en string
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
            df[col] = df[col].astype(str)
    
    # Utilisation du chemin absolu pour sauvegarder le fichier parquet
    output_path = DATA_LAKE_PATH / 'raw' / 'sales_crememo_line.parquet'
    df.to_parquet(str(output_path))
    logger.info(f"{len(df)} lignes extraites pour Sales_CreMemo_Line.")
    return df