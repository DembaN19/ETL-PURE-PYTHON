from prefect import task, get_run_logger
import pandas as pd
import uuid
from pathlib import Path
from config.settings import SQLSERVER_CONN_BC

# Chemin absolu vers le dossier data_lake dans prefect_ods
DATA_LAKE_PATH = Path(__file__).parent.parent.parent / 'data_lake'

@task(name="Extract Sales_Invoice_Line", retries=3, retry_delay_seconds=10)
def extract_sales_invoice_line():
    logger = get_run_logger()
    
    query = """
    SELECT T1.[Document No_] [DocumentNo]
	,T1.[Line No_] [LineNo]
	,T1.[Sell-to Customer No_] [SelltoCustomerNo]
	,T1.[Type] [Type]
	,T1.[No_] [No]
	,T1.[Location Code] [LocationCode]
	,T1.[Posting Group] [PostingGroup]
	,T1.[Shipment Date] [ShipmentDate]
	,T1.[Description] [Description]
	,T1.[Description 2] [Description2]
	,T1.[Unit of Measure] [UnitofMeasure]
	,T1.[Quantity] [Quantity]
	,T1.[Unit Price] [UnitPrice]
	,T1.[Unit Cost (LCY)] [UnitCostLCY]
	,T1.[VAT _] [VATPercent]
	,T1.[Line Discount _] [LineDiscountPercent]
	,T1.[Line Discount Amount] [LineDiscountAmount]
	,T1.[Amount] [Amount]
	,T1.[Amount Including VAT] [AmountIncludingVAT]
	,T1.[Allow Invoice Disc_] [AllowInvoiceDisc]
	,T1.[Gross Weight] [GrossWeight]
	,T1.[Net Weight] [NetWeight]
	,T1.[Units per Parcel] [UnitsPerParcel]
	,T1.[Unit Volume] [UnitVolume]
	,T1.[Appl_-to Item Entry] [ApplToItemEntry]
	,T1.[Shortcut Dimension 1 Code] [ShortcutDimension1Code]
	,T1.[Shortcut Dimension 2 Code] [ShortcutDimension2Code]
	,T1.[Customer Price Group] [CustomerPriceGroup]
	,T1.[Job No_] [JobNo]
	,T1.[Work Type Code] [WorkTypeCode]
	,T1.[Shipment No_] [ShipmentNo]
	,T1.[Shipment Line No_] [ShipmentLineNo]
	,T1.[Order No_] [OrderNo]
	,T1.[Order Line No_] [OrderLineNo]
	,T1.[Bill-to Customer No_] [BilltoCustomerNo]
	,T1.[Inv_ Discount Amount] [InvDiscountAmount]
	,T1.[Drop Shipment] [DropShipment]
	,T1.[Gen_ Bus_ Posting Group] [GenBusPostingGroup]
	,T1.[Gen_ Prod_ Posting Group] [GenProdPostingGroup]
	,T1.[VAT Calculation Type] [VATCalculationType]
	,T1.[Transaction Type] [TransactionType]
	,T1.[Transport Method] [TransportMethod]
	,T1.[Attached to Line No_] [AttachedtoLineNo]
	,T1.[Exit Point] [ExitPoint]
	,T1.[Area] [Area]
	,T1.[Transaction Specification] [TransactionSpecification]
	,T1.[Tax Category] [TaxCategory]
	,T1.[Tax Area Code] [TaxAreaCode]
	,T1.[Tax Liable] [TaxLiable]
	,T1.[Tax Group Code] [TaxGroupCode]
	,T1.[VAT Clause Code] [VATClauseCode]
	,T1.[VAT Bus_ Posting Group] [VATBusPostingGroup]
	,T1.[VAT Prod_ Posting Group] [VATProdPostingGroup]
	,T1.[Blanket Order No_] [BlanketOrderNo]
	,T1.[Blanket Order Line No_] [BlanketOrderLineNo]
	,T1.[VAT Base Amount] [VATBaseAmount]
	,T1.[Unit Cost] [UnitCost]
	,T1.[System-Created Entry] [SystemCreatedEntry]
	,T1.[Line Amount] [LineAmount]
	,T1.[VAT Difference] [VATDifference]
	,T1.[VAT Identifier] [VATIdentifier]
	,T1.[IC Partner Ref_ Type] [ICPartnerRefType]
	,T1.[IC Partner Reference] [ICPartnerReference]
	,T1.[Prepayment Line] [PrepaymentLine]
	,T1.[IC Partner Code] [ICPartnerCode]
	,T1.[Posting Date] [PostingDate]
	,T1.[Pmt_ Discount Amount] [PmtDiscountAmount]
	,T1.[Line Discount Calculation] [LineDiscountCalculation]
	,T1.[Dimension Set ID] [DimensionSetID]
	,T1.[Job Task No_] [JobTaskNo]
	,T1.[Job Contract Entry No_] [JobContractEntryNo]
	,T1.[Deferral Code] [DeferralCode]
	,T1.[Variant Code] [VariantCode]
	,T1.[Bin Code] [BinCode]
	,T1.[Qty_ per Unit of Measure] [QtyPerUnitofMeasure]
	,T1.[Unit of Measure Code] [UnitofMeasureCode]
	,T1.[Quantity (Base)] [QuantityBase]
	,T1.[FA Posting Date] [FAPostingDate]
	,T1.[Depreciation Book Code] [DepreciationBookCode]
	,T1.[Depr_ until FA Posting Date] [DeprUntilFAPostingDate]
	,T1.[Duplicate in Depreciation Book] [DuplicateinDepreciationBook]
	,T1.[Use Duplication List] [UseDuplicationList]
	,T1.[Responsibility Center] [ResponsibilityCenter]
	,T1.[Cross-Reference No_] [CrossReferenceNo]
	,T1.[Unit of Measure (Cross Ref_)] [UnitofMeasureCrossRef]
	,T1.[Cross-Reference Type] [CrossReferenceType]
	,T1.[Cross-Reference Type No_] [CrossReferenceTypeNo]
	,T1.[Item Category Code] [ItemCategoryCode]
	,T1.[Nonstock] [Nonstock]
	,T1.[Purchasing Code] [PurchasingCode]
	,T1.[Product Group Code] [ProductGroupCode]
	,T1.[Appl_-from Item Entry] [ApplFromItemEntry]
	,T1.[Return Reason Code] [ReturnReasonCode]
	,T1.[Allow Line Disc_] [AllowLineDisc]
	,T1.[Customer Disc_ Group] [CustomerDiscGroup]
	,T1.[Price description] [PriceDescription]
	,T2.[TM PGM ORG$067a6169-f228-4252-9802-cc5452726985] [TMPGMORG]
	,T2.[TM DateTime Created$067a6169-f228-4252-9802-cc5452726985] [TMDateTimeCreated]
	,T2.[TM Org Line No_$067a6169-f228-4252-9802-cc5452726985] [TMOrgLineNo]
	,T2.[TM Org Order No_$067a6169-f228-4252-9802-cc5452726985] [TMOrgOrderNo]
    FROM [company_name].[dbo].[table_business_central_prefix$Sales Invoice Line$id_business_central] T1
    INNER JOIN [company_name].[dbo].[table_business_central_prefix$Sales Invoice Line$id_business_central$ext]
    T2 ON T1.[Document No_] = T2.[Document No_]
        AND T1.[Line No_] = T2.[Line No_]
    """
    
    df = pd.read_sql(query, con=SQLSERVER_CONN_BC)
    
    # Convertir les colonnes UUID en string
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
            df[col] = df[col].astype(str)
    
    # Utilisation du chemin absolu pour sauvegarder le fichier parquet
    output_path = DATA_LAKE_PATH / 'raw' / 'sales_invoice_line.parquet'
    df.to_parquet(str(output_path))
    logger.info(f"{len(df)} lignes extraites pour Sales_Invoice_Line.")
    return df
