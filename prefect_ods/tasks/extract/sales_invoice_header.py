from prefect import task, get_run_logger
import pandas as pd
import uuid
from pathlib import Path
from config.settings import SQLSERVER_CONN_BC

# Chemin absolu vers le dossier data_lake dans prefect_ods
DATA_LAKE_PATH = Path(__file__).parent.parent.parent / 'data_lake'

@task(name="Extract Sales_Invoice_Header", retries=3, retry_delay_seconds=10)
def extract_sales_invoice_header():
    logger = get_run_logger()
    
    query = """
    SELECT A.[No_] [No]
	,[Sell-to Customer No_] [SelltoCustomerNo]
	,[Bill-to Customer No_] [BilltoCustomerNo]
	,[Bill-to Name] [BilltoName]
	,[Bill-to Name 2] [BilltoName2]
	,[Bill-to Address] [BilltoAddress]
	,[Bill-to Address 2] [BilltoAddress2]
	,[Bill-to City] [BilltoCity]
	,[Bill-to Contact] [BilltoContact]
	,[Your Reference] [YourReference]
	,[Ship-to Code] [ShiptoCode]
	,[Ship-to Name] [ShiptoName]
	,[Ship-to Name 2] [ShiptoName2]
	,[Ship-to Address] [ShiptoAddress]
	,[Ship-to Address 2] [ShiptoAddress2]
	,[Ship-to City] [ShiptoCity]
	,[Ship-to Contact] [ShiptoContact]
	,[Order Date] [OrderDate]
	,[Posting Date] [PostingDate]
	,[Shipment Date] [ShipmentDate]
	,[Posting Description] [PostingDescription]
	,[Payment Terms Code] [PaymentTermsCode]
	,[Due Date] [DueDate]
	,[Payment Discount _] [PaymentDiscountPercent]
	,[Pmt_ Discount Date] [PmtDiscountDate]
	,[Shipment Method Code] [ShipmentMethodCode]
	,[Location Code] [LocationCode]
	,[Shortcut Dimension 1 Code] [ShortcutDimension1Code]
	,[Shortcut Dimension 2 Code] [ShortcutDimension2Code]
	,[Customer Posting Group] [CustomerPostingGroup]
	,[Currency Code] [CurrencyCode]
	,[Currency Factor] [CurrencyFactor]
	,[Customer Price Group] [CustomerPriceGroup]
	,[Prices Including VAT] [PricesIncludingVAT]
	,[Invoice Disc_ Code] [InvoiceDiscCode]
	,[Customer Disc_ Group] [CustomerDiscGroup]
	,[Language Code] [LanguageCode]
	,[Salesperson Code] [SalespersonCode]
	,[Order No_] [OrderNo]
	,[No_ Printed] [NoPrinted]
	,[On Hold] [OnHold]
	,[Applies-to Doc_ Type] [AppliestoDocType]
	,[Applies-to Doc_ No_] [AppliestoDocNo]
	,[Bal_ Account No_] [BalAccountNo]
	,[VAT Registration No_] [VATRegistrationNo]
	,[Reason Code] [ReasonCode]
	,[Gen_ Bus_ Posting Group] [GenBusPostingGroup]
	,[EU 3-Party Trade] [EU3PartyTrade]
	,[Transaction Type] [TransactionType]
	,[Transport Method] [TransportMethod]
	,[VAT Country_Region Code] [VATCountryRegionCode]
	,[Sell-to Customer Name] [SelltoCustomerName]
	,[Sell-to Customer Name 2] [SelltoCustomerName2]
	,[Sell-to Address] [SelltoAddress]
	,[Sell-to Address 2] [SelltoAddress2]
	,[Sell-to City] [SelltoCity]
	,[Sell-to Contact] [SelltoContact]
	,[Bill-to Post Code] [BilltoPostCode]
	,[Bill-to County] [BilltoCounty]
	,[Bill-to Country_Region Code] [BilltoCountryRegionCode]
	,[Sell-to Post Code] [SelltoPostCode]
	,[Sell-to County] [SelltoCounty]
	,[Sell-to Country_Region Code] [SelltoCountryRegionCode]
	,[Ship-to Post Code] [ShiptoPostCode]
	,[Ship-to County] [ShiptoCounty]
	,[Ship-to Country_Region Code] [ShiptoCountryRegionCode]
	,[Bal_ Account Type] [BalAccountType]
	,[Exit Point] [ExitPoint]
	,[Correction] [Correction]
	,[Document Date] [DocumentDate]
	,[External Document No_] [ExternalDocumentNo]
	,[Area] [Area]
	,[Transaction Specification] [TransactionSpecification]
	,[Payment Method Code] [PaymentMethodCode]
	,[Shipping Agent Code] [ShippingAgentCode]
	,[Package Tracking No_] [PackageTrackingNo]
	,[Pre-Assigned No_ Series] [PreAssignedNoSeries]
	,[No_ Series] [NoSeries]
	,[Order No_ Series] [OrderNoSeries]
	,[Pre-Assigned No_] [PreAssignedNo]
	,[User ID] [UserID]
	,[Source Code] [SourceCode]
	,[Tax Area Code] [TaxAreaCode]
	,[Tax Liable] [TaxLiable]
	,[VAT Bus_ Posting Group] [VATBusPostingGroup]
	,[VAT Base Discount _] [VATBaseDiscountPercent]
	,[Invoice Discount Calculation] [InvoiceDiscountCalculation]
	,[Invoice Discount Value] [InvoiceDiscountValue]
	,[Prepayment No_ Series] [PrepaymentNoSeries]
	,[Prepayment Invoice] [PrepaymentInvoice]
	,[Prepayment Order No_] [PrepaymentOrderNo]
	,[Quote No_] [QuoteNo]
	,[Sell-to Phone No_] [SelltoPhoneNo]
	,[Sell-to E-Mail] [SelltoEmail]
	,[Payment Instructions] [PaymentInstructions]
	,[Payment Instructions Name] [PaymentInstructionsName]
	,[Work Description] [WorkDescription]
	,[Dimension Set ID] [DimensionSetID]
	,[Payment Service Set ID] [PaymentServiceSetID]
	,[Document Exchange Identifier] [DocumentExchangeIdentifier]
	,[Document Exchange Status] [DocumentExchangeStatus]
	,[Doc_ Exch_ Original Identifier] [DocExchOriginalIdentifier]
	,[Coupled to CRM] [CoupledtoCRM]
	,[Direct Debit Mandate ID] [DirectDebitMandateID]
	,[Cust_ Ledger Entry No_] [CustLedgerEntryNo]
	,[Campaign No_] [CampaignNo]
	,[Sell-to Contact No_] [SelltoContactNo]
	,[Bill-to Contact No_] [BilltoContactNo]
	,[Opportunity No_] [OpportunityNo]
	,[Responsibility Center] [ResponsibilityCenter]
	,[Allow Line Disc_] [AllowLineDisc]
	,[Get Shipment Used] [GetShipmentUsed]
	,[ACY Created By$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYCreatedBy]
	,[TM DateTime Created$067a6169-f228-4252-9802-cc5452726985] [TMDateTimeCreated]
	,[TM Origing Customer Inv_$067a6169-f228-4252-9802-cc5452726985] [TMOrigingCustomerInv]
	,[TM PGM ORG$067a6169-f228-4252-9802-cc5452726985] [TMPGMORG]
	,[TM SO Blocked$067a6169-f228-4252-9802-cc5452726985] [TMSOBlocked]
    FROM [company_name].[dbo].[table_business_central_prefix$Sales Invoice Header$id_business_central] A
    INNER JOIN [company_name].[dbo].[table_business_central_prefix$Sales Invoice Header$id_business_central$ext] B ON A.[No_] = B.[No_]
    """
    
    df = pd.read_sql(query, con=SQLSERVER_CONN_BC)
    
    # Convertir les colonnes UUID en string
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
            df[col] = df[col].astype(str)
    
    # Utilisation du chemin absolu pour sauvegarder le fichier parquet
    output_path = DATA_LAKE_PATH / 'raw' / 'sales_invoice_header.parquet'
    df.to_parquet(str(output_path))
    logger.info(f"{len(df)} lignes extraites pour Sales_Invoice_Header.")
    return df
