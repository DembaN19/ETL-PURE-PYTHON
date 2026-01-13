from prefect import task, get_run_logger
import pandas as pd
import uuid
from pathlib import Path
from config.settings import SQLSERVER_CONN_BC

# Chemin absolu vers le dossier data_lake dans prefect_ods
DATA_LAKE_PATH = Path(__file__).parent.parent.parent / 'data_lake'

@task(name="Extract Customer", retries=3, retry_delay_seconds=10)
def extract_customer():
    logger = get_run_logger()

    query = """
    SELECT
       A.[No_],
      [Name],
      [Search Name] SearchName,
      [Name 2] Name2,
      [Address] Address,
      [Address 2] Address2,
      [City] City,
      [Contact] Contact,
      [Phone No_] PhoneNo_,
      [Telex No_] TelexNo_,
      [Document Sending Profile] DocumentSendingProfile,
      [Ship-to Code] ShipToCode,
      [Our Account No_] OurAccountNo_,
      [Territory Code] TerritoryCode,
      [Global Dimension 1 Code] GlobalDimension1Code,
      [Global Dimension 2 Code] GlobalDimension2Code,
      [Chain Name] ChainName,
      [Budgeted Amount] BudgetedAmount,
      [Credit Limit (LCY)] CreditLimitLCY,
      [Customer Posting Group] CustomerPostingGroup,
      [Currency Code] CurrencyCode,
      [Customer Price Group] CustomerPriceGroup,
      [Language Code] LanguageCode,
      [Statistics Group] StatisticsGroup,
      [Payment Terms Code] PaymentTermsCode,
      [Fin_ Charge Terms Code] FinChargeTermsCode,
      [Salesperson Code] SalespersonCode,
      [Shipment Method Code] ShipmentMethodCode,
      [Shipping Agent Code] ShippingAgentCode,
      [Place of Export] PlaceOfExport,
      [Invoice Disc_ Code] InvoiceDiscCode,
      [Customer Disc_ Group] CustomerDiscGroup,
      [Country_Region Code] CountryRegionCode,
      [Collection Method] CollectionMethod,
      [Amount] Amount,
      [Blocked] Blocked,
      [Invoice Copies] InvoiceCopies,
      [Last Statement No_] LastStatementNo_,
      [Print Statements] PrintStatements,
      [Bill-to Customer No_] BillToCustomerNo_,
      [Priority] Priority,
      [Payment Method Code] PaymentMethodCode,
      [Last Modified Date Time] LastModifiedDateTime,
      [Last Date Modified] LastDateModified,
      [Application Method] ApplicationMethod,
      [Prices Including VAT] PricesIncludingVAT,
      [Location Code] LocationCode,
      [Fax No_] FaxNo_,
      [Telex Answer Back] TelexAnswerBack,
      [VAT Registration No_] VATRegistrationNo_,
      [Combine Shipments] CombineShipments,
      [Gen_ Bus_ Posting Group] GenBusPostingGroup,
      [Picture] Picture,
      [GLN] GLN,
      [Post Code] PostCode,
      [County] County,
      [E-Mail] EMail,
      [Home Page] HomePage,
      [Reminder Terms Code] ReminderTermsCode,
      [No_ Series] NoSeries,
      [Tax Area Code] TaxAreaCode,
      [Tax Liable] TaxLiable,
      [VAT Bus_ Posting Group] VATBusPostingGroup,
      [Reserve] Reserve,
      [Block Payment Tolerance] BlockPaymentTolerance,
      [IC Partner Code] ICPartnerCode,
      [Prepayment _] Prepayment,
      [Partner Type] PartnerType,
      [Image] Image,
      [Privacy Blocked] PrivacyBlocked,
      [Disable Search by Name] DisableSearchByName,
      [Preferred Bank Account Code] PreferredBankAccountCode,
      [Cash Flow Payment Terms Code] CashFlowPaymentTermsCode,
      [Primary Contact No_] PrimaryContactNo_,
      [Contact Type] ContactType,
      [Responsibility Center] ResponsibilityCenter,
      [Shipping Advice] ShippingAdvice,
      [Shipping Time] ShippingTime,
      [Shipping Agent Service Code] ShippingAgentServiceCode,
      [Service Zone Code] ServiceZoneCode,
      [Allow Line Disc_] AllowLineDisc_,
      [Base Calendar Code] BaseCalendarCode,
      [Copy Sell-to Addr_ to Qte From] CopySelltoAddrToQteFrom,
      [Validate EU Vat Reg_ No_] ValidateEUVatRegNo_,
      [Id] Id,
      [Currency Id] CurrencyId,
      [Payment Terms Id] PaymentTermsId,
      [Shipment Method Id] ShipmentMethodId,
      [Payment Method Id] PaymentMethodId,
      [Tax Area ID] TaxAreaID,
      [Contact ID] ContactID,
      [Contact Graph Id] ContactGraphId,
      [Exclude from Payment Reporting] ExcludeFromPaymentReporting,
      [$systemId],
      [ACY Address 4$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYAddress4],
      [ACY Credit Message Code$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYCreditMessageCode],
      [ACY Parent Company$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYParentCompany],
      [ACY Grouping No_$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYGroupingNo_],
      [ACY Reference 5$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYReference5],
      [ACY Dossier Code$df74057f-5b58-4d6f-8a52-989845f6320c] [ACDYossierCode],
      [ACY XRM Reserve$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYXRMReserve],
      [ACY Source Customer$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYSourceCustomer],
      [ACY Top 50 Code$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYTop50Code],
      [ACY Invoice Grouping$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYInvoiceGrouping],
      [ACY Invoice Grouping 2$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYInvoiceGrouping2],
      [ACY PDF EDI$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYPDFEDI],
      [ACY Moore Express Activity$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYMooreExpressActivity],
      [ACY Invoice Edition Type$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYInvoiceEditionType],
      [ACY SIREN$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYSIREN],
      [ACY Credit Revision Last Date$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYCreditRevisionLastDate],
      [ACY Recovery Manager$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYRecoveryManager],
      [ACY Is RFA$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYIsRFA],
      [ACY Allow Delay$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYAllowDelay],
      [ACY APE Code$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYAPECode],
      [ACY Credit Limit$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYCreditLimit],
      [ACY RFA _$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYRFA],
      [ACY Exported To JDE$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYExportedToJDE],
      [ACY JDE Export Date_Time$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYJDEExportDateTime],
      [ACY Score Pattern$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYScorePattern],
      [ACY Current Score$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYCurrentScore],
      [ACY Factor Guaranteed Amount$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYFactorGuaranteedAmount],
      [ACY Risk Control Exemption$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYRiskControlExemption],
      [ACY XRM Information$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYXRMInformation],
      [ACY Exclude From Factor$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYExcludeFromFactor],
      [ACY Factor Approval Request$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYFactorApprovalRequest],
      [ACY Factor Appro_ Req_ Manual$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYFactorApproReqManual],
      [ACY Costing Center$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYCostingCenter],
      [ACY Creation User Code$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYCreationUserCode],
      [ACY XRM Export$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYXRMExport],
      [ACY Source No_$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYSourceNo_],
      [ACY Ref_ Invoice Type$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYRefInvoiceType],
      [ACY Typologie$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYTypologie],
      [ACY Olympe Info$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYOlympeInfo],
      [ACY Address 3$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYAddress3],
      [TM Cabinet Code$067a6169-f228-4252-9802-cc5452726985] [TMCabinetCode],
      [TM Contract Code$067a6169-f228-4252-9802-cc5452726985] [TMContractCode]
        FROM 
        [company_name].[dbo].[table_business_central_prefix$Customer$id_business_central] A
        LEFT JOIN 
        [company_name].[dbo].[table_business_central_prefix$Customer$id_business_central$ext] B
        ON A.No_ = B.No_
    """
    df = pd.read_sql(query, con=SQLSERVER_CONN_BC)
    
    # Convertir les colonnes UUID en string
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
            df[col] = df[col].astype(str)
    
    # Utilisation du chemin absolu pour sauvegarder le fichier parquet
    output_path = DATA_LAKE_PATH / 'raw' / 'customer.parquet'
    df.to_parquet(str(output_path))
    logger.info(f"{len(df)} lignes extraites pour Customer.")
    return df
