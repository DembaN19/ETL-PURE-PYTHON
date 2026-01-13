from prefect import task, get_run_logger
import pandas as pd
import uuid
from pathlib import Path
from config.settings import SQLSERVER_CONN_BC

# Chemin absolu vers le dossier data_lake dans prefect_ods
DATA_LAKE_PATH = Path(__file__).parent.parent.parent / 'data_lake'

@task(name="Extract Vendor", retries=3, retry_delay_seconds=10)
def extract_vendor():
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
      [Our Account No_] OurAccountNo_,
      [Territory Code] TerritoryCode,
      [Global Dimension 1 Code] GlobalDimension1Code,
      [Global Dimension 2 Code] GlobalDimension2Code,
      [Budgeted Amount] BudgetedAmount,
      [Vendor Posting Group] VendorPostingGroup,
      [Currency Code] CurrencyCode,
      [Language Code] LanguageCode,
      [Statistics Group] StatisticsGroup,
      [Payment Terms Code] PaymentTermsCode,
      [Fin_ Charge Terms Code] FinChargeTermsCode,
      [Purchaser Code] PurchaserCode,
      [Shipment Method Code] ShipmentMethodCode,
      [Shipping Agent Code] ShippingAgentCode,
      [Invoice Disc_ Code] InvoiceDiscCode,
      [Country_Region Code] CountryRegionCode,
      [Blocked] Blocked,
      [Pay-to Vendor No_] PayToVendorNo_,
      [Priority] Priority,
      [Payment Method Code] PaymentMethodCode,
      [Last Modified Date Time] LastModifiedDateTime,
      [Last Date Modified] LastDateModified,
      [Application Method] ApplicationMethod,
      [Prices Including VAT] PricesIncludingVAT,
      [Fax No_] FaxNo_,
      [Telex Answer Back] TelexAnswerBack,
      [VAT Registration No_] VATRegistrationNo_,
      [Gen_ Bus_ Posting Group] GenBusPostingGroup,
      [Picture] Picture,
      [GLN] GLN,
      [Post Code] PostCode,
      [County] County,
      [E-Mail] EMail,
      [Home Page] HomePage,
      [No_ Series] NoSeries,
      [Tax Area Code] TaxAreaCode,
      [Tax Liable] TaxLiable,
      [VAT Bus_ Posting Group] VATBusPostingGroup,
      [Block Payment Tolerance] BlockPaymentTolerance,
      [IC Partner Code] ICPartnerCode,
      [Prepayment _] Prepayment,
      [Partner Type] PartnerType,
      [Image] Image,
      [Privacy Blocked] PrivacyBlocked,
      [Disable Search by Name] DisableSearchbyName,
      [Creditor No_] CreditorNo_,
      [Preferred Bank Account Code] PreferredBankAccountCode,
      [Cash Flow Payment Terms Code] CashFlowPaymentTermsCode,
      [Primary Contact No_] PrimaryContactNo_,
      [Responsibility Center] ResponsibilityCenter,
      [Location Code] LocationCode,
      [Lead Time Calculation] LeadTimeCalculation,
      [Base Calendar Code] BaseCalendarCode,
      [Document Sending Profile] DocumentSendingProfile,
      [Validate EU Vat Reg_ No_] ValidateEUVatRegNo_,
      [Id] Id,
      [Currency Id] CurrencyId,
      [Payment Terms Id] PaymentTermsId,
      [Payment Method Id] PaymentMethodId,
      [Exclude from Payment Reporting] ExcludefromPaymentReporting,
      [$systemId],
      [ACY Address 4$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYAddress4],
      [ACY SIREN$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYSIREN],
      [ACY Is RFA$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYIsRFA],
      [ACY APE Code$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYAPECode],
      [ACY Vendor Category$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYVendorCategory],
      [ACY Vendor Sub-Category$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYVendorSubCategory],
      [ACY Sub-Group$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYSubGroup],
      [ACY Vendor Activity$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYVendorActivity],
      [ACY Factor Vendor No_$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYFactorVendorNo_],
      [ACY RFA _$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYRFA_],
      [ACY External Customer Code$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYExternalCustomerCode],
      [ACY Documalis Code$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYDocumalisCode],
      [ACY Exported To JDE$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYExportedToJDE],
      [ACY JDE Export Date_Time$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYJDEExportDateTime],
      [ACY Sell-To Vendor No_$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYSellToVendorNo_],
      [ACY Address 3$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYAddress3],
      [ACY Creation User Code$df74057f-5b58-4d6f-8a52-989845f6320c] [ACYCreationUserCode]
        FROM
        [company_name].[dbo].[table_business_central_prefix$Vendor$id_business_central] A
        INNER JOIN
        [company_name].[dbo].[table_business_central_prefix$Vendor$id_business_central$ext] B
        ON A.No_ = B.No_
    """
    df = pd.read_sql(query, con=SQLSERVER_CONN_BC)
    
    # Convertir les colonnes UUID en string
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
            df[col] = df[col].astype(str)
    
    # Utilisation du chemin absolu pour sauvegarder le fichier parquet
    output_path = DATA_LAKE_PATH / 'raw' / 'vendor.parquet'
    df.to_parquet(str(output_path))
    logger.info(f"{len(df)} lignes extraites pour Vendor.")
    return df