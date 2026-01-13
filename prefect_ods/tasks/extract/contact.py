from prefect import task, get_run_logger
import pandas as pd
import uuid
from pathlib import Path
from config.settings import SQLSERVER_CONN_BC

# Chemin absolu vers le dossier data_lake dans prefect_ods
DATA_LAKE_PATH = Path(__file__).parent.parent.parent / 'data_lake'

@task(name="Extract Contact", retries=3, retry_delay_seconds=10)
def extract_contact():
    logger = get_run_logger()

    query = """
    SELECT 
    t1.[No_] AS No_,
    t1.[Name] AS Name,
    t1.[Search Name] AS SearchName,
    t1.[Name 2] AS Name2,
    t1.[Address] AS Address,
    t1.[Address 2] AS Address2,
    t1.[City] AS City,
    t1.[Phone No_] AS PhoneNo,
    t1.[Telex No_] AS TelexNo,
    t1.[Territory Code] AS TerritoryCode,
    t1.[Currency Code] AS CurrencyCode,
    t1.[Language Code] AS LanguageCode,
    t1.[Salesperson Code] AS SalespersonCode,
    t1.[Country_Region Code] AS CountryRegionCode,
    t1.[Last Date Modified] AS LastDateModified,
    t1.[Fax No_] AS FaxNo,
    t1.[Telex Answer Back] AS TelexAnswerBack,
    t1.[VAT Registration No_] AS VATRegistrationNo,
    t1.[Post Code] AS PostCode,
    t1.[County] AS County,
    t1.[E-Mail] AS EMail,
    t1.[Home Page] AS HomePage,
    t1.[No_ Series] AS NoSeries,
    t1.[Privacy Blocked] AS PrivacyBlocked,
    t1.[Minor] AS Minor,
    t1.[Parental Consent Received] AS ParentalConsentReceived,
    t1.[Type] AS Type,
    t1.[Company No_] AS CompanyNo,
    t1.[Company Name] AS CompanyName,
    t1.[Lookup Contact No_] AS LookupContactNo,
    t1.[First Name] AS FirstName,
    t1.[Middle Name] AS MiddleName,
    t1.[Surname] AS Surname,
    t1.[Job Title] AS JobTitle,
    t1.[Initials] AS Initials,
    t1.[Extension No_] AS ExtensionNo,
    t1.[Mobile Phone No_] AS MobilePhoneNo,
    t1.[Pager] AS Pager,
    t1.[Organizational Level Code] AS OrganizationalLevelCode,
    t1.[Exclude from Segment] AS ExcludefromSegment,
    t1.[External ID] AS ExternalID,
    t1.[Correspondence Type] AS CorrespondenceType,
    t1.[Salutation Code] AS SalutationCode,
    t1.[Search E-Mail] AS SearchEMail,
    t1.[Last Time Modified] AS LastTimeModified,
    t1.[E-Mail 2] AS EMail2,
    t1.[Trade Register] AS TradeRegister,
    t1.[APE Code] AS APECode,
    t1.[Legal Form] AS LegalForm,
    t1.[Stock Capital] AS StockCapital,

    t2.[TM Email Address Option$067a6169-f228-4252-9802-cc5452726985] AS TMEmailAddresseOption,
    t2.[TM Email Reporting Option$067a6169-f228-4252-9802-cc5452726985] AS TMEmailReportingOption,
    t2.[TM Deliv Copy Mail Option$067a6169-f228-4252-9802-cc5452726985] AS TMDelivCopyMailOption,
    t2.[TM Deliv Copy Mail Aff Option$067a6169-f228-4252-9802-cc5452726985] AS TMDelivCopyMailAffOption,
    t2.[TM Acces company_name AR Option$067a6169-f228-4252-9802-cc5452726985] AS TMAccesscompany_nameAROption,
    t2.[TM Reminder O_N Option$067a6169-f228-4252-9802-cc5452726985] AS TMReminderONOption,

    t3.[No_] AS CustomerNo,
    t3.[Business Relation Code] AS BusinessRelationCode,
    t3.[Link to Table] AS LinktoTable

    FROM company_name.dbo.[table_business_central_prefix$Contact$id_business_central] t1
    LEFT JOIN company_name.dbo.[table_business_central_prefix$Contact$id_business_central$ext] t2
        ON t1.[No_] = t2.[No_]
    LEFT JOIN company_name.dbo.[table_business_central_prefix$Contact Business Relation$id_business_central] t3
        ON t1.[Company No_] = t3.[Contact No_]

    """
    df = pd.read_sql(query, con=SQLSERVER_CONN_BC)
    
    # Convertir les colonnes UUID en string
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
            df[col] = df[col].astype(str)
    
    # Utilisation du chemin absolu pour sauvegarder le fichier parquet
    output_path = DATA_LAKE_PATH / 'raw' / 'contact.parquet'
    df.to_parquet(str(output_path))
    logger.info(f"{len(df)} lignes extraites pour Contact.")
    return df
