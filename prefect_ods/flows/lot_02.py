from prefect import flow, get_run_logger
import sys
from pathlib import Path

# Add prefect_ods to Python path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from tasks.truncate import truncate_table
from tasks.extract.postcode import extract_postcode
from tasks.load.postcode import load_postcode
from tasks.extract.vatbuspostinggroup import extract_vatbuspostinggroup
from tasks.load.vatbuspostinggroup import load_vatbuspostinggroup
from tasks.extract.vendorpostinggroup import extract_vendorpostinggroup
from tasks.load.vendorpostinggroup import load_vendorpostinggroup
from tasks.extract.contact import extract_contact
from tasks.load.contact import load_contact

@flow(name="Lot 02 - PostCode - VAT Bus Posting Group - Vendor Posting Group - Contact", retries=3, retry_delay_seconds=10)
def lot_02():
    logger = get_run_logger()
    logger.info("Job lot 02 Start")
    # PostCode
    truncate_table("dbo", "PostCode")
    df_postcode = extract_postcode()
    load_postcode(df_postcode)
    
    # VAT Business Posting Group
    truncate_table("dbo", "Vat_Business_Posting")
    df_vatbuspostinggroup = extract_vatbuspostinggroup()
    load_vatbuspostinggroup(df_vatbuspostinggroup)
    
    # Vendor Posting Group
    truncate_table("dbo", "Vendor_Posting_Group")
    df_vendorpostinggroup = extract_vendorpostinggroup()
    load_vendorpostinggroup(df_vendorpostinggroup)

    # Contact
    truncate_table("dbo", "Contact")
    df_contact = extract_contact()
    load_contact(df_contact)
    
    logger.info("Job lot 02 End")



if __name__ == "__main__":
    lot_02()
