from prefect import flow, get_run_logger
import sys
from pathlib import Path

# Add prefect_dw to Python path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from tasks.truncate import truncate_table
from tasks.extract.vat_entry import extract_vat_entry
from tasks.load.vat_entry import load_vat_entry
from tasks.extract.vendor_ledger_entry import extract_vendor_ledger_entry
from tasks.load.vendor_ledger_entry import load_vendor_ledger_entry
from tasks.extract.cust_ledger_entry import extract_cust_ledger_entry
from tasks.load.cust_ledger_entry import load_cust_ledger_entry
from tasks.extract.g_lentry import extract_g_lentry
from tasks.load.g_lentry import load_g_lentry
from tasks.extract.detailed_cust_ledger_entry import extract_detailed_cust_ledger_entry
from tasks.load.detailed_cust_ledger_entry import load_detailed_cust_ledger_entry
from tasks.extract.detailed_vendor_ledger_entry import extract_detailed_vendor_ledger_entry
from tasks.load.detailed_vendor_ledger_entry import load_detailed_vendor_ledger_entry

@flow(name="DATAWAREHOUSE NAME Lot 03 - Vat_Entry - Vendor_Ledger_Entry - Cust_Ledger_Entry - G_LEntry - Detailed_Cust_Ledg_Entry - Detailed_Vendor_Ledg_Entry", retries=3, retry_delay_seconds=10)
def lot_03():
    logger = get_run_logger()
    logger.info("Job lot 03 Start")
    
    # Vat_Entry
    truncate_table("dbo", "Vat_Entry")
    df_vat_entry = extract_vat_entry()
    load_vat_entry(df_vat_entry)
    
    # Vendor_Ledger_Entry
    truncate_table("dbo", "Vendor_Ledger_Entry")
    df_vendor_ledger_entry = extract_vendor_ledger_entry()
    load_vendor_ledger_entry(df_vendor_ledger_entry)
    
    # Cust_Ledger_Entry
    truncate_table("dbo", "Cust_Ledger_Entry")
    df_cust_ledger_entry = extract_cust_ledger_entry()
    load_cust_ledger_entry(df_cust_ledger_entry)
    
    # G_LEntry
    truncate_table("dbo", "G_LEntry")
    df_g_lentry = extract_g_lentry()
    load_g_lentry(df_g_lentry)
    
    # Detailed_Cust_Ledg_Entry
    truncate_table("dbo", "Detailed_Cust_Ledg_Entry")
    df_detailed_cust_ledger_entry = extract_detailed_cust_ledger_entry()
    load_detailed_cust_ledger_entry(df_detailed_cust_ledger_entry)
    
    # Detailed_Vendor_Ledg_Entry
    truncate_table("dbo", "Detailed_Vendor_Ledg_Entry")
    df_detailed_vendor_ledger_entry = extract_detailed_vendor_ledger_entry()
    load_detailed_vendor_ledger_entry(df_detailed_vendor_ledger_entry)
    
    logger.info("Job lot 03 End")

if __name__ == "__main__":
    lot_03()
