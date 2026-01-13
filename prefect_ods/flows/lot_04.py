import sys
from prefect import flow, get_run_logger
from pathlib import Path

# Add prefect_ods to Python path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from tasks.truncate import truncate_table
from tasks.extract.customer_bank_account import extract_customer_bank_account
from tasks.load.customer_bank_account import load_customer_bank_account
from tasks.extract.bank_account_posting_group import extract_bank_account_posting_group
from tasks.load.bank_account_posting_group import load_bank_account_posting_group
from tasks.extract.bank_account_ledger_entry import extract_bank_account_ledger_entry
from tasks.load.bank_account_ledger_entry import load_bank_account_ledger_entry
from tasks.extract.bank_account_statement_line import extract_bank_account_statement_line
from tasks.load.bank_account_statement_line import load_bank_account_statement_line

@flow(name="Lot 04 - Customer_Bank_Account - Bank_Account_Posting_Group - Bank_Account_Ledger_Entry - Bank_Account_Statement_Line", retries=3, retry_delay_seconds=10)
def lot_04():
    logger = get_run_logger()
    logger.info("Job lot 04 Start")
    
    # Customer_Bank_Account
    truncate_table("dbo", "Customer_Bank_Account")
    df_customer_bank_account = extract_customer_bank_account()
    load_customer_bank_account(df_customer_bank_account)
    
    # Bank_Account_Posting_Group
    truncate_table("dbo", "Bank_Account_Posting_Group")
    df_bank_account_posting_group = extract_bank_account_posting_group()
    load_bank_account_posting_group(df_bank_account_posting_group)
    
    # Bank_Account_Ledger_Entry
    truncate_table("dbo", "Bank_Account_Ledger_Entry")
    df_bank_account_ledger_entry = extract_bank_account_ledger_entry()
    load_bank_account_ledger_entry(df_bank_account_ledger_entry)
    
    # Bank_Account_Statement_Line
    truncate_table("dbo", "Bank_Account_Statement_Line")
    df_bank_account_statement_line = extract_bank_account_statement_line()
    load_bank_account_statement_line(df_bank_account_statement_line)
    
    logger.info("Job lot 04 End")

if __name__ == "__main__":
    lot_04()
