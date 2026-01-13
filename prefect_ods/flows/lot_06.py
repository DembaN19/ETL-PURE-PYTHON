import sys
from prefect import flow, get_run_logger
from pathlib import Path

# Add prefect_ods to Python path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from tasks.truncate import truncate_table
from tasks.extract.sales_invoice_header import extract_sales_invoice_header
from tasks.load.sales_invoice_header import load_sales_invoice_header
from tasks.extract.sales_invoice_line import extract_sales_invoice_line
from tasks.load.sales_invoice_line import load_sales_invoice_line
from tasks.extract.sales_header import extract_sales_header
from tasks.load.sales_header import load_sales_header
from tasks.extract.sales_line import extract_sales_line
from tasks.load.sales_line import load_sales_line
from tasks.extract.sales_crememo_header import extract_sales_crememo_header
from tasks.load.sales_crememo_header import load_sales_crememo_header
from tasks.extract.sales_crememo_line import extract_sales_crememo_line
from tasks.load.sales_crememo_line import load_sales_crememo_line
from tasks.extract.dimension_entry import extract_dimension_entry
from tasks.load.dimension_entry import load_dimension_entry

@flow(name="Lot 06 - Sales_Invoice_Header - Sales_Invoice_Line - Sales_Header - Sales_Line - Sales_CreMemo_Header - Sales_CreMemo_Line - Dimension_Entry", retries=3, retry_delay_seconds=10)
def lot_06():
    logger = get_run_logger()
    logger.info("Job lot 06 Start")
    
    # Sales_Invoice_Header
    truncate_table("dbo", "Sales_Invoice_Header")
    df_sales_invoice_header = extract_sales_invoice_header()
    load_sales_invoice_header(df_sales_invoice_header)
    
    # Sales_Invoice_Line
    truncate_table("dbo", "Sales_Invoice_Line")
    df_sales_invoice_line = extract_sales_invoice_line()
    load_sales_invoice_line(df_sales_invoice_line)
    
    # Sales_Header
    truncate_table("dbo", "Sales_Header")
    df_sales_header = extract_sales_header()
    load_sales_header(df_sales_header)
    
    # Sales_Line
    truncate_table("dbo", "Sales_Line")
    df_sales_line = extract_sales_line()
    load_sales_line(df_sales_line)
    
    # Sales_CreMemo_Header
    truncate_table("dbo", "Sales_CreMemo_Header")
    df_sales_crememo_header = extract_sales_crememo_header()
    load_sales_crememo_header(df_sales_crememo_header)
    
    # Sales_CreMemo_Line
    truncate_table("dbo", "Sales_CreMemo_Line")
    df_sales_crememo_line = extract_sales_crememo_line()
    load_sales_crememo_line(df_sales_crememo_line)
    
    # Dimension_Set_Entry
    truncate_table("dbo", "Dimension_Set_Entry")
    df_dimension_entry = extract_dimension_entry()
    load_dimension_entry(df_dimension_entry)
    
    logger.info("Job lot 06 End")

if __name__ == "__main__":
    lot_06()
