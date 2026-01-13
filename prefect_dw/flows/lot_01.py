from prefect import flow, get_run_logger
import sys
from pathlib import Path

# Add prefect_dw to Python path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from tasks.truncate import truncate_table
from tasks.extract.customer import extract_customer
from tasks.extract.vendor import extract_vendor
from tasks.extract.salesperson import extract_salesperson
from tasks.load.customer import load_customer
from tasks.load.vendor import load_vendor
from tasks.load.salesperson import load_salesperson

@flow(name="DATAWAREHOUSE NAME Lot 01 - Customer - Vendor - Salesperson", retries=3, retry_delay_seconds=10)
def lot_01():
    logger = get_run_logger()
    logger.info("Job lot 01 Start")
    # Customer
    truncate_table("dbo", "Customer")
    df_customer = extract_customer()
    load_customer(df_customer)

    # Vendor
    truncate_table("dbo", "Vendor")
    df_vendor = extract_vendor()
    load_vendor(df_vendor)

    # Salesperson (sans différentiel)
    truncate_table("dbo", "Salesperson_Perchaser")
    df_sales = extract_salesperson()
    load_salesperson(df_sales)
    logger.info("Job lot 01 End")
if __name__ == "__main__":
    lot_01()
