from prefect import task, get_run_logger
import sys
from pathlib import Path

# Add parent directory to Python path
parent_dir = str(Path(__file__).parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from src.utils import *
from src.setup import *



@task(name="Load Customer", retries=3, retry_delay_seconds=10)
def load_customer(df):
    logger = get_run_logger()
    insert_df_bcp_dw(df,
    "dbo.Customer",
    batch_size=10000
    )
    logger.info(f"{len(df)} lignes chargées pour Customer.")
    return 
