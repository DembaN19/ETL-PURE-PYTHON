from prefect import flow, get_run_logger
import sys
from pathlib import Path


# Add prefect_ods to Python path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from lot_01 import lot_01
from lot_02 import lot_02
from lot_03 import lot_03
from lot_04 import lot_04
from lot_06 import lot_06

@flow(name="ODS NAME", retries=3, retry_delay_seconds=10)
def ODS_SERVER():
    logger = get_run_logger()
    logger.info("Starting ODS NAME flow Global")
    lot_01()
    lot_02()
    lot_03()
    lot_04()
    lot_06()
    logger.info("ODS NAME flow Global End")

if __name__ == "__main__":
    ODS_SERVER()
