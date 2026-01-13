from prefect import task, get_run_logger
from config.settings import SQLSERVER_CONN_DWH
from sqlalchemy import text

@task(name="Truncate Table", retries=3, retry_delay_seconds=10)
def truncate_table(schema: str, table: str):
    logger = get_run_logger()
    with SQLSERVER_CONN_DWH.connect() as connection:
        connection.execute(text(f"TRUNCATE TABLE {schema}.{table}"))
        connection.commit()
    logger.info(f"Table {schema}.{table} vidée.")
    return
