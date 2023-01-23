from prefect import flow
from prefect_airbyte.connections import trigger_sync
from dotenv import load_dotenv
import os

load_dotenv()

@flow(name="airbyte-sync")
def run_airbyte_sync():
    trigger_sync(
        connection_id=os.getenv('DOST_AIRBYTE_CONNECTION'),
        poll_interval_s=3,
        status_updates=True
    )
    return 1