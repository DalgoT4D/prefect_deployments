from prefect import task
from pydantic import BaseModel
from prefect_airbyte.connections import trigger_sync

class Airbyte(BaseModel):
    connection_id: str

@task(name='airbyte_sync', task_run_name='airbyte_sync_{airbyte.connection_id}')
def sync(airbyte: Airbyte) -> None:

    trigger_sync(
        connection_id=airbyte.connection_id,
        poll_interval_s=15,
        status_updates=True
    )