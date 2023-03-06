from pydantic import BaseModel
from prefect_airbyte.connections import trigger_sync

class Airbyte(BaseModel):
    connection_id: str = None

    def __init__(self, connection_id: str) -> None:
        super().__init__()
        
        self.connection_id = connection_id

    def sync(self) -> None:

        trigger_sync(
            connection_id=self.connection_id,
            poll_interval_s=15,
            status_updates=True
        )