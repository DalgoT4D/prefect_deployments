from prefect import flow
from typing import List, Union
from tasks.Airbyte import Airbyte
from tasks.Dbt import Dbt
from pydantic import BaseModel

class Flow(BaseModel):
    airbytes: Union[List[Airbyte], None]
    dbt: Union[Dbt, None]

@flow
def airbyte_flow(flow: Flow):
    for airbyte in flow.airbytes:
        airbyte.sync()

@flow
def dbt_flow(flow: Flow):
    flow.dbt.pull_dbt_repo()
    flow.dbt.dbt_deps()
    flow.dbt.dbt_source_snapshot_freshness()
    flow.dbt.dbt_run()
    flow.dbt.dbt_test()

@flow
def airbyte_dbt_flow(flow: Flow):
    
    for airbyte in flow.airbytes:
        airbyte.sync()

    flow.dbt.pull_dbt_repo()
    flow.dbt.dbt_deps()
    flow.dbt.dbt_source_snapshot_freshness()
    flow.dbt.dbt_run()
    flow.dbt.dbt_test()