from prefect import flow
from typing import List, Union
from tasks.Airbyte import Airbyte, sync
from tasks.Dbt import Dbt, pull_dbt_repo, dbt_deps, dbt_source_snapshot_freshness, dbt_run, dbt_test, dbt_docs_generate
from pydantic import BaseModel

class Flow(BaseModel):
    airbytes: Union[List[Airbyte], None]
    dbt: Union[Dbt, None]

@flow(flow_run_name='airbyte_flow')
def airbyte_flow(flow: Flow):
    for airbyte in flow.airbytes:
        sync(airbyte)

@flow(flow_run_name='dbt_flow')
def dbt_flow(flow: Flow):
    pull_dbt_repo(flow.dbt)
    dbt_deps(flow.dbt)
    dbt_source_snapshot_freshness(flow.dbt)
    dbt_run(flow.dbt)
    dbt_test(flow.dbt)

@flow(flow_run_name='airbyte_dbt_flow')
def airbyte_dbt_flow(flow: Flow):
    
    for airbyte in flow.airbytes:
        sync(airbyte)

    pull_dbt_repo(flow.dbt)
    dbt_deps(flow.dbt)
    dbt_source_snapshot_freshness(flow.dbt)
    dbt_run(flow.dbt)
    dbt_test(flow.dbt)