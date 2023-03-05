from prefect import flow
from typing import List
from tasks.Airbyte import Airbyte
from tasks.Dbt import Dbt
from pydantic import BaseModel

class Flow(BaseModel):
    airbytes: List[Airbyte] = None
    dbt: Dbt = None
    org_name: str = None
    class Config:
        arbitrary_types_allowed=True

    def __init__(self, airbyte_arr: List[Airbyte], dbt: Dbt, org_name: str):
        super().__init__()

        self.airbytes = airbyte_arr
        self.dbt = dbt
        self.org_name = org_name

    @flow(name=f'{org_name} airbyte_flow')
    def airbyte_flow(self):
        for airbyte in self.airbytes:
            airbyte.sync()

    @flow(name='dbt_flow')
    def dbt_flow(self):
        self.dbt.pull_dbt_repo()
        self.dbt.dbt_deps()
        self.dbt.dbt_source_snapshot_freshness()
        self.dbt.dbt_run()
        self.dbt.dbt_test()

    @flow(name='airbyte_dbt_flow')
    def airbyte_dbt_flow(self):
        
        for airbyte in self.airbytes:
            airbyte.sync()

        self.dbt.pull_dbt_repo()
        self.dbt.dbt_deps()
        self.dbt.dbt_source_snapshot_freshness()
        self.dbt.dbt_run()
        self.dbt.dbt_test()