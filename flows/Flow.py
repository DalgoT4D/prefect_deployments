from prefect import flow
from typing import List
from tasks.Airbyte import Airbyte
from tasks.Dbt import Dbt
from pydantic import BaseModel

class Flow(BaseModel):
    airbytes: List[Airbyte] = None
    dbt: Dbt = None
    class Config:
        arbitrary_types_allowed=True

    def __init__(self, airbyte_arr: List[Airbyte], dbt: Dbt):
        super().__init__()

        self.airbytes = airbyte_arr
        self.dbt = dbt

    @flow
    def airbyte_flow(self):
        for airbyte in self.airbytes:
            airbyte.sync()

    @flow
    def dbt_flow(self):
        self.dbt.pull_dbt_repo()
        self.dbt.dbt_deps()
        self.dbt.dbt_source_snapshot_freshness()
        self.dbt.dbt_run()
        self.dbt.dbt_test()

    @flow
    def airbyte_dbt_flow(self):
        
        for airbyte in self.airbytes:
            airbyte.sync()

        self.dbt.pull_dbt_repo()
        self.dbt.dbt_deps()
        self.dbt.dbt_source_snapshot_freshness()
        self.dbt.dbt_run()
        self.dbt.dbt_test()