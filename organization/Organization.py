import os
from typing import List
from pydantic import BaseModel
from prefect.deployments import Deployment
from prefect.server.models.deployments import read_deployments, delete_deployment
from prefect.server.schemas.filters import DeploymentFilter, DeploymentFilterTags
import asyncio
from sqlalchemy.orm import sessionmaker, Session

from tasks.Dbt import Dbt
from tasks.Airbyte import Airbyte
from flows.Flow import Flow

class Organization(BaseModel):
    name: str = None
    connection_ids: List[str] = []
    dbt_dir: str = None
    session: Session = None

    class Config:
        arbitrary_types_allowed=True

    def __init__(self, name: str, connection_ids: List[str], dbt_dir: str, session: Session):
        super().__init__()

        self.name = name
        self.connection_ids = connection_ids
        self.dbt_dir = dbt_dir
        
        self.session = session
        
    def deploy(self):
        airbyte_objs = []
        for connection_id in self.connection_ids:
            airbyte = Airbyte(connection_id=connection_id)
            airbyte_objs.append(airbyte)

        dbt_obj = Dbt(self.dbt_dir, os.getenv('DBT_VENV'))

        for airbyte_obj in airbyte_objs:

            flow = Flow(airbyte=airbyte_obj, dbt=dbt_obj, org_name=self.name)

            # Deploy a dbt flow
            Deployment.build_from_flow(
                flow=flow.dbt_flow.with_options(name=f'{self.name}_dbt_flow'),
                name=f"{self.name} - dbt",
                work_queue_name="ddp",
                tags = [self.name],
                apply=True
            )

            # Deploy a airbyte flow
            Deployment.build_from_flow(
                flow=flow.airbyte_flow.with_options(name=f'{self.name}_airbyte_flow'),
                name=f"{self.name} - airbyte",
                work_queue_name="ddp",
                tags = [airbyte.connection_id, self.name],
                apply=True,
            )

            # Deploy a airbyte + dbt flow
            Deployment.build_from_flow(
                flow=flow.airbyte_dbt_flow.with_options(name=f'{self.name}_airbyte_dbt_flow'),
                name=f"{self.name} - airbyte + dbt",
                work_queue_name="ddp",
                tags = [airbyte.connection_id, self.name],
                apply=True
            )

    async def reset_deployments(self):

        deploymentFilter = DeploymentFilterTags(all_=['stir'])

        deps = await read_deployments(self.session, deployment_filter=deploymentFilter)

        print(deps)

        for dep in deps:
            a = await delete_deployment(self.session, dep.id)
            print(a)

        return
    
    async def close_session(self):

        await self.session.close()

        return
