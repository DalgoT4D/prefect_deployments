import os
import requests
from typing import List
from pydantic import BaseModel
from prefect.deployments import Deployment
from prefect.server.models.deployments import read_deployments, delete_deployment
from prefect.server.models.flows import read_flows, delete_flow
from prefect.server.schemas.filters import DeploymentFilterTags
from prefect.server.schemas.schedules import CronSchedule
from prefect.settings import PREFECT_UI_API_URL
from sqlalchemy.orm import Session

from tasks.Dbt import Dbt
from tasks.Airbyte import Airbyte
from flows.Flow import Flow

class Organization(BaseModel):
    name: str = None
    connection_ids: List[str] = []
    dbt_dir: str = None
    session: Session = None
    schedule: str = None

    class Config:
        arbitrary_types_allowed=True

    def __init__(self, name: str, connection_ids: List[str], dbt_dir: str, session: Session, schedule: str):
        super().__init__()

        self.name = name
        self.connection_ids = connection_ids
        self.dbt_dir = dbt_dir
        self.schedule = schedule
        
        self.session = session
        
    async def deploy(self):
        try:
            airbyte_objs: List[Airbyte] = []
            for connection_id in self.connection_ids:
                airbyte = Airbyte(connection_id=connection_id)
                airbyte_objs.append(airbyte)

            dbt_obj = Dbt(self.dbt_dir, os.getenv('DBT_VENV'))

            flow = Flow(airbyte=None, dbt=dbt_obj, org_name=self.name)

            # Deploy a dbt flow
            deployment = await Deployment.build_from_flow(
                flow=flow.dbt_flow.with_options(name=f'{self.name}_dbt_flow'),
                name=f"{self.name} - dbt",
                work_queue_name=os.getenv('PREFECT_WORK_QUEUE'),
                tags = [self.name],
            )
            if self.schedule:
                deployment.schedule = CronSchedule(cron = self.schedule)
            await deployment.apply()

            for airbyte_obj in airbyte_objs:

                flow.airbyte = airbyte_obj

                # Deploy a airbyte flow
                deployment = await Deployment.build_from_flow(
                    flow=flow.airbyte_flow.with_options(name=f'{self.name}_airbyte_flow'),
                    name=f"{self.name} - airbyte",
                    work_queue_name=os.getenv('PREFECT_WORK_QUEUE'),
                    tags = [airbyte_obj.connection_id, self.name],
                )
                if self.schedule:
                    deployment.schedule = CronSchedule(cron = self.schedule)
                await deployment.apply()

                # Deploy a airbyte + dbt flow
                deployment = await Deployment.build_from_flow(
                    flow=flow.airbyte_dbt_flow.with_options(name=f'{self.name}_airbyte_dbt_flow'),
                    name=f"{self.name} - airbyte + dbt",
                    work_queue_name=os.getenv('PREFECT_WORK_QUEUE'),
                    tags = [airbyte_obj.connection_id, self.name],
                )
                if self.schedule:
                    deployment.schedule = CronSchedule(cron = self.schedule)
                await deployment.apply()

        except Exception as e:

            print(e)

            await self.close_session()

    async def reset_deployments(self):

        try:

            deploymentFilter = DeploymentFilterTags(all_=[self.name])

            flows = await read_flows(self.session, deployment_filter=deploymentFilter)

            api_url = PREFECT_UI_API_URL.value()

            # delete a flow removes the deployments too
            for flow in flows:
                requests.delete(api_url + f'/flows/{flow.id}')
                # the code below doesn't seem to work
                # await delete_flow(self.session, flow.id)

            await self.deploy()

            return
        
        except Exception as e:

            print(e)

            await self.close_session()

    async def close_session(self):

        await self.session.close()

        return
