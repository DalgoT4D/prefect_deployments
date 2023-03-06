import os
import requests
from typing import List, Mapping, Union
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
    pipelines: List = None
    session: Session = None

    class Config:
        arbitrary_types_allowed=True

    def __init__(self, name: str, session: Session, pipelines: List):
        super().__init__()

        self.name = name
        self.pipelines = pipelines
        self.session = session
        
    async def deploy(self, reset = False):
        try:
            
            if reset:

                deploymentFilter = DeploymentFilterTags(all_=[self.name])
                flows = await read_flows(self.session, deployment_filter=deploymentFilter)
                api_url = PREFECT_UI_API_URL.value()

                # delete a flow removes the deployments too
                for flow in flows:
                    requests.delete(api_url + f'/flows/{flow.id}')
                    # the code below doesn't seem to work
                    # await delete_flow(self.session, flow.id)

            work_queue_name = os.getenv('PREFECT_WORK_QUEUE')

            for pipeline in self.pipelines:
                
                # Each pipeline will deployed and run as a flow as per the schedule
                
                airbyte_objs = []
                dbt_obj = None
                flow_name = ''
                deployment_name = ''
                flow_function = None
                tags = [self.name]

                has_airbyte = False
                has_dbt = False
                
                # Check if airbyte connections are part of pipeline
                if 'connection_ids' in pipeline and len(pipeline['connection_ids']) > 0:
                    has_airbyte = True
                    for connection_id in pipeline['connection_ids']:
                        airbyte = Airbyte(connection_id=connection_id)
                        airbyte_objs.append(airbyte)

                # Check if dbt transformation is part of repo
                if 'dbt_dir' in pipeline and pipeline['dbt_dir'] is not None:
                    has_dbt = True
                    dbt_obj = Dbt(pipeline['dbt_dir'], os.getenv('DBT_VENV'))

                flow = Flow(airbyte_arr=airbyte_objs, dbt=dbt_obj)

                if has_airbyte and has_dbt:
                    flow_name = f'{self.name}_airbyte_dbt_flow'
                    deployment_name = f'{self.name}_airbyte_dbt_deploy'
                    flow_function = getattr(flow, 'airbyte_dbt_flow') # Callable
                elif has_airbyte:
                    flow_name = f'{self.name}_airbyte_flow'
                    deployment_name = f'{self.name}_airbyte_deploy'
                    flow_function = getattr(flow, 'airbyte_flow') # Callable
                elif has_dbt:
                    flow_name = f'{self.name}_dbt_flow'
                    deployment_name = f'{self.name}_dbt_deploy'
                    flow_function = getattr(flow, 'dbt_flow') # Callable

                deployment = await Deployment.build_from_flow(
                    flow=flow_function.with_options(name=flow_name),
                    name=deployment_name,
                    work_queue_name=work_queue_name,
                    tags = tags,
                )
                if 'schedule' in pipeline and len(pipeline['schedule']) > 3:
                    deployment.schedule = CronSchedule(cron = pipeline['schedule'])
                await deployment.apply()

                return deployment

        except Exception as e:

            print(e)

            await self.close_session()

    async def close_session(self):

        await self.session.close()

        return
