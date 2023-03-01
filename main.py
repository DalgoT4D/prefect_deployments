import sys, getopt, argparse, json, os
from pydantic import BaseModel
from dotenv import load_dotenv
from typing import List
from pathlib import Path
from prefect.deployments import Deployment

from tasks.Dbt import Dbt
from tasks.Airbyte import Airbyte
from flows.Flow import Flow

# Load env
load_dotenv()

# Load config

class Organization(BaseModel):
    name: str = None
    connection_ids: List[str] = []
    dbt_dir: str = None

    def __init__(self, name: str, connection_ids: List[str], dbt_dir: str):
        super().__init__()

        self.name = name
        self.connection_ids = connection_ids
        self.dbt_dir = dbt_dir
        
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

if __name__ == "__main__":

    try:
        config = None
        if not Path('config.json').is_file():
            raise Exception('Config file not found')

        with open('config.json') as f:
            config = json.load(f)

        parser = argparse.ArgumentParser(description='Prefect deployment of NGOs')
        parser.add_argument(
            '--deploy',
            required=True,
            choices=['stir', 'sneha'],
            help='please enter the name of the NGO',
            metavar='<org_name>'
        )
        args = parser.parse_args()

        if args.deploy not in config:
            raise Exception(f'Config for {args.deploy} org not found')
        
        org_name = args.deploy
        connection_ids = config[org_name]['connection_ids']
        dbt_dir = config[org_name]['dbt_dir']
        
        organization = Organization(org_name, connection_ids, dbt_dir)

        organization.deploy()

    except Exception as e:

        print(e)