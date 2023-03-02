import sys, getopt, argparse, json, os
from dotenv import load_dotenv
from pathlib import Path
import asyncio
from sqlalchemy import create_engine
from prefect.settings import PREFECT_API_DATABASE_CONNECTION_URL
from sqlalchemy.ext.asyncio import create_async_engine, async_session, AsyncSession
from sqlalchemy.orm import sessionmaker

from organization.Organization import Organization

# Load env
load_dotenv()

# Load config
async def main():

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

        # create a datbase session
        url = PREFECT_API_DATABASE_CONNECTION_URL.value()
        engine = create_async_engine(
            url,
            echo=True,
        )
        session = sessionmaker(engine, class_=AsyncSession)
        
        organization = Organization(org_name, connection_ids, dbt_dir, session())

        await organization.reset_deployments()

        await organization.close_session()

    except Exception as e:

        print(e)

if __name__ == "__main__":

    asyncio.run(main())
    

   