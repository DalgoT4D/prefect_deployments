import sys, getopt, argparse, json, os
from dotenv import load_dotenv
from pathlib import Path
import asyncio
from sqlalchemy import create_engine
from prefect.settings import PREFECT_API_DATABASE_CONNECTION_URL, PREFECT_API_URL
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
        parser.add_argument(
            '--reset',
            required=False,
            choices=['yes', 'no'],
            default='no',
            help='resetting the deployments will remove all deployments and create fresh ones',
            metavar='<yes or no>'
        )
        args = parser.parse_args()

        if args.deploy not in config:
            raise Exception(f'Config for {args.deploy} org not found')
        
        # cli args
        org_name = args.deploy
        reset = args.reset

        # create a datbase session
        url = PREFECT_API_DATABASE_CONNECTION_URL.value()
        engine = create_async_engine(
            url,
            echo=True,
        )
        session = sessionmaker(engine, class_=AsyncSession)

        organization = Organization(name=org_name, session=session(), pipelines=config[org_name])

        organization.deploy(reset == 'yes')
        await organization.close_session()

    except Exception as e:

        print(e)

if __name__ == "__main__":

    asyncio.run(main())
    

   