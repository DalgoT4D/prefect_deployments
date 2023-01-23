from prefect import flow
from dotenv import load_dotenv
from flows.transform import *
from flows.sync import *

load_dotenv()

@flow(name="orchestrate-flow")
def run_flow():

      #syncing airbyte
      run_airbyte_sync()

      #dbt transform
      run_dbt_transform()

@flow(name="orchestrate-airbyte")
def run_airbyte_flow():

      #syncing airbyte
      run_airbyte_sync()

@flow(name="orchestrate-dbt")
def run_dbt_flow():

      #dbt transform
      run_dbt_transform()