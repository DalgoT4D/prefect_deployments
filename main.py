from prefect import flow, task
from prefect_airbyte.connections import trigger_sync
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from dotenv import load_dotenv

load_dotenv()

@flow(name="airbyte-sync")
def run_airbyte_sync():
      trigger_sync(
            connection_id=os.getenv('STIR_SURVEY_CTO_AIRBYTE'),
            poll_interval_s=3,
            status_updates=True
      )
      return 1

@flow(name="dbt-transform")
def run_dbt_transform():
      trigger_dbt_cli_command(
            command="dbt run", project_dir='stir'
      )
      return 1

@flow(name="orchestration-flow")
def run_flow():

      #syncing airbyte
      run_airbyte_sync()

      #dbt transform
      run_dbt_transform()


if __name__ == "__main__":
      run_flow()