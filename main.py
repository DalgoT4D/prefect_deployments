from prefect import flow
from prefect_airbyte.connections import trigger_sync
from prefect_dbt.cli.commands import trigger_dbt_cli_command


@flow(name="Run Airbyte Sync")
def run_airbyte_sync():
      trigger_sync(
            connection_id="7a2c0dc6-c08b-4bc3-8cab-760e64ae8a8f",
            poll_interval_s=3,
            status_updates=True
      )
      return 1

@flow(name="Run dbt Transform")
def run_dbt_transform():
      trigger_dbt_cli_command(
            command="dbt run", project_dir='stir'
      )
      return 1

@flow(name="Orchestration flow")
def run_flow():

      #syncing airbyte
      run_airbyte_sync()

      #dbt transform
      run_dbt_transform()


if __name__ == "__main__":
      run_flow()