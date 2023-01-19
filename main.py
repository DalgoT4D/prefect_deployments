from prefect import flow, task
from prefect_airbyte.connections import trigger_sync
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from prefect_shell import shell_run_command
from dotenv import load_dotenv
import os

load_dotenv()

@flow(name="airbyte-sync")
def run_airbyte_sync():
      trigger_sync(
            connection_id=os.getenv('DOST_AIRBYTE_CONNECTION'),
            poll_interval_s=3,
            status_updates=True
      )
      return 1

@flow(name="github-pull")
def pull_dost_github_repo():
      shell_run_command('rm -rf dbt && git clone '+ os.getenv('DOST_GITHUB_URL'))
      return 1

@flow(name="dbt-transform")
def run_dbt_transform():
      trigger_dbt_cli_command(
            command="dbt deps", project_dir='dbt'
      )
      trigger_dbt_cli_command(
            command="dbt run", project_dir='dbt'
      )
      return 1

@flow(name="orchestration-flow")
def run_flow():

      #syncing airbyte
      run_airbyte_sync()

      # pull dbt repo
      pull_dost_github_repo()

      #dbt transform
      run_dbt_transform()

@flow(name="orchestrate-airbyte")
def run_airbyte_flow():

      #syncing airbyte
      run_airbyte_sync()

@flow(name="orchestrate-dbt")
def run_dbt_flow():

      # pull dbt repo
      pull_dost_github_repo()

      #dbt transform
      run_dbt_transform()

if __name__ == "__main__":
      run_dbt_flow()