from prefect import task
from prefect_shell import shell_run_command
from pathlib import Path
from pydantic import BaseModel, validator

class Dbt(BaseModel):
    dbt_code_path: str
    dbt_venv_path: str

    @validator('dbt_code_path')
    def check_dbt_code_dir_exists(value):
        if Path(value).is_dir() is None:
            raise Exception('Dbt organization repo does not exist')

    @validator('dbt_venv_path')
    def check_dbt_venv_dir_exist(value):
        if Path(value).is_dir() is None:
            raise Exception('Dbt virtual environment is not setup')
        
    def __init__(self, dbt_code_path, dbt_venv_path):
        super().__init__()

        self.dbt_code_path = dbt_code_path
        self.dbt_venv_path = dbt_venv_path


@task(task_run_name='pull_dbt_repo')
def pull_dbt_repo(dbt: Dbt) -> None:
    shell_run_command(command=f'git pull', cwd=dbt.dbt_code_path)

@task(task_run_name='dbt_deps')
def dbt_deps(dbt: Dbt) -> None:
    shell_run_command(helper_command= f'source {dbt.dbt_venv_path}/bin/activate', command=f'dbt deps', cwd=dbt.dbt_code_path)

@task(task_run_name='dbt_source_snapshot_freshness')
def dbt_source_snapshot_freshness(dbt: Dbt):
    shell_run_command(helper_command= f'source {dbt.dbt_venv_path}/bin/activate', command=f'dbt source snapshot-freshness', cwd=dbt.dbt_code_path)

@task(task_run_name='dbt_run')
def dbt_run(dbt: Dbt) -> None:
    shell_run_command(helper_command= f'source {dbt.dbt_venv_path}/bin/activate', command=f'dbt run', cwd=dbt.dbt_code_path)

@task(task_run_name='dbt_test')
def dbt_test(dbt: Dbt) -> None:
    shell_run_command(helper_command= f'source {dbt.dbt_venv_path}/bin/activate', command=f'dbt test', cwd=dbt.dbt_code_path)

@task(task_run_name='dbt_docs_generate')
def dbt_docs_generate(dbt: Dbt) -> None:
    shell_run_command(helper_command= f'source {dbt.dbt_venv_path}/bin/activate', command=f'dbt docs generate', cwd=dbt.dbt_code_path)