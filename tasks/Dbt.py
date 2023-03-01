from prefect_shell import shell_run_command
from pathlib import Path
from pydantic import BaseModel

class Dbt(BaseModel):
    dbt_code_path: str = None
    dbt_venv_path: str = None

    def __init__(self, dbt_code_path: str, dbt_venv_path: str) -> None:
        super().__init__()
        
        if Path(dbt_code_path).is_dir():
            self.dbt_code_path = dbt_code_path
        else:
            raise Exception('Dbt organization repo does not exist')
        
        if Path(dbt_venv_path).is_dir():
            self.dbt_venv_path = dbt_venv_path
        else:
            raise Exception('Dbt virtual environment is not setup')

    def pull_dbt_repo(self) -> None:
        shell_run_command(command=f'git pull', cwd=self.dbt_code_path)

    def dbt_deps(self) -> None:
        shell_run_command(helper_command= f'source ${self.dbt_venv_path}/bin/activate', command=f'dbt deps', cwd=self.dbt_code_path)

    def dbt_source_snapshot_freshness(self):
        shell_run_command(helper_command= f'source ${self.dbt_venv_path}/bin/activate', command=f'dbt source snapshot-freshness', cwd=self.dbt_code_path)

    def dbt_run(self) -> None:
        shell_run_command(helper_command= f'source ${self.dbt_venv_path}/bin/activate', command=f'dbt run', cwd=self.dbt_code_path)

    def dbt_test(self) -> None:
        shell_run_command(helper_command= f'source ${self.dbt_venv_path}/bin/activate', command=f'dbt test', cwd=self.dbt_code_path)

    def dbt_docs_generate(self) -> None:
        shell_run_command(helper_command= f'source ${self.dbt_venv_path}/bin/activate', command=f'dbt docs generate', cwd=self.dbt_code_path)