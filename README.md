# ddp_prefect_starter

- Setup a virtual environment using `python3 -m venv <name>`
- Activate the virtual env `source <name>/bin/activate`
- Install the dependencies from requirements.txt:
  ```bash
  pip3 install -r requirements.txt
  ```
- If you add new packages, install them in your virtual env and update requirements.txt:
  ```bash
  pip3 freeze > requirements.txt
  ```

## Environment Variables

Create a `.env` file in the project root and define the following variables:

```bash
DOST_AIRBYTE_CONNECTION=<your Airbyte connection ID>
DOST_GITHUB_URL=<your GitHub repository URL>
```

## Starting Prefect

1. Start the Prefect Orion server:

   ```bash
   prefect orion start
   ```

2. In a separate terminal, start an agent to poll the `default` work queue:

   ```bash
   prefect agent start -q default
   ```

## Deploying Flows

Register the provided deployment YAML files:

```bash
prefect deployment apply orchestrate-airbyte-deployment.yaml
prefect deployment apply orchestrate-dbt-deployment.yaml
prefect deployment apply orchestration-flow-deployment.yaml
```

## Running Flows

- To run the **orchestration-flow** manually:

  ```bash
  prefect deployment run dost-orchestrate-flow
  ```

- To run the **orchestrate-airbyte** flow manually:

  ```bash
  prefect deployment run dost-orchestrate-airbyte
  ```

- The **orchestrate-dbt** flow is scheduled to run every hour once applied.
