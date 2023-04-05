# ddp_prefect_starter

- Setup a virtual environment using ``` python3 -m venv <name> ```
- Activate the virtual env ``` source <name>/bin/activate ```
- Install the dependencies from requirements.txt ``` pip3 install -r requirements.txt ```
- If you are using any new packages, install them in your virtual env and use this to update the requirements.txt ``` pip3 freeze > requirements.txt ```
- Start the prefect orion UI server ``` prefect server start ```
- To execute your flow you need to run an agent in different terminal ``` prefect agent start -q default ```. This will run the default queue worker.

# prefect cli tools

- Run the help command to understand the options ``` python3 main.py -h ```
- Setup the organization config in ```config.json``` . The boilerplate for this is available in ``` configExample.json ```
- Run the main file to deploy your organization ``` python3 main.py --deploy <org_name> ``` based on the config entered in ```config.json```
- To reset the deployments after updates in config run ``` python3 main.py --deploy <org_name> --reset yes ```
